#!/usr/bin/env python3

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


def parse_json_line(line):
    try:
        return json.loads(line)
    except Exception:
        logging.exception("Invalid JSON line")
        return None


def parse_json_array(contents):
    try:
        data = json.loads(contents)
        return data if isinstance(data, list) else []
    except Exception:
        logging.exception("Invalid JSON array")
        return []


class NormalizeRecord(beam.DoFn):
    def process(self, element):
        if not isinstance(element, dict):
            return

        try:
            first = element.get("first_name") or element.get("firstname") or ""
            last = element.get("last_name") or element.get("lastname") or ""
            name = f"{first.strip()} {last.strip()}".strip()

            age_val = None
            try:
                if element.get("age") is not None:
                    age_val = int(element["age"])
            except Exception:
                pass

            if age_val is None:
                tag = "unknown"
            elif age_val < 18:
                tag = "minor"
            elif age_val <= 60:
                tag = "adult"
            else:
                tag = "senior"

            out = dict(element)
            out["name"] = name
            out["age"] = age_val
            out["age_tag"] = tag
            out["ingestion_time"] = datetime.now(timezone.utc)

            yield out

        except Exception:
            logging.exception("Normalization failed")


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_gcs", required=True)
    parser.add_argument("--bq_table", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--single_json_array", action="store_true")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    bq_schema = { 
        "fields": [
            {"name": "name", "type": "STRING"},
            {"name": "first_name", "type": "STRING"},
            {"name": "last_name", "type": "STRING"},
            {"name": "age", "type": "INTEGER"},
            {"name": "age_tag", "type": "STRING"},
            {"name": "ingestion_time", "type": "TIMESTAMP"},
        ]
    }

    with beam.Pipeline(options=pipeline_options) as p:

        if known_args.single_json_array:
            records = (
                p
                | "MatchFiles" >> beam.io.fileio.MatchFiles(known_args.input_gcs)
                | "ReadFiles" >> beam.io.fileio.ReadMatches()
                | "ReadUtf8" >> beam.Map(lambda f: f.read_utf8())
                | "ParseArray" >> beam.FlatMap(parse_json_array)
            )
        else:
            records = (
                p
                | "ReadText" >> beam.io.ReadFromText(known_args.input_gcs)
                | "ParseJSON" >> beam.Map(parse_json_line)
            )

        (
            records
            | "Normalize" >> beam.ParDo(NormalizeRecord())
            | "WriteBQ" >> WriteToBigQuery(
                known_args.bq_table,
                schema=bq_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                method=WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=known_args.temp_location,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
