FROM gcr.io/dataflow-templates-base/python310-template-launcher-base

WORKDIR /dataflow/template

# Copy only requirements first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code
COPY main.py .

# Tell Dataflow which file to run
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=main.py
