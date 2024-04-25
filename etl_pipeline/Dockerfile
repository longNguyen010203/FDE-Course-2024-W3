FROM python:3.10-slim

WORKDIR /opt/dagster/app/elt_pipeline

COPY requirements.txt /opt/dagster/app/elt_pipeline

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
