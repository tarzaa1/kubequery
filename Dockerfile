FROM python:3.10-slim

WORKDIR /app

COPY ./kubequery ./kubequery

COPY requirements.txt .
COPY setup.py .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "kubequery/run.py"]