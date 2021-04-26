FROM python:3.8

COPY ./ingress_adapter_jao ./ingress_adapter_jao
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt