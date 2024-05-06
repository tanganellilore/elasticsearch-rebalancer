FROM python:3.11

LABEL maintainer="Lorenzo Tanganelli"

COPY ./* /elasticsearch_rebalancer/

RUN python /elasticsearch_rebalancer/setup.py install

