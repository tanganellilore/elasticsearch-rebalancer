FROM python:3.11

LABEL maintainer="Lorenzo Tanganelli"

COPY ./* /elasticsearch_rebalance/

RUN python /elasticsearch_rebalance/setup.py install

