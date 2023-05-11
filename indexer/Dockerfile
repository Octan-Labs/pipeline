FROM python:3.9-alpine
MAINTAINER Truong Ma Phi <trmaphi@gmail.com>
ENV PROJECT_DIR=ethereum-etl

RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR
COPY . .
RUN pip install --upgrade pip && pip install -e /$PROJECT_DIR/[streaming]

# Add Tini
RUN apk add --no-cache tini

# Tini is now available at /sbin/tini
ENTRYPOINT ["/sbin/tini", "--", "python", "ethereumetl"]
