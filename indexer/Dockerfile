FROM python:3.9-slim-bullseye
MAINTAINER Truong Ma Phi <trmaphi@gmail.com>
ENV PROJECT_DIR=ethereum-etl

    
RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR
COPY . .
RUN pip install --upgrade pip && pip install -e /$PROJECT_DIR/[streaming]

# Add Tini
RUN apt update && apt install -y --no-install-recommends tini \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Tini is now available at /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--", "python", "ethereumetl"]
