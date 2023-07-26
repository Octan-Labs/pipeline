FROM python:3.9-slim-bullseye

WORKDIR /app
COPY . .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Add Tini
RUN apt update && apt install -y --no-install-recommends tini \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Tini is now available at /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--", "python", "main.py"]
