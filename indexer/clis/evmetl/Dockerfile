# FROM python:3.9-slim-bullseye
# ENV PROJECT_DIR=evmetl

    
# RUN mkdir /$PROJECT_DIR
# WORKDIR /$PROJECT_DIR
# COPY . .
# RUN pip install --upgrade pip && pip install -e /$PROJECT_DIR/[streaming]

# # Add Tini
# RUN apt update && apt install -y --no-install-recommends tini \
#     && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# # Tini is now available at /usr/bin/tini
# ENTRYPOINT ["/usr/bin/tini", "--", "python", "evmetl"]

FROM python:3.9-slim-bullseye as builder

RUN pip install poetry==1.5.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache \
    PROJECT_DIR=evmetl

RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --without dev --no-root

FROM python:3.9-slim-bullseye as runtime

ENV PROJECT_DIR=evmetl
RUN mkdir /$PROJECT_DIR
WORKDIR /$PROJECT_DIR

ENV VIRTUAL_ENV=/$PROJECT_DIR/.venv \
    PATH="/$PROJECT_DIR/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY evmetl ./evmetl

# Add Tini
RUN apt update && apt install -y --no-install-recommends tini \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "evmetl"]