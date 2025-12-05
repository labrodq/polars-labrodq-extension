FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

RUN apt-get update -qq \
    && apt-get install -y \
    build-essential \
    && apt-get clean autoclean \
    && apt-get autoremove -y

COPY . /polars_labrodq_extension

ARG UV_NO_CACHE=true
ENV UV_LINK_MODE=copy
# ENV PYTHONPATH=/polars_labrodq_extension/src/polars_labrodq_extension

WORKDIR /polars_labrodq_extension

RUN uv sync

RUN uv pip install -e .
