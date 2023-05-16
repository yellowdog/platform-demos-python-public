# syntax=docker/dockerfile:1

FROM python:3.9-slim-buster

ARG PYPI_INDEX_URL=https://pypi.python.org/simple

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

COPY requirements.txt /build/requirements.txt
RUN --mount=type=secret,id=netrc,dst=/root/.netrc pip3 install --no-cache-dir -U -r /build/requirements.txt --extra-index-url "$PYPI_INDEX_URL"

COPY src /src
COPY LICENSE /src/LICENSE
COPY docker /docker

WORKDIR /

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/docker/docker-entrypoint.sh"]
