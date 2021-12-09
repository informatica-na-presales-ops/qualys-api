FROM python:3.10.1-alpine3.15

RUN /usr/sbin/adduser -g python -D python

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

ENV PATH="/home/python/venv/bin:${PATH}" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.iamge.source="https://github.com/informatica-na-presales-ops/qualys-api"
