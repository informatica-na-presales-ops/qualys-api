FROM python:3.10.4-alpine3.15

RUN /sbin/apk add --no-cache libpq
RUN /usr/sbin/adduser -g python -D python

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

COPY --chown=python:python requirements.txt /home/python/qualys-api/requirements.txt
RUN /home/python/venv/bin/pip install --no-cache-dir --requirement /home/python/qualys-api/requirements.txt

ENV PATH="/home/python/venv/bin:${PATH}" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.source="https://github.com/informatica-na-presales-ops/qualys-api"

COPY --chown=python:python get-cloud-resource-ids.py /home/python/qualys-api/get-cloud-resource-ids.py
