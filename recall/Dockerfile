FROM python:3.12-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/recall:/opt \
    COMPOSIO_CACHE_DIR=/tmp/recall-composio-cache

WORKDIR /opt/recall

COPY server/requirements.txt /opt/recall/requirements.txt
RUN python -m pip install --no-cache-dir --disable-pip-version-check --no-compile -r requirements.txt \
    && groupadd --gid 10001 recall \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin recall \
    && (getent group 1000 >/dev/null || groupadd --gid 1000 render-secrets) \
    && usermod -a -G 1000 recall \
    && install -d -m 0700 -o 10001 -g 10001 \
         /var/lib/recall /tmp/recall-composio-cache

COPY --chown=10001:10001 server/recall_server /opt/recall/recall_server
COPY --chown=10001:10001 server/schema /opt/recall/schema
COPY --chown=10001:10001 contracts /opt/contracts
COPY --chown=10001:10001 connectors /opt/connectors
COPY --chown=10001:10001 client /opt/recall/client
COPY --chown=10001:10001 collector /opt/recall/collector
COPY --chown=10001:10001 privacy /opt/recall/privacy
COPY --chown=10001:10001 scripts/install_gws.py /opt/recall/scripts/install_gws.py
COPY --chown=10001:10001 skills/recall/scripts/recall.py /opt/skills/recall/scripts/recall.py

RUN python /opt/recall/scripts/install_gws.py \
      --target x86_64-unknown-linux-gnu \
      --prefix /opt/recall/vendor/gws/0.22.5

USER 10001
EXPOSE 8788
VOLUME ["/var/lib/recall"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import http.client; connection=http.client.HTTPConnection('127.0.0.1',8788,timeout=2); connection.request('GET','/readyz'); response=connection.getresponse(); raise SystemExit(0 if response.status==200 else 1)"]

ENTRYPOINT ["python", "-m", "recall_server.cli"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8788", "--require-auth", "--capability-profile", "production"]
