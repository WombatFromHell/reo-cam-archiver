# Builder stage: fetches and verifies supercronic. curl/ca-certificates live ONLY here,
# so the final image ships no network client or CA bundle.
FROM ghcr.io/linuxserver/ffmpeg:version-9.0-cli AS builder

# Latest releases available at https://github.com/aptible/supercronic/releases
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.49/supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=e63c11a9726b775a6a11801e81af4f3fb926aa68 \
    SUPERCRONIC=supercronic-linux-amd64

RUN apt-get update -y && apt-get install -y curl ca-certificates && \
    curl -fsSLO "$SUPERCRONIC_URL" && \
    echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - && \
    chmod +x "$SUPERCRONIC" && \
    mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}"

FROM ghcr.io/linuxserver/ffmpeg:version-9.0-cli

ARG TZ=$TZ

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
  apt-get install -y tini tmux nano && \
  ln -snf /usr/share/zoneinfo/${TZ} /etc/localtime && \
  echo "${TZ}" > /etc/timezone

# pull the verified supercronic binary from the builder; no curl/ca-certificates here
COPY --from=builder /usr/local/bin/supercronic-linux-amd64 /usr/local/bin/supercronic-linux-amd64
RUN ln -s /usr/local/bin/supercronic-linux-amd64 /usr/local/bin/supercronic

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY 99archival /app/99archival

RUN chmod 0755 /usr/local/bin/entrypoint.sh /app/99archival

WORKDIR /app/

COPY reo-archiver.sh \
  archive-task.sh \
  cleanup-task.sh /app/
RUN chmod 0755 /app/*.sh

# ponytail: tini as PID1 reaps zombies + forwards SIGTERM; the container runs as
# PUID:PGID (compose `user:`), so supercronic and every job are unprivileged.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
