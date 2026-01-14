#!/usr/bin/env bash

ENV_FILE="${ENV_FILE:-./compose.env}"
# read vars from .env
if [ -r "${ENV_FILE}" ]; then # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

build() {
  docker build -t "${IMAGENAME}:${VERSION}" -f container/Containerfile "${COMPOSEROOT}"
}

run() {
  docker run --rm -it \
    --build-arg TZ="${TZ}" \
    -v "${SHAREROOT}:/camera" \
    --device "/dev/dri:/dev/dri" \
    --name "${IMAGENAME}" "${IMAGENAME}:${VERSION}"
}

shell() {
  docker exec -it "${IMAGENAME}" /bin/bash
}

up() {
  docker compose -p "${IMAGENAME}" --env-file "${ENV_FILE}" up -d
}

down() {
  docker compose -p "${IMAGENAME}" down
}

help() {
  echo
  echo "Usage: $0 [--build | -b] [--run | -r] [--shell | -s] [--test] [--help | -h]"
  echo
  echo "  --build, -b      rebuild the image"
  echo "  --run, -r        run the tests"
  echo "  --shell, -s      open a shell inside the image"
  echo "  --test           rerun the tests"
  echo "  --help, -h       this message"
  echo
  echo "  up               start the container via docker compose"
  echo "  down             stop the container via docker compose"
  echo
  exit 1
}

CMD=${1:-} # only use first arg
case "$CMD" in
"--help" | "-h")
  help
  ;;
"--build" | "-b")
  build
  ;;
"--run" | "-r")
  run
  ;;
"--shell" | "-s")
  shell
  ;;
"up")
  up
  ;;
"down")
  down
  ;;
*)
  build
  ;;
esac

exit 0
