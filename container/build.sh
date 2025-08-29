#!/bin/bash

set -euxo pipefail
IMAGE_NAME="cam_archiver"

build() {
  docker build -t "$IMAGE_NAME" -f ./Containerfile .
}
run() {
  docker run --rm -it \
    -v "/share/FTPRoot/camera:/camera" \
    --device "/dev/dri:/dev/dri" \
    --name "$IMAGE_NAME" "$IMAGE_NAME"
}
shell() {
  docker exec -it "$IMAGE_NAME" /bin/bash
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
  exit 1
}

CMD=${@:-""}
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
*)
  build
  run
  ;;
esac

exit 0
