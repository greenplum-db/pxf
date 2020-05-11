#!/bin/bash

log() {
  echo "=====> $(date) $* <======"
}

_main() {
  sleep 1
  source "${PXF_HOME}/conf/pxf-env-default.sh"
  log 'Stopping PXF'
  "${PXF_HOME}/bin/pxf" stop && return
  # "insurance policy":
  local PID=$1
  if [[ -n $PID ]]; then
    ps "$1" >/dev/null && kill -9 "$1"
  fi
}

log 'PXF Out of memory detected'
_main "$1" &
log 'PXF shutdown scheduled'
