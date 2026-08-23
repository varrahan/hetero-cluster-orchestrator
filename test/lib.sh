#!/usr/bin/env bash

retry() {
  local deadline=$((SECONDS + $1)) status
  shift
  while true; do
    if "$@"; then
      return 0
    else
      status=$?
    fi
    (( status != 2 )) || return "$status"
    (( SECONDS < deadline )) || return 1
    sleep 2
  done
}
