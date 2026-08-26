#!/usr/bin/env bash

require_commands() {
  local name
  for name; do
    command -v "$name" >/dev/null || { echo "$name is required" >&2; return 1; }
  done
}

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
