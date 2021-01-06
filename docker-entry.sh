#!/bin/bash

# use the build from the image if there is no local build
if [ ! -d "/app/target" ]; then
  cp -r /app-build/target /app/target
fi

exec "$@"
