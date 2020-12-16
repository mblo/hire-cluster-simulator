#!/usr/bin/env bash

if [ -z "$CMD" ]
then
      echo "You need to set the variable CMD, and export it"
      exit 1
fi

source env/bin/activate

# make this command a one-liner command
CMD_ONELINE=$(echo $CMD | tr '\r\n' ' ' | sed 's/\\ //g')
# add additional cmd line arguments
CMD_ONELINE=$(echo "$CMD_ONELINE $@")

echo "Start preparing following run:"
echo "$CMD"
echo "As a one-line cmd: $CMD_ONELINE"

read -p "Are you sure? " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    eval $CMD_ONELINE
fi