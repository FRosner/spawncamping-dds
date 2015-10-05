#!/bin/bash

hash python 2>/dev/null || { echo >&2 "Python executable needed but not installed. Aborting."; exit 1; }

ugly=$1
if [ -d "${ugly}" ] ; then
  for f in $ugly/*.js
  do
    python beautify-js.py $f
  done
else
  if [ -f "${ugly}" ]; then
    python beautify-js.py $ugly
  else
    echo "$ugly is neither a file nor a directory. Please specify the file or a directory to beautify."
  fi
fi
