#!/bin/bash

# TODO: check if python is available and show error if not

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
