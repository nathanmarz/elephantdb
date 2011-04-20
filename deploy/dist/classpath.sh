#!/bin/sh

CLASSPATH="$(find lib/ -follow -mindepth 1 -print0 2> /dev/null | tr \\0 \:)"

echo $CLASSPATH
