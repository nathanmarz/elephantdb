#!/bin/bash

# Remove all previously generated files.
rm -rf gen-javabean
rm -rf src/py/genpy
rm -rf gen-py
rm -rf src/jvm/elephantdb/generated

# Generate source for each thrift file.
for f in src/*.thrift
do
    thrift -r --gen py:utf8strings --gen java:beans,hashcode,nocamel $f
done

# Move generated files into proper directories
mv gen-py src/py/genpy
mv gen-javabean/elephantdb/generated src/jvm/elephantdb/generated

# Final cleanup.
rm -rf gen-javabean
