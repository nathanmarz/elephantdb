#!/bin/bash

rm -rf gen-javabean
rm -rf src/py/genpy
rm -rf gen-py
rm -rf src/jvm/elephantdb/generated
thrift -r --gen py:utf8strings -out src/py/genpy --gen java:beans,hashcode,nocamel -out src/jvm/ src/core.thrift
mv gen-py src/py/genpy
mv gen-javabean/elephantdb/generated src/jvm/elephantdb/generated
rm -rf gen-javabean
