rm -rf gen-javabean
rm -rf src/py/genpy
rm -rf gen-py
rm -rf src/jvm/elephantdb/generated
thrift --gen py:utf8strings --gen java:beans,hashcode,nocamel src/elephantdb.thrift
mv gen-py src/py/genpy
mv gen-javabean/elephantdb/generated src/jvm/elephantdb/generated
rm -rf gen-javabean
