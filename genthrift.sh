rm -rf gen-javabean

rm -rf src/jvm/elephantdb/generated
thrift --gen java:beans,hashcode,nocamel  src/elephantdb.thrift
mv gen-javabean/elephantdb/generated src/jvm/elephantdb/generated
rm -rf gen-javabean
