#!/bin/bash

RELEASE=`head -1 project.clj | awk '{print $3}' | sed -e 's/\"//' | sed -e 's/\"//'`

echo Generating release $RELEASE

DIR=_release/elephantdb-$RELEASE

rm -rf _release
rm *.zip
rm *jar
export LEIN_ROOT=1 && lein clean, deps, compile, jar
mkdir -p $DIR
mkdir $DIR/lib
cp elephantdb*jar $DIR/
cp lib/*.jar $DIR/lib

echo $RELEASE > $DIR/RELEASE

cp README.md $DIR/
cp LICENSE $DIR/
cp BDBJE.LICENSE $DIR/

cd _release
zip -r elephantdb-$RELEASE.zip *
cd ..
mv _release/elephantdb-*.zip .
rm -rf _release

