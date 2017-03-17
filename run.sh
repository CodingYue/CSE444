#!/bin/bash
ant clean
ant
chmod u+x ./bin/startSimpleDB.sh
if [ -n "$1" ]; then
    appendix=" -f $1"
fi
#echo "./bin/startSimpleDB.sh imdb.schema ${appendix}"
./bin/startSimpleDB.sh imdb.schema ${appendix}
