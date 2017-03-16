#!/bin/bash
ant clean
ant
chmod u+x ./bin/startSimpleDB.sh
./bin/startSimpleDB.sh imdb.schema
