#!/bin/bash
for file in `ps aux |grep "java -classpath" | awk '{print $2}'`
do
    kill -9 $file
done

