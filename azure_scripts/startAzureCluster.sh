#!/bin/bash
instList="instances.txt"
while read line
do
  azure vm start $line &
done < ${instList}	
