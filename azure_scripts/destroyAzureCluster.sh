#!/bin/bash
instList="instances.txt"
while read line
do
  azure vm delete -b -q $line &
done < ${instList}	
