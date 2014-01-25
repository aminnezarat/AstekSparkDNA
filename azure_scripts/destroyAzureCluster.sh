#!/bin/bash
instList="instances.txt"
while read line
do
  #azure vm delete -b -q $line &
  azure vm delete -b -q `echo $line | cut -f1 -d'.'` &
done < ${instList}	
