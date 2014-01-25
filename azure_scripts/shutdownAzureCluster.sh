#!/bin/bash
instList="instances.txt"
while read line
do
  azure vm shutdown $line &
done < ${instList}	
