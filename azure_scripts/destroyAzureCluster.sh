#!/bin/bash
instList="instances.txt"
while read line
do
  azure vm delete $line -q &
done < ${instList}	
