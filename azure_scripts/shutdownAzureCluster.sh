#!/bin/bash
. azureConfig.cfg

while read line
do
  azure vm shutdown $line &
done < ${instList}	
