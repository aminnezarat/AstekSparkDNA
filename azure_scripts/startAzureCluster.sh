#!/bin/bash

. azureConfig.cfg

while read line
do
  azure vm start $line &
done < ${instList}	
