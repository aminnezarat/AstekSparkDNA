#!/bin/bash

source azureConfig.cfg

while read line
do
  #azure vm delete -b -q $line &
  #azure vm delete -b -q `echo $line | cut -f1 -d'.'` &
  (ifRetry=1
    while [ $ifRetry -eq 1 ]
    do
      echo -e "${blue}Trying to destroy instance ${line} ...${NC}\n"
      error=$( azure vm delete -b -q `echo $line | cut -f1 -d'.'` 2>&1)
      exitCode=$?
      if [ $exitCode -ne 0 ]
      then
        echo -e "${red}Deleting instance $line failed...${NC}\n"
        echo $error >> azure_destroy.err
          echo -e "${blue}Retrying to destroy instance ${line} ...${NC}\n"
          sleep 5
      else
        ifRetry=0
        echo -e "${green}Deleting instance $line succesful...${NC}\n"
      fi
     done ) &

done < ${instList}	
#wait for disk to detach
sleep 10

#Delete unused disks (workaround!)
azure vm disk list | cut -f5 -d" " | grep sparkseq0 | while read line; do azure vm disk delete -b $line; done;
