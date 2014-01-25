#!/bin/bash

#colors
red='\033[31m'
NC='\033[0m'  # No Color
green='\033[32m'
blue='\033[34m'

instanceList="instances.txt"


echo -e "${blue}Creating instances in Azure...${NC}\n" 
sh ./createAzureCluster.sh

#check if all instance are up and running (barrier)
allReady=0

while [ $allReady -eq 0 ]
do
  allReady=1
  while read host
  do 
    instStatus=`azure vm show $host | grep -i instancestatus | cut -f6 -d" " | sed s/\"//\g`
    if [ "$instStatus" != "ReadyRole" ]
    then
      allReady=0
      echo -e "${red}Instance ${host} not started...${NC}\n" 
    else
     echo -e "${green}Instance ${host} started...${NC}\n"   
    fi	
  done < ${instanceList}
  sleep 30
done


echo -e "${blue}Copying priv keys to instances...${NC}\n" 
sh ./copyKeyToAzureCluster.sh

echo -e "${blue}Adding instances to known hosts locally...${NC}\n" 
sh ./addToKnownHostsAzureCluster.sh

echo -e "${blue}Adding instances to known hosts in Azure Cluster...${NC}\n" 
sh ./addRemoteToKnownHostsAzureCluster.sh	
