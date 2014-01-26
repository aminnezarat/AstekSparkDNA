#!/bin/bash

#colors
red='\033[31m'
NC='\033[0m'  # No Color
green='\033[32m'
blue='\033[34m'
userName=mesos
instList="instances.txt"
privKey="myPrivateKey.key"

echo -e "${blue}Creating instances in Azure...${NC}\n" 
sh ./createAzureCluster.sh

#check if all instance are up and running (barrier)
allReady=0

while [ $allReady -eq 0 ]
do
  allReady=1
  while read host
  do 
    inst=`echo $host | cut -f1 -d'.'`
    instStatus=`azure vm show ${inst} | grep -i instancestatus | cut -f6 -d" " | sed s/\"//\g`
    if [ "$instStatus" != "ReadyRole" ]
    then
      allReady=0
      echo -e "${red}Instance ${host} not yet started...${NC}\n" 
    else
     echo -e "${green}Instance ${host} started...${NC}\n"   
    fi	
  done < ${instList}
  sleep 30
done

echo -e "${green}All instances started...${NC}\n"

echo -e "${blue}Checking ssh connectivity...${NC}\n"
#Check ssh connectivity
allReady=0
while read host
do
 ssh-keygen -f "/home/marek/.ssh/known_hosts" -R ${host} 1>/dev/null
done <${instList}

while [ $allReady -eq 0 ]
do
   allReady=1
   sh ./addToKnownHostsAzureCluster.sh
   parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} date
   exitCode=$?
   if [ $exitCode -ne 0 ]
   then
     allReady=0
     echo -e "${red}SSH connectivity not ready for some hosts...${NC}\n"     
   fi
   sleep 30
done

echo -e "${blue}Copying priv keys to instances...${NC}\n" 
#sh ./copyKeyToAzureCluster.sh
parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} ${privKey} /home/mesos/.ssh/id_rsa
#sleep 60

#echo -e "${blue}Adding instances to known hosts locally...${NC}\n" 
#sh ./addToKnownHostsAzureCluster.sh
#sleep 10

echo -e "${blue}Adding instances to known hosts in Azure Cluster...${NC}\n" 
#sh ./addRemoteToKnownHostsAzureCluster.sh
parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} ${instList} /home/mesos
parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} addToKnownHostsAzureCluster.sh /home/mesos/
parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} chmod +x /home/mesos/addToKnownHostsAzureCluster.sh
parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} /home/mesos/addToKnownHostsAzureCluster.sh

echo -e "${blue}Azure Cluster setup completed!${NC}\n"	
