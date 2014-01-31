#!/bin/bash

. azureConfig.cfg

echo -e "${blue}Creating instances in Azure...${NC}\n" 
workDir=`pwd`

rm -f ${instanceList}

for i in $(seq 1 ${instNumber});
  do
    i=$(printf %03d $i)
    (
    ifRetry=1
    while [ $ifRetry -eq 1 ]
    do
      error=$( azure vm create --virtual-network-name ${vpnName} --subnet-names Subnet-1 --affinity-group ${agName} ${instPreffix}$i ${imageName} ${userName} -z ${vmSize} --ssh -t ${cert} -P 2>&1)
      exitCode=$?
      if [ $exitCode -ne 0 ]
      then
        echo -e "${red}Creating instance ${instPreffix}$i failed...${NC}\n"
        echo $error >> azure_create.err
        ifRetry=`echo $error | grep "failed" | wc -l`
        if  [ $ifRetry -eq 1 ]
        then
          echo -e "${green}Retrying to create instance ${instPreffix}$i ...${NC}\n"
        fi
      else
        ifRetry=0
      fi
     done
    ) &
    echo "${instPreffix}${i}.cloudapp.net" >> ${instanceList}
  done



#check if all instance are up and running (barrier)
allReady=0

while [ $allReady -eq 0 ]
do
  allReady=1
  while read host
  do 
    inst=`echo $host | cut -f1 -d'.'`
    instStatus=$(azure vm show ${inst} | grep -i instancestatus | cut -f6 -d" " | sed s/\"//\g)
    if [ "$instStatus" != "ReadyRole" ]
    then
      allReady=0
      echo -e "${red}Instance ${host} not yet started...${NC}\n" 
    else
     echo -e "${green}Instance ${host} started...${NC}\n"
     azure vm endpoint create-multiple ${inst} ${endPoints} &   
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
   while read h
   do
     ip=$(dig +short $h)
     ssh-keygen -f ~/.ssh/known_hosts -R $h 1>/dev/null
     ssh-keygen -f ~/.ssh/known_hosts -R $ip 1>/dev/null
     ssh-keyscan -H $ip >> ~/.ssh/known_hosts
     ssh-keyscan -H $h >> ~/.ssh/known_hosts
   done < ${instList}
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
