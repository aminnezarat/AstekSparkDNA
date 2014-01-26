#!/bin/bash

instList="instance.txt"

#first do this locally
while read h
do
  ip=$(dig +short $h)
  ssh-keygen -f ~/.ssh/known_hosts -R $h 1>/dev/null
  ssh-keygen -f ~/.ssh/known_hosts -R $ip 1>/dev/null
  ssh-keyscan -H $ip >> ~/.ssh/known_hosts
  ssh-keyscan -H $h >> ~/.ssh/known_hosts 	
done < ${instList}

#copy instances list to all cluster nodes and
#parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} ${instList} /home/mesos
#parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} addToKnownHostsAzureCluster.sh /home/mesos/
#parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} chmod +x /home/mesos/addToKnownHostsAzureCluster.sh
#parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} /home/mesos/addToKnownHostsAzureCluster.sh
#add all cluster nodes to known_hosts
#parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} "<<EOF
