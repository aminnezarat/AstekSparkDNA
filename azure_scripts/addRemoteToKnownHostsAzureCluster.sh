#!/bin/bash
##setup
instList="instances.txt"
userName=mesos
privKey="myPrivateKey.key"

parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} ${instList} /home/mesos
parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} addToKnownHostsAzureCluster.sh /home/mesos/
parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} chmod +x /home/mesos/addToKnownHostsAzureCluster.sh
parallel-ssh -v -O IdentityFile=${privKey} -l ${userName} -e ../error -o ../output -h ${instList} /home/mesos/addToKnownHostsAzureCluster.sh

