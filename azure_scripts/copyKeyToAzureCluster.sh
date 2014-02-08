#!/bin/bash

. azureConfig.cfg
#while read host
#do
# #ssh-keygen -f "/home/marek/.ssh/known_hosts" -R ${host}.cloudapp.net 1>/dev/null 	
# scp -i ${privKey} ${privKey} mesos@${host}:/home/mesos/.ssh/id_rsa 1>/dev/null
#done <${instList} 

parallel-scp -O IdentityFile=${privKey} -O User=${userName} -h ${instList} ${privKey} /home/mesos/.ssh/id_rsa 
