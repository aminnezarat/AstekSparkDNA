#!/bin/bash
##setup
instList="instances.txt"
while read h
do
  ip=$(dig +short $h)
  ssh-keygen -f ~/.ssh/known_hosts -R $h 1>/dev/null
  ssh-keygen -f ~/.ssh/known_hosts -R $ip 1>/dev/null
  ssh-keyscan -H $ip >> ~/.ssh/known_hosts
  ssh-keyscan -H $h >> ~/.ssh/known_hosts 	
done < ${instList}
