#!/bin/bash

###setup section
instNumber=2
imageName=b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-12_04_3-LTS-amd64-server-20131205-en-us-30GB
vmSzie=a6
instPreffix=sparkseq
userName=mesos
location="West Europe"
endPoints="443:8888,80:80,4040:4040"
instanceList="instances.txt"
cert="myCert.pem"
#cleanup
rm -f ${instanceList}

for i in $(seq 1 ${instNumber});
  do
    i=$(printf %03d $i)
    (
     azure vm create ${instPreffix}$i ${imageName} ${userName} -l "${location}" -z ${vmSzie} --ssh -t ${cert} -P
     azure vm endpoint create-multiple ${instPreffix}$i ${endPoints}
    ) &
    echo ${instPreffix}$i >> ${instanceList}
  done
