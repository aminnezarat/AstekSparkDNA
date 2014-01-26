#!/bin/bash


#colors
red='\033[31m'
NC='\033[0m'  # No Color
green='\033[32m'
blue='\033[34m'


###setup section
instNumber=8
imageName=b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-12_04_3-LTS-amd64-server-20131205-en-us-30GB
vmSzie=a6
instPreffix=sparkseq
userName=mesos
location="West Europe"
endPoints="443:8888,80:80,4040:4040"
instanceList="instances.txt"
cert="myCert.pem"
diskSizeGB=100

#cleanup
rm -f ${instanceList}

for i in $(seq 1 ${instNumber});
  do
    i=$(printf %03d $i)
    (
    ifRetry=1
    while [ $ifRetry -eq 1 ]
    do 
      error=$( azure vm create ${instPreffix}$i ${imageName} ${userName} -l "${location}" -z ${vmSzie} --ssh -t ${cert} -P 2>&1)
      exitCode=$?
      if [ $exitCode -ne 0 ]
      then
        echo -e "${red}Creating instance ${instPreffix}$i failed...${NC}\n"
        echo $error >> azure_create.err
        ifRetry=`echo $error | grep "conflict" | wc -l`
        if  [ $ifRetry -eq 1 ]
        then
	  echo -e "${green}Retrying to create instance ${instPreffix}$i ...${NC}\n"
        fi
      else
        ifRetry=0
      fi
     done 
     sleep 2
     azure vm endpoint create-multiple ${instPreffix}$i ${endPoints}
     sleep 5
    ) &
    echo "${instPreffix}${i}.cloudapp.net" >> ${instanceList}
  done

#azure vm disk attach-new ${instPreffix}$i ${diskSizeGB}
