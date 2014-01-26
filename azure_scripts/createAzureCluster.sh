#!/bin/bash

. azureConfig.cfg
#cleanup
rm -f ${instanceList}

for i in $(seq 1 ${instNumber});
  do
    i=$(printf %03d $i)
    (
    ifRetry=1
    while [ $ifRetry -eq 1 ]
    do 
      error=$( azure vm create --virtual-network-name ${vpnName} --subnet-names Subnet-1 --affinity-group ${agName} ${instPreffix}$i ${imageName} ${userName} -l "${location}" -z ${vmSzie} --ssh -t ${cert} -P 2>&1)
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
