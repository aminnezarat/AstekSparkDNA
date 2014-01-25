#!/bin/bash

#colors
red='\033[31m'
NC='\033[0m'  # No Color
green='\033[32m'
blue='\033[34m'

echo -e "${blue}Creating instances in Azure...${NC}\n" 
sh ./createAzureCluster.sh

sleep 180
echo -e "${blue}Copying priv keys to instances...${NC}\n" 
sh ./copyKeyToAzureCluster.sh

echo -e "${blue}Adding instances to known hosts locally...${NC}\n" 
sh ./addToKnownHostsAzureCluster.sh

echo -e "${blue}Adding instances to known hosts in Azure Cluster...${NC}\n" 
sh ./addRemoteToKnownHostsAzureCluster.sh	
