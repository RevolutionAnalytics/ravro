#!/bin/bash

## Update versions of ravro and rmr2 based on current repo clones
# Build packages and push to hdfs 
rm -rf .packages && mkdir .packages
cd .packages
R CMD build ../ravro/pkg/ravro/
R CMD build ../rmr2/pkg/

hadoop fs -rm /user/ec2-user/packages/*
hadoop fs -put *.tar.gz /user/ec2-user/packages/

# Define the deploy command
install_cmd='
rm -r .deploy;
mkdir .deploy &&
cd .deploy && 
hadoop fs -get /user/ec2-user/packages/*.tar.gz && 
sudo R CMD INSTALL ravro*.tar.gz && 
sudo R CMD INSTALL rmr*.tar.gz &&
cd ..'

# Run it locally
eval $install_cmd

#Run it on each data node
dn_list=(`hadoop dfsadmin -printTopology`)
for i in `seq 2 2 $(echo "${#dn_list[*]}-1" | bc)`
do
dn_ip=${dn_list[$i]}
ssh -t ${dn_ip%%:*} $install_cmd
done
