#!/bin/bash

## Initial install of dependencies

install_cmd='sudo R -e "install.packages(c(\"Rcpp\",\"rjson\",\"bit64\",\"bitops\",\"RJSONIO\",\"digest\",\"functional\",\"reshape2\",\"stringr\",\"plyr\",\"caTools\"))"'
eval $install_cmd

dn_list=(`hadoop dfsadmin -printTopology`)
for i in `seq 2 2 $(echo "${#dn_list[*]}-1" | bc)`
do
dn_ip=${dn_list[$i]}
ssh -t ${dn_ip%%:*} sudo yum -y install gcc-c++
ssh -t ${dn_ip%%:*} $install_cmd
done
