#!/bin/bash

# start ssh 
sudo service ssh start

# start NameNode
/usr/local/hadoop/bin/hdfs namenode -format 
/usr/local/hadoop/sbin/start-all.sh 

# keep docker running
tail -f /dev/null