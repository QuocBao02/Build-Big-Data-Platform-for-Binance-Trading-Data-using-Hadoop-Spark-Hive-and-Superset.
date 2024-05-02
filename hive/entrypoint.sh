#!/bin/bash 

set -e 


#start all hadoop services 
sudo service ssh start
/usr/local/hadoop/bin/hdfs namenode -format 
/usr/local/hadoop/sbin/start-all.sh 
/usr/local/hive/bin/schematool -initSchema -dbType mysql --verbose
/usr/local/hive/bin/hive --service metastore &
/usr/local/hive/bin/hive --service hiveserver2 &


# run forever 
tail -f /dev/null