ssh-copy-id -i $HOME/.ssh/id_rsa.pub  -o StrictHostKeyChecking=no  root@master
ssh-copy-id -i $HOME/.ssh/id_rsa.pub  -o StrictHostKeyChecking=no  root@slave1

$HADOOP_HOME/bin/hadoop namenode -format
sed -i s/master/$HOSTNAME/ $HADOOP_CONF_DIR/core-site.xml
sed -i s/master/$HOSTNAME/ $HADOOP_CONF_DIR/yarn-site.xml

$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-slaves.sh

hdfs dfs -mkdir /logs_nasa
hdfs dfs -put /root/NASA_access_log_Jul95 /logs_nasa/