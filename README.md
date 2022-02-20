# hdfs Format
$HADOOP_HOME/bin/hdfs namenode -format

# Start hdfs
$HADOOP_HOME/sbin/start-dfs.sh

http://localhost:9870

# Create the hdfs tree
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/ubuntu/input

# Start yarn
$HADOOP_HOME/sbin/start-yarn.sh

http://localhost:8088

