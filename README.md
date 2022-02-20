# Formatage du hdfs
$HADOOP_HOME/bin/hdfs namenode -format

# Démarrage du hdfs
$HADOOP_HOME/sbin/start-dfs.sh

http://localhost:9870

# Créer l'arborescence hdfs
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/ubuntu/input

# Démarrer yarn
$HADOOP_HOME/sbin/start-yarn.sh

http://localhost:8088

