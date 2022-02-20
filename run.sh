#!/bin/sh

# Java class Name
MAIN=$1
NB_CLUSTERS=$2
INPUT=$3
SRC=src/*.java
HADOOP_CLASSPATH="$(${HADOOP_HOME}/bin/hadoop classpath)"
OUT_CLASS_DIR=bin
OUT_JAR_DIR=.

mkdir -p $OUT_CLASS_DIR $OUT_JAR_DIR

# Compile with classpath
javac -classpath "$HADOOP_CLASSPATH" -d $OUT_CLASS_DIR $SRC &&

# creation du jar executable
jar -cf $OUT_JAR_DIR/out.jar -C $OUT_CLASS_DIR . &&

(
# clear hdfs output
${HADOOP_HOME}/bin/hdfs dfs -rm -r output

# run jar with input output args
${HADOOP_HOME}/bin/hadoop jar out.jar $MAIN $NB_CLUSTERS $INPUT output
)
