#!/usr/bin/env bash

CURRENT_USER=`whoami`
CURRENT_USER_ID=`id -u`
# TODO change this!
CLI_HOME=/opt/mapr
CLI_LOG_DIR=${CLI_HOME}/logs
CLI_LOG_FILE=${CLI_LOG_DIR}/sandboxcli-${CURRENT_USER}-${CURRENT_USER_ID}.log
CLI_CLASSPATH="${CLI_HOME}:${CLI_HOME}/build:${CLI_HOME}/conf"
for i in ${CLI_HOME}/lib/*.jar; do
  # Ignore all the hadoop jars since we add them later anyway
  if [[ $1 = "table" && $2 = "copy" && "${i:14}" =~ hadoop-*.*-dev-core*.jar ]]
  then
     continue
  else
     CLI_CLASSPATH=${CLI_CLASSPATH}:$i;
  fi
done
CLI_CLASSPATH=${CLI_CLASSPATH}:$(/usr/bin/hbase classpath)
# java.library.path for rpc in c++
CLI_JAVA_LIBRARY_PATH="${CLI_HOME}/lib"
HADOOP_LIB_PATH=$(/usr/bin/hadoop jnipath)
CLI_JAVA_LIBRARY_PATH="${CLI_JAVA_LIBRARY_PATH}:${HADOOP_LIB_PATH}"

# TODO add logic to detect where's JAVA (through JAVA_HOME)

#-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999
java  \
 -Dlog.file=${CLI_LOG_FILE} \
     -Djava.library.path=${CLI_JAVA_LIBRARY_PATH} \
     -classpath ${CLI_CLASSPATH} \
     com.mapr.db.sandbox.SandboxTool "$@"