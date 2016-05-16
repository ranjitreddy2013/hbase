#!/usr/bin/env bash

CURRENT_USER=`whoami`
CURRENT_USER_ID=`id -u`
SANDBOX_HOME=/opt/mapr/sandbox
SANDBOX_LOG_DIR=${SANDBOX_HOME}/logs
SANDBOX_LOG_FILE=${SANDBOX_LOG_DIR}/sandboxcli-${CURRENT_USER}-${CURRENT_USER_ID}.log

CLI_CLASSPATH="${SANDBOX_HOME}/lib:${SANDBOX_HOME}/conf"
for i in ${SANDBOX_HOME}/lib/*.jar; do
  CLI_CLASSPATH=${CLI_CLASSPATH}:$i;
done
CLI_CLASSPATH=${CLI_CLASSPATH}:$(/usr/bin/hbase classpath)
# java.library.path for rpc in c++
CLI_JAVA_LIBRARY_PATH="${CLI_HOME}/lib"
HADOOP_LIB_PATH=$(/usr/bin/hadoop jnipath)
CLI_JAVA_LIBRARY_PATH="${CLI_JAVA_LIBRARY_PATH}:${HADOOP_LIB_PATH}"

HADOOP_CLASSPATH=$(hbase classpath)

# TODO add logic to detect where's JAVA (through JAVA_HOME)
#-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999
java  \
 -Dhttps.protocols=TLSv1.2 \
 -Dlog.file=${CLI_LOG_FILE} \
     -Djava.library.path=${CLI_JAVA_LIBRARY_PATH} \
     -classpath ${CLI_CLASSPATH} \
     com.mapr.db.sandbox.SandboxTool "$@"