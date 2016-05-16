#!/usr/bin/env bash

CURRENT_USER=`whoami`
CURRENT_USER_ID=`id -u`
SANDBOX_HOME=/opt/mapr/sandbox
SANDBOX_LOG_DIR=${SANDBOX_HOME}/logs
SANDBOX_LOG_FILE=${SANDBOX_LOG_DIR}/sandboxcli-${CURRENT_USER}-${CURRENT_USER_ID}.log

# hadoop env
HADOOP_VERSION=`cat /opt/mapr/hadoop/hadoopversion`
HADOOP_HOME=/opt/mapr/hadoop/hadoop-$HADOOP_VERSION
HADOOP_CLASSPATH=$(hbase classpath)
HADOOP_LIB_PATH=$(/usr/bin/hadoop jnipath)

CP="${SANDBOX_HOME}/conf:${SANDBOX_HOME}/lib"
for i in ${SANDBOX_HOME}/lib/*.jar; do
  CP=$CP:$i;
done

# Add drill to classpath if it's installed
if [ -f "/opt/mapr/drill/drillversion" ];
then
   DRILL_VERSION=`cat /opt/mapr/drill/drillversion`;
   DRILL_HOME=/opt/mapr/drill/drill-${DRILL_VERSION};
   CP=$CP:$DRILL_HOME/jars/*;
   for jar in $DRILL_HOME/jars/classb/*.jar; do
    if [[ ! $jar =~ .*logback.* ]]; then
        CP=$CP:$jar;
    fi
   done
   for jar in $DRILL_HOME/jars/3rdparty/*.jar; do
    if [[ ! $jar =~ .*slf4j.* ]]; then
        CP=$CP:$jar;
    fi
   done
fi

CP=$CP:$(/usr/bin/hbase classpath)
# java.library.path for rpc in c++
CLI_JAVA_LIBRARY_PATH="${SANDBOX_HOME}/lib"
CLI_JAVA_LIBRARY_PATH="${CLI_JAVA_LIBRARY_PATH}:${HADOOP_LIB_PATH}"


# Test for or find JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
  if [ -e `which java` ]; then
    SOURCE=`which java`
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    JAVA_HOME="$( cd -P "$( dirname "$SOURCE" )" && cd .. && pwd )"
  fi
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
    Error: JAVA_HOME is not set and Java could not be found
EOF
    exit 1
  fi
fi

JAVA=$JAVA_HOME/bin/java
if [ ! -e "$JAVA" ]; then
  echo "Java not found at JAVA_HOME=$JAVA_HOME"
  exit 1
fi

#$JAVA -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=9999 \
$JAVA \
  -Dhttps.protocols=TLSv1.2 \
  -Dlog4j.configuration=sandboxcli-log4j.properties \
  -Dlog.file=$SANDBOX_LOG_FILE \
  -Djava.library.path=${CLI_JAVA_LIBRARY_PATH} \
  -classpath $CP \
  com.mapr.db.sandbox.SandboxTool "$@"