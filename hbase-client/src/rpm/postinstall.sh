#!/usr/bin/env bash

# backup old hbase-client jar
HBASE_HOME=/opt/mapr/hbase/hbase-0.98.12
mv $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar.old
ln -s /opt/mapr/sandbox/lib/client/hbase-client-0.98.12-mapr-1506.jar $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar

# backup original jars in Drill folders
for i in $(find /opt/mapr/drill -name "hbase-client*jar"); do
    if [ ! -e `dirname $i`/mapr-hbase-5.0.0-mapr.jar ]; then
        ln -s /opt/mapr/lib/mapr-hbase-5.0.0-mapr.jar `dirname $i`/;
    fi
    mv $i ${i/.jar/.jar.old};
done


# link to hbase-client
for dir in $(find /opt/mapr/drill -name "3rdparty" -type d); do
    echo "Creating hbase-client symlink in $dir"
    if [ ! -e $dir/hbase-client-0.98.12-mapr-1506.jar ]; then
        ln -s $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar $dir/ ;
    fi
done
