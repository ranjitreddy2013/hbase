#!/usr/bin/env bash

# restore old hbase-client jar
HBASE_HOME=/opt/mapr/hbase/hbase-0.98.12
rm -f $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar
mv $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar.old $HBASE_HOME/lib/hbase-client-0.98.12-mapr-1506.jar

# restore original jars in Drill folders
for i in $(find /opt/mapr/drill -name "hbase-client*jar.old"); do
    mv $i ${i/.jar.old/.jar};

    POTENTIAL_LINK=`dirname $i`/mapr-hbase-5.0.0-mapr.jar
    if [ -L $POTENTIAL_LINK ]; then
        rm -f $POTENTIAL_LINK
    fi
done

# unlink to hbase-client
for link in $(find /opt/mapr/drill -name "hbase-client-0.98.12-mapr-1506.jar" -type l); do
    if [ -L $link ]; then
        echo "Removing $link"
        rm -rf $link;
    fi
done

