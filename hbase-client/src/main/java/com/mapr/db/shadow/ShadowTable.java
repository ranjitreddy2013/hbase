package com.mapr.db.shadow;

import org.apache.hadoop.hbase.client.mapr.AbstractHTable;

/**
 * Shadow table definition
 */
final public class ShadowTable {
    public final AbstractHTable originalTable;
    public final AbstractHTable table;

    public ShadowTable(AbstractHTable shadowTable, AbstractHTable originalTable) {
        this.table = shadowTable;
        this.originalTable = originalTable;
    }
}
