package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.mapr.AbstractHTable;

/**
 * Sandbox table definition
 */
final public class SandboxTable {
    public static final String DEFAULT_META_CF_NAME = "_shadow";
    public static final byte[] DEFAULT_META_CF = DEFAULT_META_CF_NAME.getBytes();
    public static final String METADATA_FILENAME_FORMAT = ".meta_%s"; // shadow table fid

    public enum InfoType {
        ORIGINAL_FID,
        PROXY_FID
    }

    public final AbstractHTable originalTable;
    public final AbstractHTable table;
    public final String proxyFid;

    public SandboxTable(AbstractHTable sandboxTable, AbstractHTable originalTable, String proxyFid) {
        this.table = sandboxTable;
        this.originalTable = originalTable;
        this.proxyFid = proxyFid;
    }
}
