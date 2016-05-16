package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.mapr.AbstractHTable;

/**
 * Sandbox table definition
 */
final public class SandboxTable {
    public static final String DEFAULT_META_CF_NAME = "_sandmeta";
    public static final String DEFAULT_DIRTY_CF_NAME = "_sanddirty";
    public static final String DEFAULT_TID_COL_NAME = "_TID"; // transaction ID column
    public static final byte[] DEFAULT_META_CF = DEFAULT_META_CF_NAME.getBytes();
    public static final byte[] DEFAULT_DIRTY_CF = DEFAULT_DIRTY_CF_NAME.getBytes();
    public static final byte[] DEFAULT_TID_COL = DEFAULT_TID_COL_NAME.getBytes();
    public static final String METADATA_FILENAME_FORMAT = ".meta_%s"; // shadow table fid

    public enum InfoType {
        ORIGINAL_FID,
        SANDBOX_FID,
        SANDBOX_STATE
    }

    public enum SandboxState {
        ENABLED("Enabled"), // can be queried, manipulated, etc.
        SNAPSHOT_CREATE("SnapshotCreate"),
        PUSH_STARTED("PushStarted"),
        PUSH_FINISHED("PushFinished");

        String state;
        SandboxState(String state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return state;
        }

        public static SandboxState fromString(String state) {
            if (state != null) {
                for (SandboxState b : SandboxState.values()) {
                    if (state.equals(b.state)) {
                        return b;
                    }
                }
            }
            return null;
        }
    }

    public final AbstractHTable originalTable;
    public final AbstractHTable table;
    public final SandboxState state;

    public SandboxTable(AbstractHTable sandboxTable, AbstractHTable originalTable, SandboxState state) {
        this.table = sandboxTable;
        this.originalTable = originalTable;
        this.state = state;
    }
}
