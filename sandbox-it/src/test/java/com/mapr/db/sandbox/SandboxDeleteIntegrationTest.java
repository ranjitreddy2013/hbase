package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SandboxDeleteIntegrationTest extends BaseSandboxIntegrationTest {

    @Test
    public void testSandboxDelete() throws IOException {
        String sandboxTablePath = String.format("%s_sand1", originalTablePath);
        String sandboxTableMetaFile = String.format("%s_meta", sandboxTablePath);
        sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);
        HTable hTableOriginal = new HTable(conf, originalTablePath);
        HTable hTableSandbox = new HTable(conf, sandboxTablePath);

        HBaseAdmin hba = new HBaseAdmin(new Configuration());
        FileSystem fs = FileSystem.get(new Configuration());

        // check if sandbox exists
        assertEquals("sandbox table exists", hba.tableExists(sandboxTablePath), true);
        assertEquals("sandbox meta file exists", fs.exists(new Path(sandboxTableMetaFile)), true);

        // delete sandbox
        sandboxAdmin.deleteSandbox(sandboxTablePath);

        // check if sandbox has been removed
        assertEquals("sandbox table deleted", hba.tableExists(sandboxTablePath), false);
        assertEquals("sandbox meta file deleted", fs.exists(new Path(sandboxTableMetaFile)), false);
    }

}
