package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SandboxDeleteIntegrationTest extends BaseSandboxIntegrationTest {

    @Test
    public void testSandboxDelete() throws IOException, SandboxException {
        String sandboxTablePath = String.format("%s_sand1", originalTablePath);
        sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);

        Path sandboxMetaPath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);

        HTable hTableOriginal = new HTable(conf, originalTablePath);
        HTable hTableSandbox = new HTable(conf, sandboxTablePath);

        HBaseAdmin hba = new HBaseAdmin(new Configuration());
        FileSystem fs = FileSystem.get(new Configuration());

        // check if sandbox exists
        assertEquals("sandbox table exists", hba.tableExists(sandboxTablePath), true);
        assertEquals("sandbox meta file exists", fs.exists(sandboxMetaPath), true);

        // delete sandbox
        sandboxAdmin.deleteSandbox(sandboxTablePath);

        // check if sandbox has been removed
        assertEquals("sandbox table deleted", hba.tableExists(sandboxTablePath), false);
        assertEquals("sandbox meta file deleted", fs.exists(sandboxMetaPath), false);
    }

}
