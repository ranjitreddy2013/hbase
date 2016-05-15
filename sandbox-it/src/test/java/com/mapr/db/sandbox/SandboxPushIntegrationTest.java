package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.countRows;
import static org.junit.Assert.assertEquals;

public class SandboxPushIntegrationTest extends BaseSandboxIntegrationTest {

    @Test
    public void testSandboxCountRows() throws IOException, SandboxException {
        HTable hTableOriginal = new HTable(conf, originalTablePath);
        HTable hTableSandbox = new HTable(conf, sandboxTablePath);

        ResultScanner scanner_original = hTableOriginal.getScanner(new Scan());
        ResultScanner scanner_sandbox = hTableSandbox.getScanner(new Scan());
        assertEquals("original and sandbox tables should be empty", 0L, countRows(scanner_original));
        assertEquals("original and sandbox tables should be empty", 0L, countRows(scanner_sandbox));

        // Insert some data to original table
        Put put = new Put(Bytes.toBytes("r1"));
        put.add(FAMILY_BYTES, Bytes.toBytes("name"), Bytes.toBytes("kanna"));
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        // Insert some data to sandbox table
        Put put1 = new Put(Bytes.toBytes("r2"));
        put1.add(FAMILY_BYTES, Bytes.toBytes("name"), Bytes.toBytes("alex"));
        hTableSandbox.put(put1);
        hTableSandbox.flushCommits();

        ResultScanner scanner_original1 = hTableOriginal.getScanner(new Scan());
        ResultScanner scanner_sandbox1 = hTableSandbox.getScanner(new Scan());
        assertEquals("original table should have the row r1", 1L, countRows(scanner_original1));
        assertEquals("sandbox should have the new row", 2L, countRows(scanner_sandbox1));

        // Push sandbox to original
        sandboxAdmin.pushSandbox(sandboxTablePath, true);
        ResultScanner scanner_original2 = hTableOriginal.getScanner(new Scan());
        assertEquals("original table should have the additional row", 2L, countRows(scanner_original2));
    }
}
