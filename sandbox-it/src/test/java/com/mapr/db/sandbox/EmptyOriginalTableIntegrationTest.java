package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.countRows;
import static org.junit.Assert.assertEquals;

public class EmptyOriginalTableIntegrationTest extends BaseSandboxIntegrationTest {
    @Test
    public void testSandboxCountRows() throws IOException {
        HTable hTable = new HTable(conf, sandboxTablePath);

        ResultScanner scanner = hTable.getScanner(new Scan());
        assertEquals("original and sandbox tables should be empty", 0L, countRows(scanner));


        for (int i = 0; i < 25; i++) {
            Put put = new Put(new String("rowId"+i).getBytes());
            put.add(FAMILY_BYTES, "col".getBytes(), Integer.toString(i).getBytes());
            hTable.put(put);
        }
        hTable.flushCommits();

        ResultScanner scanner2 = hTable.getScanner(new Scan());
        assertEquals("sandbox should have the new rows", 25L, countRows(scanner2));

        HTable originalHTable = new HTable(conf, originalTablePath);
        scanner = originalHTable.getScanner(new Scan());
        assertEquals("original table should remain empty", 0L, countRows(scanner));
    }
}
