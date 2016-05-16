package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class SandboxTableGetRowOrBeforeIntegrationTest extends BaseSandboxIntegrationTest {
    @Test(expected = UnsupportedOperationException.class)
    public void testGetRowOrBefore() throws IOException {
        HTable hTable = new HTable(conf, sandboxTablePath);
        HTable originalHTable = new HTable(conf, originalTablePath);

        Result result;
        // test empty table case
        String testRow = "rowId0";
        result = originalHTable.getRowOrBefore(testRow.getBytes(), CF1);
        assertTrue("original should not return any row", result.isEmpty());
        result = hTable.getRowOrBefore(testRow.getBytes(), CF1);
        assertTrue("sandbox should not return any row", result.isEmpty());
    }
}
