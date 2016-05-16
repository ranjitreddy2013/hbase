package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class SandboxTableGetRowOrBeforeIntegrationTest extends BaseSandboxIntegrationTest {
    @Test(expected = UnsupportedOperationException.class)
    public void testGetRowOrBefore() throws IOException {
        Result result;
        // test empty table case
        String testRow = "rowId0";
        result = hTableOriginal.getRowOrBefore(testRow.getBytes(), CF1);
        assertTrue("original should not return any row", result.isEmpty());
        result = hTableSandbox.getRowOrBefore(testRow.getBytes(), CF1);
        assertTrue("sandbox should not return any row", result.isEmpty());
    }
}
