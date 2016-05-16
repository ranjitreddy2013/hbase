package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.getCellValue;
import static com.mapr.db.sandbox.SandboxTestUtils.setCellValue;
import static org.junit.Assert.*;

public class SandboxTableCheckAndMutateIntegrationTest extends BaseSandboxIntegrationTest {
    @Test
    public void testCheckAndMutateOnNullColumns() throws IOException, SandboxException {
        testCheckAndMutateOnNullColumnsForTable(hTableMimic);
        // TODO add for sandbox
    }
    public void testCheckAndMutateOnNullColumnsForTable(HTable htable) throws IOException, SandboxException {
        Put put = new Put(existingRowId);
        put.add(CF2, COL2, "v2".getBytes());
        Delete del = new Delete(existingRowId);
        del.deleteColumns(CF2, COL1);

        RowMutations rm = new RowMutations(existingRowId);
        rm.add(put);
        rm.add(del);

        // initial state
        setCellValue(htable, existingRowId, CF2, COL2, "v1");
        setCellValue(htable, existingRowId, CF2, COL1, "someContent");

        // verify initial state
        assertEquals("someContent", getCellValue(htable, existingRowId, CF2, COL1));
        assertEquals("v1", getCellValue(htable, existingRowId, CF2, COL2));

        //
        assertTrue(htable
                .checkAndMutate(existingRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, null, rm));

        assertEquals(null, getCellValue(htable, existingRowId, CF2, COL1));
        assertEquals("v2", getCellValue(htable, existingRowId, CF2, COL2));

        // CASE: verify that null match doesn't work for filled columns
        // set matching cell
        setCellValue(htable, existingRowId, CF1, COL1, "nonEmptyMatchingCell");
        // restore initial state
        setCellValue(htable, existingRowId, CF2, COL2, "v1");
        setCellValue(htable, existingRowId, CF2, COL1, "someContent");

        // verify it's all in good state to start the test
        assertEquals("nonEmptyMatchingCell", getCellValue(htable, existingRowId, CF1, COL1));
        assertEquals("someContent", getCellValue(htable, existingRowId, CF2, COL1));
        assertEquals("v1", getCellValue(htable, existingRowId, CF2, COL2));

        boolean opSucceeded = true;
        try {
            opSucceeded = htable
                    .checkAndMutate(existingRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, null, rm);
        } catch (DoNotRetryIOException ex) {
            opSucceeded = false;
        }
        assertFalse("op shouldn't succeed", opSucceeded);

        // verify nothing was done
        assertEquals("nonEmptyMatchingCell", getCellValue(htable, existingRowId, CF1, COL1));
        assertEquals("someContent", getCellValue(htable, existingRowId, CF2, COL1));
        assertEquals("v1", getCellValue(htable, existingRowId, CF2, COL2));
    }
}
