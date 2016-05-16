package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.*;

public class SandboxTableCheckAndMutateIntegrationTest extends BaseSandboxIntegrationTest {
    static final Put put1, put2;
    static final Delete delete1, delete2;
    static final RowMutations rm1, rm2;

    static byte[] COL3 = "col_z".getBytes();

    static {
        put1 = new Put(newRowId);
        put1.add(CF1, COL2, "otherString".getBytes());
        put2 = new Put(existingRowId);
        put2.add(CF1, COL2, "otherString".getBytes());

        delete1 = new Delete(newRowId);
        delete1.deleteColumns(CF1, COL3);
        delete2 = new Delete(existingRowId);
        delete2.deleteColumns(CF1, COL3);

        rm1 = new RowMutations(newRowId);
        rm2 = new RowMutations(existingRowId);
        try {
            rm1.add(put1);
            rm1.add(delete1);
            rm2.add(put2);
            rm2.add(delete2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCheckAndMutateOnNullColumns() throws IOException, SandboxException {
        testCheckAndMutateOnNullColumnsForTable(hTableMimic);
        testCheckAndMutateOnNullColumnsForTable(hTableSandbox);
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

        boolean opSucceeded;
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


    @Test
    public void testCheckAndMutateOnEmptyOriginal() throws IOException, SandboxException {
        testCheckAndMutateOnEmptyOriginalForTable(hTableMimic);
        testCheckAndMutateOnEmptyOriginalForTable(hTableSandbox);

        pushSandbox();

        verifyFinalStateCheckAndMutateOnEmptyOriginal(hTableOriginal);
    }

    private void testCheckAndMutateOnEmptyOriginalForTable(HTable hTable) throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        // verify there's nothing in the tables
        ResultScanner origResults, results;

        origResults = hTableOriginal.getScanner(scan);
        results = hTable.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("table should have no rows", 0L, countRows(results));

        // CASE: insert when cell is empty
        assertFalse("should fail on non-existent cell",
                hTable.checkAndMutate(newRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, "someString".getBytes(), rm1));

        assertEquals("no value should be added", null, getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTable, newRowId, CF1, COL2));
        assertEquals("no value should be added", null, getCellValue(hTable, newRowId, CF1, COL3));

        // CASE original empty, sandbox filled
        // insert when cell is filled but doesn't match value
        setCellValue(hTable, newRowId, CF1, COL1, "v1");
        setCellValue(hTable, newRowId, CF1, COL3, "col3");

        assertFalse("should fail on non-matching value cell",
                hTable.checkAndMutate(newRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, "someString".getBytes(), rm1));

        assertEquals("filled value should be there", "v1", getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTable, newRowId, CF1, COL2));
        assertEquals("no value should be deleted", "col3", getCellValue(hTable, newRowId, CF1, COL3));


        // CASE: insert when cell is filled and matches value
        setCellValue(hTable, newRowId, CF1, COL1, "v2");

        assertTrue("should work on matching value cell",
                hTable.checkAndMutate(newRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, "v2".getBytes(), rm1));


        assertEquals("filled value should be there", "v2", getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTable, newRowId, CF1, COL2));
        assertEquals("value should be deleted", null, getCellValue(hTable, newRowId, CF1, COL3));

        // CASE: insert when cell is filled in sandbox, then deleted
        setCellValue(hTable, newRowId, CF1, COL2, "v3");
        delCell(hTable, newRowId, CF1, COL1);

        verifyFinalStateCheckAndMutateOnEmptyOriginal(hTable);
    }

    private void verifyFinalStateCheckAndMutateOnEmptyOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndMutate(newRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, "v2".getBytes(), rm1));

        assertEquals("no value should be returned for deleted cell", null, getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("value should remain untouched", "v3", getCellValue(hTable, newRowId, CF1, COL2));
        assertEquals("value should be remain deleted", null, getCellValue(hTable, newRowId, CF1, COL3));
    }

    @Test
    public void testCheckAndMutateOnFilledOriginal() throws IOException, SandboxException {
        fillOriginalTable();
        setCellValue(hTableOriginal, existingRowId, CF1, COL3, "col3");
        setCellValue(hTableMimic, existingRowId, CF1, COL3, "col3");

        testCheckAndMutateOnFilledOriginalForTable(hTableMimic);
        testCheckAndMutateOnFilledOriginalForTable(hTableSandbox);

        pushSandbox();

        verifyFinalStateCheckAndMutateOnFilledOriginal(hTableOriginal);
    }

    public void testCheckAndMutateOnFilledOriginalForTable(HTable hTable) throws IOException, SandboxException {
        // CASE original filled, sandbox empty

        // verify there's nothing in the tables
        ResultScanner origResults, results;

        origResults = hTableOriginal.getScanner(scan);
        results = hTable.getScanner(scan);
        assertEquals("original table should have no rows", 20L, countRows(origResults));
        assertEquals("table should have no rows", 20L, countRows(results));

        // CASE: insert when cell is empty
        assertFalse("should fail on non-existent cell",
                hTable.checkAndMutate(existingRowId, CF2, COL1, CompareFilter.CompareOp.EQUAL,"someString".getBytes(), rm2));

        assertEquals("no value should be added", "1", getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTable, existingRowId, CF1, COL2));
        assertEquals("no value should be deleted", "col3", getCellValue(hTable, existingRowId, CF1, COL3));

        // insert when cell is filled but doesn't match value
        assertFalse("should fail on non-matching value cell",
                hTable.checkAndMutate(existingRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL,"someString".getBytes(), rm2));

        assertEquals("filled value should be there", "1", getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTable, existingRowId, CF1, COL2));
        assertEquals("no value should be deleted", "col3", getCellValue(hTable, existingRowId, CF1, COL3));

        // insert when cell is filled and matches value
        assertTrue("should work on matching value cell",
                hTable.checkAndMutate(existingRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL,"1".getBytes(), rm2));

        assertEquals("filled value should be there", "1", getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTable, existingRowId, CF1, COL2));
        assertEquals("value should be deleted", null, getCellValue(hTable, existingRowId, CF1, COL3));

        // CASE original filled, sandbox filled (by previous checkAndMutate)
        // non matching value
        assertFalse("should work on non matching value cell",
                hTable.checkAndMutate(existingRowId, CF1, COL2, CompareFilter.CompareOp.EQUAL, "1".getBytes(), rm2));

        // CASE: insert when cell is filled in sandbox, then deleted
        setCellValue(hTable, existingRowId, CF1, COL2, "v3");
        setCellValue(hTable, newRowId, CF1, COL3, "col3");
        delCell(hTable, existingRowId, CF1, COL1);

        verifyFinalStateCheckAndMutateOnFilledOriginal(hTable);
    }

    private void verifyFinalStateCheckAndMutateOnFilledOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndMutate(existingRowId, CF1, COL1, CompareFilter.CompareOp.EQUAL, "1".getBytes(), rm2));
        assertEquals("no value should be returned for deleted cell", null, getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("value should remain untouched", "v3", getCellValue(hTable, existingRowId, CF1, COL2));
        assertEquals("value should remain untouched", "col3", getCellValue(hTable, newRowId, CF1, COL3));
    }

}
