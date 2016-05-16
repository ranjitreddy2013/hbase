package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.*;

public class SandboxTableCheckAndPutIntegrationTest extends BaseSandboxIntegrationTest {
    static Put put1,put2;

    static {
        put1 = new Put(newRowId);
        put1.add(CF1, COL2, "otherString".getBytes());

        put2 = new Put(existingRowId);
        put2.add(CF1, COL2, "otherString".getBytes());
    }

    Scan scan = new Scan();

    @Test
    public void testCheckAndPutOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        // verify there's nothing in the tables
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 0L, countRows(mimicResults));

        // CASE: insert when cell is empty
        assertFalse("should fail on non-existent cell",
                hTableSandbox.checkAndPut(newRowId, CF1, COL1, "someString".getBytes(), put1));
        assertFalse("should fail on non-existent cell",
                hTableMimic.checkAndPut(newRowId, CF1, COL1, "someString".getBytes(), put1));

        assertEquals("no value should be added", null, getCellValue(hTableSandbox, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableSandbox, newRowId, CF1, COL2));
        assertEquals("no value should be added", null, getCellValue(hTableMimic, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableMimic, newRowId, CF1, COL2));

        // CASE original empty, sandbox filled
        // insert when cell is filled but doesn't match value
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v1");
        setCellValue(hTableMimic, newRowId, CF1, COL1, "v1");

        assertFalse("should fail on non-matching value cell",
                hTableSandbox.checkAndPut(newRowId, CF1, COL1, "someString".getBytes(), put1));
        assertFalse("should fail on non-matching value cell",
                hTableMimic.checkAndPut(newRowId, CF1, COL1, "someString".getBytes(), put1));

        assertEquals("filled value should be there", "v1", getCellValue(hTableSandbox, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableSandbox, newRowId, CF1, COL2));
        assertEquals("filled value should be there", "v1", getCellValue(hTableMimic, newRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableMimic, newRowId, CF1, COL2));


        // CASE: insert when cell is filled and matches value
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v2");
        setCellValue(hTableMimic, newRowId, CF1, COL1, "v2");

        assertTrue("should work on matching value cell",
                hTableSandbox.checkAndPut(newRowId, CF1, COL1, "v2".getBytes(), put1));
        assertTrue("should work on matching value cell",
                hTableMimic.checkAndPut(newRowId, CF1, COL1, "v2".getBytes(), put1));

        assertEquals("filled value should be there", "v2", getCellValue(hTableSandbox, newRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTableSandbox, newRowId, CF1, COL2));
        assertEquals("filled value should be there", "v2", getCellValue(hTableMimic, newRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTableMimic, newRowId, CF1, COL2));

        // CASE: insert when cell is filled in sandbox, then deleted
        setCellValue(hTableSandbox, newRowId, CF1, COL2, "v3");
        setCellValue(hTableMimic, newRowId, CF1, COL2, "v3");
        delCell(hTableSandbox, newRowId, CF1, COL1);
        delCell(hTableMimic, newRowId, CF1, COL1);

        verifyFinalStateCheckAndPutOnEmptyOriginal(hTableSandbox);
        verifyFinalStateCheckAndPutOnEmptyOriginal(hTableMimic);

        pushSandbox();

        verifyFinalStateCheckAndPutOnEmptyOriginal(hTableOriginal);
    }

    private void verifyFinalStateCheckAndPutOnEmptyOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndPut(newRowId, CF1, COL1, "v2".getBytes(), put1));

        assertEquals("no value should be returned for deleted cell", null, getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("value should remain untouched", "v3", getCellValue(hTable, newRowId, CF1, COL2));
    }


    @Test
    public void testCheckAndPutOnFilledOriginal() throws IOException, SandboxException {
        // CASE original filled, sandbox empty
        fillOriginalTable();

        // verify there's nothing in the tables
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 20L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 20L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 20L, countRows(mimicResults));


        // CASE: insert when cell is empty
        assertFalse("should fail on non-existent cell",
                hTableSandbox.checkAndPut(existingRowId, CF2, COL1, "someString".getBytes(), put2));
        assertFalse("should fail on non-existent cell",
                hTableMimic.checkAndPut(existingRowId, CF2, COL1, "someString".getBytes(), put2));

        assertEquals("no value should be added", "1", getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableSandbox, existingRowId, CF1, COL2));
        assertEquals("no value should be added", "1", getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableMimic, existingRowId, CF1, COL2));

        // insert when cell is filled but doesn't match value
        assertFalse("should fail on non-matching value cell",
                hTableSandbox.checkAndPut(existingRowId, CF1, COL1, "someString".getBytes(), put2));
        assertFalse("should fail on non-matching value cell",
                hTableMimic.checkAndPut(existingRowId, CF1, COL1, "someString".getBytes(), put2));

        assertEquals("filled value should be there", "1", getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableSandbox, existingRowId, CF1, COL2));
        assertEquals("filled value should be there", "1", getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("no value should be added", null, getCellValue(hTableMimic, existingRowId, CF1, COL2));

        // insert when cell is filled and matches value
        assertTrue("should work on matching value cell",
                hTableSandbox.checkAndPut(existingRowId, CF1, COL1, "1".getBytes(), put2));
        assertTrue("should work on matching value cell",
                hTableMimic.checkAndPut(existingRowId, CF1, COL1, "1".getBytes(), put2));

        assertEquals("filled value should be there", "1", getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTableSandbox, existingRowId, CF1, COL2));
        assertEquals("filled value should be there", "1", getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("value should be added", "otherString", getCellValue(hTableMimic, existingRowId, CF1, COL2));

        // CASE original filled, sandbox filled (by previous checkAndPut)
        // non matching value
        assertFalse("should work on non matching value cell",
                hTableSandbox.checkAndPut(existingRowId, CF1, COL2, "1".getBytes(), put2));
        assertFalse("should work on non matching value cell",
                hTableMimic.checkAndPut(existingRowId, CF1, COL2, "1".getBytes(), put2));

        // CASE: insert when cell is filled in sandbox, then deleted
        setCellValue(hTableSandbox, existingRowId, CF1, COL2, "v3");
        setCellValue(hTableMimic, existingRowId, CF1, COL2, "v3");
        delCell(hTableSandbox, existingRowId, CF1, COL1);
        delCell(hTableMimic, existingRowId, CF1, COL1);

        verifyFinalStateCheckAndPutOnFilledOriginal(hTableSandbox);
        verifyFinalStateCheckAndPutOnFilledOriginal(hTableMimic);

        pushSandbox();

        verifyFinalStateCheckAndPutOnFilledOriginal(hTableOriginal);
    }

    private void verifyFinalStateCheckAndPutOnFilledOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndPut(existingRowId, CF1, COL1, "1".getBytes(), put2));
        assertEquals("no value should be returned for deleted cell", null, getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("value should remain untouched", "v3", getCellValue(hTable, existingRowId, CF1, COL2));
    }

    // TODO add test where original is filled, and matching cell is updated in original before push (should mantain) the new value
}
