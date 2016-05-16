package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.*;

public class SandboxTableCheckAndDeleteIntegrationTest extends BaseSandboxIntegrationTest {
    static Delete delete1, delete2;

    static {
        delete1 = new Delete(newRowId);
        delete1.deleteColumns(CF1, COL2);
        delete2 = new Delete(existingRowId);
        delete2.deleteColumns(CF2, COL2);
    }

    Scan scan = new Scan();

    @Test
    public void testCheckAndDeleteOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        // verify there's nothing in the tables but the inserted row
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 0L, countRows(mimicResults));

        // CASE: delete when match cell is empty
        assertFalse("should fail on non-existent cell",
                hTableSandbox.checkAndDelete(newRowId, CF1, COL1, "someString".getBytes(), delete1));
        assertFalse("should fail on non-existent cell",
                hTableMimic.checkAndDelete(newRowId, CF1, COL1, "someString".getBytes(), delete1));

        // assert results – nothing added, nothing deleted
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("nothing added nothing deleted", 0L, countRows(origResults));
        assertEquals("nothing added nothing deleted", 0L, countRows(sandResults));
        assertEquals("nothing added nothing deleted", 0L, countRows(mimicResults));

        // CASE original empty, sandbox filled
        // insert something to be deleted :)
        setCellValue(hTableSandbox, newRowId, CF1, COL2, "deleteMe");
        setCellValue(hTableMimic, newRowId, CF1, COL2, "deleteMe");

        // insert match cell with not matching value
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v1");
        setCellValue(hTableMimic, newRowId, CF1, COL1, "v1");

        assertFalse("should fail on non-matching value cell",
                hTableSandbox.checkAndDelete(newRowId, CF1, COL1, "someString".getBytes(), delete1));
        assertFalse("should fail on non-matching value cell",
                hTableMimic.checkAndDelete(newRowId, CF1, COL1, "someString".getBytes(), delete1));

        assertEquals("filled value should be there", "v1", getCellValue(hTableSandbox, newRowId, CF1, COL1));
        assertEquals("no value should be deleted", "deleteMe", getCellValue(hTableSandbox, newRowId, CF1, COL2));
        assertEquals("filled value should be there", "v1", getCellValue(hTableMimic, newRowId, CF1, COL1));
        assertEquals("no value should be deleted", "deleteMe", getCellValue(hTableMimic, newRowId, CF1, COL2));

        // CASE: match cell has matching value
        assertTrue("should work on matching value cell",
                hTableSandbox.checkAndDelete(newRowId, CF1, COL1, "v1".getBytes(), delete1));
        assertTrue("should work on matching value cell",
                hTableMimic.checkAndDelete(newRowId, CF1, COL1, "v1".getBytes(), delete1));

        assertEquals("filled value should be there", "v1", getCellValue(hTableSandbox, newRowId, CF1, COL1));
        assertEquals("value should be deleted", null, getCellValue(hTableSandbox, newRowId, CF1, COL2));
        assertEquals("filled value should be there", "v1", getCellValue(hTableMimic, newRowId, CF1, COL1));
        assertEquals("value should be deleted", null, getCellValue(hTableMimic, newRowId, CF1, COL2));

        // CASE: insert when 'match' cell is deleted in the sandbox
        delCell(hTableSandbox, newRowId, CF1, COL1);
        delCell(hTableMimic, newRowId, CF1, COL1);
        // insert something to be deleted :)
        setCellValue(hTableSandbox, newRowId, CF1, COL2, "deleteMe");
        setCellValue(hTableMimic, newRowId, CF1, COL2, "deleteMe");

        verifyFinalStateCheckAndDeleteOnEmptyOriginal(hTableSandbox);
        verifyFinalStateCheckAndDeleteOnEmptyOriginal(hTableMimic);

        pushSandbox();

        verifyFinalStateCheckAndDeleteOnEmptyOriginal(hTableOriginal);
    }

    private void verifyFinalStateCheckAndDeleteOnEmptyOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndDelete(newRowId, CF1, COL1, "v1".getBytes(), delete1));

        assertEquals("no value should be returned for deleted cell", null, getCellValue(hTable, newRowId, CF1, COL1));
        assertEquals("value should remain untouched", "deleteMe", getCellValue(hTable, newRowId, CF1, COL2));
    }


    @Test
    public void testCheckAndDeleteOnFilledOriginal() throws IOException, SandboxException {
        // CASE original filled, sandbox empty
        fillOriginalTable();

        // verify there's nothing in the tables
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have all rows", 40L, countCells(origResults));
        assertEquals("sandbox table should have all rows", 40L, countCells(sandResults));
        assertEquals("mimic table should have all rows", 40L, countCells(mimicResults));

        // CASE: match cell is null
        assertFalse("should fail on non-existent cell",
                hTableSandbox.checkAndDelete(existingRowId, CF2, COL1, "someString".getBytes(), delete2));
        assertFalse("should fail on non-existent cell",
                hTableMimic.checkAndDelete(existingRowId, CF2, COL1, "someString".getBytes(), delete2));

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("nothing added nothing deleted", 40L, countCells(origResults));
        assertEquals("nothing added nothing deleted", 40L, countCells(sandResults));
        assertEquals("nothing added nothing deleted", 40L, countCells(mimicResults));

        assertEquals("no value should be different", null, getCellValue(hTableSandbox, existingRowId, CF2, COL1));
        assertEquals("no value should be different", "someString", getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("no value should be different", null, getCellValue(hTableMimic, existingRowId, CF2, COL1));
        assertEquals("no value should be different", "someString", getCellValue(hTableMimic, existingRowId, CF2, COL2));

        // CASE: match cell doesn't match condition
        assertFalse("should fail on non-matching cell",
                hTableSandbox.checkAndDelete(existingRowId, CF1, COL1, "someString".getBytes(), delete2));
        assertFalse("should fail on non-matching cell",
                hTableMimic.checkAndDelete(existingRowId, CF1, COL1, "someString".getBytes(), delete2));

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("nothing added nothing deleted", 40L, countCells(origResults));
        assertEquals("nothing added nothing deleted", 40L, countCells(sandResults));
        assertEquals("nothing added nothing deleted", 40L, countCells(mimicResults));

        assertEquals("no value should be different", "1", getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("no value should be different", "someString", getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("no value should be different", "1", getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("no value should be different", "someString", getCellValue(hTableMimic, existingRowId, CF2, COL2));


        // CASE: match cell matches condition
        assertTrue("should succeed on matching cell",
                hTableSandbox.checkAndDelete(existingRowId, CF1, COL1, "1".getBytes(), delete2));
        assertTrue("should succeed on matching cell",
                hTableMimic.checkAndDelete(existingRowId, CF1, COL1, "1".getBytes(), delete2));

        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("one cell less", 39L, countCells(sandResults));
        assertEquals("one cell less", 39L, countCells(mimicResults));

        assertEquals("matching cell content should be intact", "1", getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("value should have been deleted", null, getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("matching cell content should be intact", "1", getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("value should have been deleted", null, getCellValue(hTableMimic, existingRowId, CF2, COL2));


        // CASE: insert when 'match' cell is deleted in the sandbox
        delCell(hTableSandbox, existingRowId, CF1, COL1);
        delCell(hTableMimic, existingRowId, CF1, COL1);
        // insert something to be deleted :)
        setCellValue(hTableSandbox, existingRowId, CF2, COL2, "deleteMe");
        setCellValue(hTableMimic, existingRowId, CF2, COL2, "deleteMe");

        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("all initial rows with matching cell deleted", 39L, countCells(sandResults));
        assertEquals("all initial rows with matching cell deleted", 39L, countCells(mimicResults));

        assertEquals("matching cell was deleted", null, getCellValue(hTableSandbox, existingRowId, CF1, COL1));
        assertEquals("cell to be deleted with expected content", "deleteMe", getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("matching cell was deleted", null, getCellValue(hTableMimic, existingRowId, CF1, COL1));
        assertEquals("cell to be deleted with expected content", "deleteMe", getCellValue(hTableMimic, existingRowId, CF2, COL2));

        verifyFinalStateCheckAndDeleteOnFilledOriginal(hTableSandbox);
        verifyFinalStateCheckAndDeleteOnFilledOriginal(hTableMimic);

        pushSandbox();

        verifyFinalStateCheckAndDeleteOnFilledOriginal(hTableOriginal);
    }

    private void verifyFinalStateCheckAndDeleteOnFilledOriginal(HTable hTable) throws IOException {
        assertFalse("should not work on deleted cell",
                hTable.checkAndDelete(existingRowId, CF1, COL1, "1".getBytes(), delete2));
        assertEquals("matching cell was deleted", null, getCellValue(hTable, existingRowId, CF1, COL1));
        assertEquals("value should remain untouched", "deleteMe", getCellValue(hTable, existingRowId, CF2, COL2));
    }

    // TODO add test where original is filled, and matching cell is updated in original before push (should mantain) the new value
}
