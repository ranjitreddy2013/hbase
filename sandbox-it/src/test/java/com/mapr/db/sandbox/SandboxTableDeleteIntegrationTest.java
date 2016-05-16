package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxTableDeleteIntegrationTest extends BaseSandboxIntegrationTest {
    static Scan scan, scanCF;

    static {
        scan = new Scan();
        scanCF = new Scan();
        scanCF.addFamily(CF1);
    }

    final byte[] testRowId1 = "aRowId1".getBytes();
    final byte[] testRowId2 = "aRowId2".getBytes();
    final byte[] testRowId3 = "aRowId3".getBytes();

    @Test
    public void testDeleteOnEmptyOriginal() throws IOException, SandboxException {
        // CASE: original empty, sandbox empty
        ResultScanner origResults, sandResults, mimicResults;

        // scan all rows and count
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        // verify there's nothing on any table
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have rows", 0L, countRows(mimicResults));

        // delete things in the empty sandbox table (and mimic)
        delCell(hTableSandbox, testRowId1, CF1, COL1);
        delFamily(hTableSandbox, testRowId2, CF2);
        delRow(hTableSandbox, testRowId3);


        delCell(hTableMimic, testRowId1, CF1, COL1);
        delFamily(hTableMimic, testRowId2, CF2);
        delRow(hTableMimic, testRowId3);

        // assert same result
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        // TODO analyse if it should return empty results or not
        assertEquals("sandbox table should return rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have rows", 0L, countRows(mimicResults));

        // CASE: original empty, sandbox filled
        // load data into sandbox
        setCellValue(hTableSandbox, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableSandbox, testRowId1, CF1, COL2, "v2");
        setCellValue(hTableSandbox, testRowId2, CF2, COL1, "v3");
        setCellValue(hTableSandbox, testRowId2, CF2, COL2, "v4");
        setCellValue(hTableSandbox, testRowId3, CF1, COL1, "v5");
        setCellValue(hTableSandbox, testRowId3, CF2, COL1, "v6");

        setCellValue(hTableMimic, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableMimic, testRowId1, CF1, COL2, "v2");
        setCellValue(hTableMimic, testRowId2, CF2, COL1, "v3");
        setCellValue(hTableMimic, testRowId2, CF2, COL2, "v4");
        setCellValue(hTableMimic, testRowId3, CF1, COL1, "v5");
        setCellValue(hTableMimic, testRowId3, CF2, COL1, "v6");

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 3L, countRows(sandResults));
        assertEquals("mimic table should have rows", 3L, countRows(mimicResults));

        // repeat the deletions
        delCell(hTableSandbox, testRowId1, CF1, COL1);
        delFamily(hTableSandbox, testRowId2, CF2);
        delRow(hTableSandbox, testRowId3);

        delCell(hTableMimic, testRowId1, CF1, COL1);
        delFamily(hTableMimic, testRowId2, CF2);
        delRow(hTableMimic, testRowId3);

        // verify results before and after push
        verifyFinalStateDeleteOnEmptyOriginal(hTableSandbox);
        verifyFinalStateDeleteOnEmptyOriginal(hTableMimic);

        pushSandbox();
        verifyFinalStateDeleteOnEmptyOriginal(hTableOriginal);
    }

    private void verifyFinalStateDeleteOnEmptyOriginal(HTable hTable) throws IOException {
        ResultScanner results = hTable.getScanner(scan);
        assertEquals("table should return a single row", 1L, countRows(results));
        results = hTable.getScanner(scan);
        assertEquals("table should return a single cell", 1L, countCells(results));
        assertEquals("the right cell stays after all the deletions", "v2", getCellValue(hTable, testRowId1, CF1, COL2));
    }

    @Test
    public void testSingleCellDeletion() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        // verify there is something there
        assertEquals("2", getCellValue(hTableOriginal, existingRowId2, CF1, COL1));
        assertEquals("2", getCellValue(hTableSandbox, existingRowId2, CF1, COL1));
        assertEquals("2", getCellValue(hTableMimic, existingRowId2, CF1, COL1));

        // delete cell
        delCell(hTableSandbox, existingRowId2, CF1, COL1);
        delCell(hTableMimic, existingRowId2, CF1, COL1);

        // verify there is nothing there

        verifySingleRowDeletionState(hTableMimic);
        verifySingleRowDeletionState(hTableSandbox);

        pushSandbox();

        verifySingleRowDeletionState(hTableOriginal);
    }

    private void verifySingleRowDeletionState(HTable hTable) throws IOException {
        assertEquals(null, getCellValue(hTable, existingRowId2, CF1, COL1));
    }

    @Test
    public void testColFamilyDeletion() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        ResultScanner origResults, sandResults, mimicResults;

        // scan all rows and count
        origResults = hTableOriginal.getScanner(scanCF);
        sandResults = hTableSandbox.getScanner(scanCF);
        mimicResults = hTableMimic.getScanner(scanCF);
        // verify there's nothing on any table
        assertEquals("original table should have cells", 20L, countCells(origResults));
        assertEquals("sandbox table should return cells", 20L, countCells(sandResults));
        assertEquals("mimic table should have cells", 20L, countCells(mimicResults));

        // verify there is something there
        assertEquals("2", getCellValue(hTableSandbox, existingRowId2, CF1, COL1));
        assertEquals("2", getCellValue(hTableMimic, existingRowId2, CF1, COL1));

        // delete cell
        delCell(hTableSandbox, existingRowId2, CF1, COL1);
        delCell(hTableMimic, existingRowId2, CF1, COL1);

        // verify there is nothing there

        verifyColFamilyDeletionState(hTableMimic);
        verifyColFamilyDeletionState(hTableSandbox);

        pushSandbox();

        verifyColFamilyDeletionState(hTableOriginal);
    }

    private void verifyColFamilyDeletionState(HTable hTable) throws IOException {
        ResultScanner results = hTable.getScanner(scanCF);
        assertEquals("table should have one less cell", 19L, countCells(results));

        assertEquals(null, getCellValue(hTable, existingRowId2, CF1, COL1));
    }

    @Test
    public void testFullRowDeletion() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        ResultScanner origResults, sandResults, mimicResults;

        // scan all rows and count
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        // verify there's nothing on any table
        assertEquals("original table should have rows", 20L, countRows(origResults));
        assertEquals("sandbox table should return rows", 20L, countRows(sandResults));
        assertEquals("mimic table should have rows", 20L, countRows(mimicResults));

        // verify there is something there
        assertEquals("2", getCellValue(hTableSandbox, existingRowId2, CF1, COL1));
        assertEquals("2", getCellValue(hTableMimic, existingRowId2, CF1, COL1));

        // delete cell
        delRow(hTableSandbox, existingRowId2);
        delRow(hTableMimic, existingRowId2);

        // verify there is nothing there

        verifyRowDeletionState(hTableMimic);
        verifyRowDeletionState(hTableSandbox);

        pushSandbox();

        verifyColFamilyDeletionState(hTableOriginal);
    }

    private void verifyRowDeletionState(HTable hTable) throws IOException {
        ResultScanner results = hTable.getScanner(scan);
        assertEquals("table should have one less row", 19L, countRows(results));
        results = hTable.getScanner(scan);
        assertEquals("table should have one less row", 38L, countCells(results));

        assertEquals(null, getCellValue(hTable, existingRowId2, CF1, COL1));
    }


    @Test
    public void testDeleteOnFilledOriginal() throws IOException, SandboxException {
        // CASE: original filled, sandbox empty
        // load data
        loadData(hTableOriginal);
        loadData(hTableMimic);

        ResultScanner origResults, sandResults, mimicResults;

        // scan all rows and count
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        // verify that all rows are on the tables
        assertEquals("original table should have all initial rows", 20L, countRows(origResults));
        assertEquals("sandbox table should all initial rows", 20L, countRows(sandResults));
        assertEquals("mimic table should all initial rows", 20L, countRows(mimicResults));

        // scan all rows and count cells
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        // verify that all cells are on the tables
        assertEquals("original table should have all initial cells", 40L, countCells(origResults));
        assertEquals("sandbox table should all initial cells", 40L, countCells(sandResults));
        assertEquals("mimic table should all initial cells", 40L, countCells(mimicResults));


        // delete things in the empty sandbox table (and mimic)  total 5 cells, 2 entire rows
        delCell(hTableSandbox, existingRowId, CF1, COL1);
        delCell(hTableSandbox, existingRowId, CF2, COL2);
        delFamily(hTableSandbox, existingRowId2, CF2);
        delRow(hTableSandbox, existingRowId3);

        delCell(hTableMimic, existingRowId, CF1, COL1);
        delCell(hTableMimic, existingRowId, CF2, COL2);
        delFamily(hTableMimic, existingRowId2, CF2);
        delRow(hTableMimic, existingRowId3);

        // count results
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have all rows", 20L, countRows(origResults));
        assertEquals("sandbox table should return non-deleted rows", 18L, countRows(sandResults));
        assertEquals("mimic table should return non-deleted rows", 18L, countRows(mimicResults));

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have all cells", 40L, countCells(origResults));
        assertEquals("sandbox table should return non-deleted cells", 35L, countCells(sandResults));
        assertEquals("mimic table should return non-deleted cells", 35L, countCells(mimicResults));


        // CASE: original filled, sandbox filled
        // write deleted data to the sandbox and mimic
        setCellValue(hTableSandbox, existingRowId, CF1, COL1, "v1");
        setCellValue(hTableSandbox, existingRowId2, CF2, COL1, "v2");
        setCellValue(hTableSandbox, existingRowId2, CF2, COL2, "v3");
        setCellValue(hTableSandbox, existingRowId3, CF1, COL1, "v4");
        setCellValue(hTableSandbox, existingRowId3, CF2, COL1, "v5");

        setCellValue(hTableMimic, existingRowId, CF1, COL1, "v1");
        setCellValue(hTableMimic, existingRowId2, CF2, COL1, "v2");
        setCellValue(hTableMimic, existingRowId2, CF2, COL2, "v3");
        setCellValue(hTableMimic, existingRowId3, CF1, COL1, "v4");
        setCellValue(hTableMimic, existingRowId3, CF2, COL1, "v5");

        // re-count rows
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have the same rows", 20L, countRows(origResults));
        assertEquals("sandbox table should have the new rows", 20L, countRows(sandResults));
        assertEquals("mimic table should have the new rows", 20L, countRows(mimicResults));

        // re-count cells
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        // verify that all cells are on the tables
        assertEquals("original table should have all initial cells", 40L, countCells(origResults));
        assertEquals("sandbox table should all initial cells", 40L, countCells(sandResults));
        assertEquals("mimic table should all initial cells", 40L, countCells(mimicResults));

        // re-delete selected rows
        delCell(hTableSandbox, existingRowId, CF1, COL1);
        delFamily(hTableSandbox, existingRowId2, CF2);
        delRow(hTableSandbox, existingRowId3);

        delCell(hTableMimic, existingRowId, CF1, COL1);
        delFamily(hTableMimic, existingRowId2, CF2);
        delRow(hTableMimic, existingRowId3);

        // assert results on sandbox
        // verify results before and after push
        verifyFinalStateDeleteOnFilledOriginal(hTableSandbox);
        verifyFinalStateDeleteOnFilledOriginal(hTableMimic);

        pushSandbox();
        verifyFinalStateDeleteOnFilledOriginal(hTableOriginal);
    }

    private void verifyFinalStateDeleteOnFilledOriginal(HTable hTable) throws IOException {
        ResultScanner results = hTable.getScanner(scan);
        assertEquals("table should return non-deleted rows only", 18L, countRows(results));

        results = hTable.getScanner(scan);
        assertEquals("mimic table should return non-deleted cells only", 35L, countCells(results));
    }

    // TODO batch delete
}
