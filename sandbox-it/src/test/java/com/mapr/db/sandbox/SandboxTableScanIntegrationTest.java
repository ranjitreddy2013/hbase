package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.*;

public class SandboxTableScanIntegrationTest extends BaseSandboxIntegrationTest {
    Scan scanCF1 = new Scan();
    Scan scanCF2 = new Scan();

    public SandboxTableScanIntegrationTest() {
        scanCF1.addFamily(CF1);
        scanCF2.addFamily(CF2);
    }

    @Test
    public void testScanForEmptyOriginal() throws IOException, SandboxException {
        // scan all rows and count
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 0L, countRows(mimicResults));

        // insert on sandbox / mimic on multiple CFs
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v1");
        setCellValue(hTableSandbox, newRowId, CF1, COL2, "v2");
        setCellValue(hTableSandbox, newRowId, CF2, COL1, "v3");
        setCellValue(hTableSandbox, newRowId, CF2, COL2, "v4");
        setCellValue(hTableSandbox, existingRowId, CF2, COL2, "v5");

        // repeat for mimic table
        setCellValue(hTableMimic, newRowId, CF1, COL1, "v1");
        setCellValue(hTableMimic, newRowId, CF1, COL2, "v2");
        setCellValue(hTableMimic, newRowId, CF2, COL1, "v3");
        setCellValue(hTableMimic, newRowId, CF2, COL2, "v4");
        setCellValue(hTableMimic, existingRowId, CF2, COL2, "v5");

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);

        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("mimic table should have a new row", 2L, countRows(mimicResults));
        assertEquals("sandbox table should have a new row", 2L, countRows(sandResults));

        // results fetched because iterating thru results for counting alters result object state
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("mimic table should have all new cells", 5L, countCells(mimicResults));
        assertEquals("sandbox table should have all new cells", 5L, countCells(sandResults));

        // Scan specifying one CF
        origResults = hTableOriginal.getScanner(scanCF1);
        sandResults = hTableSandbox.getScanner(scanCF1);
        mimicResults = hTableMimic.getScanner(scanCF1);

        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("mimic table should have a new row", 1L, countRows(mimicResults));
        assertEquals("sandbox table should have a new row", 1L, countRows(sandResults));

        // results fetched because iterating thru results for counting alters result object state
        sandResults = hTableSandbox.getScanner(scanCF1);
        mimicResults = hTableMimic.getScanner(scanCF1);
        assertEquals("mimic table should have all new cells", 2L, countCells(mimicResults));
        assertEquals("sandbox table should have all new cells", 2L, countCells(sandResults));


        // delete some row columns from sandbox
        delCell(hTableSandbox, newRowId, CF1, COL1);
        delCell(hTableMimic, newRowId, CF1, COL1);

        verifyFinalState(hTableSandbox);
        verifyFinalState(hTableMimic);

        pushSandbox();

        verifyFinalState(hTableOriginal);
    }

    private void verifyFinalState(HTable hTable) throws IOException {
        ResultScanner result,resultSelectiveCF;

        result = hTable.getScanner(scan);
        assertEquals("table should have a new row", 2L, countRows(result));
        result = hTable.getScanner(scan);
        assertEquals("table should have all new cells except deleted", 4L, countCells(result));

        resultSelectiveCF = hTable.getScanner(scanCF1);
        assertEquals("table should have a new row with selected CF", 1L, countRows(resultSelectiveCF));

        resultSelectiveCF = hTable.getScanner(scanCF1);
        assertEquals("table should have all cells from selected CF (except deleted)", 1L, countCells(resultSelectiveCF));
    }

    // TODO repeat same thing for filled original table (inserts and deletes beginning, middle and end)

    final byte[] nonExistingRowId = "norow".getBytes();

    @Test
    public void testScanAfterDeletionOfNonExistingRow() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);



        // scan all rows and count
        ResultScanner origResults, sandResults, mimicResults;
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 20L, countRows(origResults));
        assertEquals("sandbox table should return rows", 20L, countRows(sandResults));
        assertEquals("mimic table should have rows", 20L, countRows(mimicResults));

        // count cells
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have cells", 40L, countCells(origResults));
        assertEquals("sandbox table should return cells", 40L, countCells(sandResults));
        assertEquals("mimic table should have cells", 40L, countCells(mimicResults));

        // delete some row columns from sandbox
        delCell(hTableMimic, nonExistingRowId, CF1, COL1);
        delCell(hTableSandbox, nonExistingRowId, CF1, COL1);

        mimicResults = hTableMimic.getScanner(scan);
        verifyScanIteratorAfterDeletionOfNonExistingRow(mimicResults.iterator());

        sandResults = hTableSandbox.getScanner(scan);
        verifyScanIteratorAfterDeletionOfNonExistingRow(sandResults.iterator());
    }

    private void verifyScanIteratorAfterDeletionOfNonExistingRow(Iterator<Result> iterator) {
        while (iterator.hasNext()) {
            Result row = iterator.next();

            assertFalse("scans shouldn't contain empty rows", row.isEmpty());
            assertNotNull("scans shouldn't contain rows without cells", row.listCells());
        }
    }

    @Test
    public void testScanForFilledOriginal() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        // scan all rows and count
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 20L, countRows(origResults));
        assertEquals("sandbox table should return rows", 20L, countRows(sandResults));
        assertEquals("mimic table should have rows", 20L, countRows(mimicResults));

        // count cells
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have cells", 40L, countCells(origResults));
        assertEquals("sandbox table should return cells", 40L, countCells(sandResults));
        assertEquals("mimic table should have cells", 40L, countCells(mimicResults));

        // insert on sandbox / mimic on multiple CFs
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v1");
        setCellValue(hTableSandbox, newRowId, CF1, COL2, "v2");
        setCellValue(hTableSandbox, newRowId, CF2, COL1, "v3");
        setCellValue(hTableSandbox, newRowId, CF2, COL2, "v4");
        setCellValue(hTableSandbox, existingRowId, CF2, COL2, "v5");

        // repeat for mimic table
        setCellValue(hTableMimic, newRowId, CF1, COL1, "v1");
        setCellValue(hTableMimic, newRowId, CF1, COL2, "v2");
        setCellValue(hTableMimic, newRowId, CF2, COL1, "v3");
        setCellValue(hTableMimic, newRowId, CF2, COL2, "v4");
        setCellValue(hTableMimic, existingRowId, CF2, COL2, "v5");

        // re-count results
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have the same rows", 20L, countRows(origResults));
        assertEquals("mimic table should have the new and the old rows", 21L, countRows(mimicResults));
        assertEquals("sandbox table should have the new and the old rows", 21L, countRows(sandResults));

        // results fetched because iterating thru results for counting alters result object state
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("mimic table should have all new and old cells", 44L, countCells(mimicResults));
        assertEquals("sandbox table should have all new and old cells", 44L, countCells(sandResults));

        // Scan specifying one CF
        origResults = hTableOriginal.getScanner(scanCF1);
        sandResults = hTableSandbox.getScanner(scanCF1);
        mimicResults = hTableMimic.getScanner(scanCF1);
        assertEquals("original table should have rows with selected CF", 20L, countRows(origResults));
        assertEquals("mimic table should have a new row with selected CF", 21L, countRows(mimicResults));
        assertEquals("sandbox table should have a new row with selected CF", 21L, countRows(sandResults));

        // results fetched because iterating thru results for counting alters result object state
        sandResults = hTableSandbox.getScanner(scanCF1);
        mimicResults = hTableMimic.getScanner(scanCF1);
        assertEquals("mimic table should have all new cells for selected CF", 22L, countCells(mimicResults));
        assertEquals("sandbox table should have all new cells for selected CF", 22L, countCells(sandResults));


        // delete some row columns from sandbox
        delCell(hTableSandbox, newRowId, CF1, COL1);
        delCell(hTableMimic, newRowId, CF1, COL1);

        verifyFinalState2(hTableSandbox);
        verifyFinalState2(hTableMimic);

        pushSandbox();

        verifyFinalState2(hTableOriginal);
    }

    private void verifyFinalState2(HTable hTable) throws IOException {
        ResultScanner result,resultSelectiveCF;

        result = hTable.getScanner(scan);
        assertEquals("table should have a new row", 21L, countRows(result));
        result = hTable.getScanner(scan);
        assertEquals("table should have all new cells except deleted", 43L, countCells(result));

        resultSelectiveCF = hTable.getScanner(scanCF2);
        assertEquals("table should have a new row with selected CF", 21L, countRows(resultSelectiveCF));

        resultSelectiveCF = hTable.getScanner(scanCF2);
        assertEquals("table should have all cells from selected CF (except deleted)", 22L, countCells(resultSelectiveCF));

        resultSelectiveCF = hTable.getScanner(scanCF1);
        assertEquals("table should have a new row with selected CF", 21L, countRows(resultSelectiveCF));

        resultSelectiveCF = hTable.getScanner(scanCF1);
        assertEquals("table should have all cells from selected CF (except deleted)", 21L, countCells(resultSelectiveCF));
    }
}
