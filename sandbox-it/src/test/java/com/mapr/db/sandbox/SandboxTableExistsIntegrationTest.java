package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.*;

public class SandboxTableExistsIntegrationTest extends BaseSandboxIntegrationTest {
    byte[] testRowId1 = "rowId3".getBytes();
    byte[] testRowId2 = "rowId5".getBytes();
    final Get get1 = new Get(testRowId1);
    final Get get2 = new Get(testRowId2);
    final Get filledGet1 = new Get(existingRowId);
    final Get filledGet2 = new Get(existingRowId2);

    final List<Get> batchGet = Lists.newArrayList(
            new Get(testRowId1),
            new Get(testRowId2));
    final List<Get> filledBatchGet = Lists.newArrayList(
            new Get(testRowId1),
            new Get(testRowId2));

    @Test
    public void testExistsOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        assertFalse("original should not contain any row at all",
                hTableOriginal.exists(get1));
        assertFalse("sandbox should not contain any row at all",
                hTableSandbox.exists(get1));
        assertFalse("mimic should not contain any row at all",
                hTableMimic.exists(get1));
        assertFalse("original should not contain any row at all",
                hTableOriginal.exists(get2));
        assertFalse("sandbox should not contain any row at all",
                hTableSandbox.exists(get2));
        assertFalse("mimic should not contain any row at all",
                hTableMimic.exists(get2));

        // CASE original empty, sandbox filled
        // add 2 rows to sandbox (and mimic)
        setCellValue(hTableSandbox, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableMimic, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableSandbox, testRowId2, CF1, COL1, "v2");
        setCellValue(hTableMimic, testRowId2, CF1, COL1, "v2");

        assertFalse("original table should not contain added entry to sandbox",
                hTableOriginal.exists(get1));
        assertTrue("sandbox should contain the newly added row",
                hTableSandbox.exists(get1));
        assertTrue("mimic should contain the newly added row",
                hTableMimic.exists(get1));
        assertTrue("sandbox should contain the newly added row",
                hTableSandbox.exists(get2));
        assertTrue("mimic should contain the newly added row",
                hTableMimic.exists(get2));

        // delete the column
        delCell(hTableSandbox, testRowId1, CF1, COL1);
        delCell(hTableMimic, testRowId1, CF1, COL1);

        assertFalse("original table should not contain added entry to sandbox",
                hTableOriginal.exists(get1));
        assertFalse("original table should not contain added entry to sandbox",
                hTableOriginal.exists(get2));
        assertFalse("sandbox should not contain the deleted row",
                hTableSandbox.exists(get1));
        assertFalse("mimic should not contain the deleted row",
                hTableMimic.exists(get1));
        assertTrue("sandbox should keep the 2nd row",
                hTableSandbox.exists(get2));
        assertTrue("mimic should keep the 2nd row",
                hTableMimic.exists(get2));


        // delete a col from 2nd row and check existance
        delCell(hTableSandbox, testRowId2, CF1, COL1);
        delCell(hTableMimic, testRowId2, CF1, COL1);;

        verifyFinalState(hTableMimic);
        verifyFinalState(hTableSandbox);

        pushSandbox();

        verifyFinalState(hTableOriginal);
    }

    private void verifyFinalState(HTable hTable) throws IOException {
        assertFalse("table should not contain any row",
                hTable.exists(get1));
        assertFalse("tableshould not contain any row",
                hTable.exists(get2));
    }

    @Test
    public void testExistsOnFilledOriginal() throws IOException, SandboxException {
        // CASE original filled, sandbox empty
        loadData(hTableOriginal);
        loadData(hTableMimic);

        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet1));
        assertTrue("sandbox should contain the row",
                hTableSandbox.exists(filledGet1));
        assertTrue("mimic should contain the row",
                hTableMimic.exists(filledGet1));
        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet2));
        assertTrue("sandbox should contain the row",
                hTableSandbox.exists(filledGet2));
        assertTrue("mimic should contain the row",
                hTableMimic.exists(filledGet2));

        // delete on sandbox
        delCell(hTableSandbox, existingRowId2, CF1, COL1);
        delCell(hTableSandbox, existingRowId2, CF2, COL2);
        delCell(hTableMimic, existingRowId2, CF1, COL1);
        delCell(hTableMimic, existingRowId2, CF2, COL2);

        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet1));
        assertTrue("sandbox should contain the row",
                hTableSandbox.exists(filledGet1));
        assertTrue("mimic should contain the row",
                hTableMimic.exists(filledGet1));
        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet2));
        assertFalse("sandbox should contain the row",
                hTableSandbox.exists(filledGet2));
        assertFalse("mimic should contain the row",
                hTableMimic.exists(filledGet2));


        // CASE original filled, sandbox filled
        // add row to sandbox (and mimic)
        setCellValue(hTableSandbox, existingRowId2, CF1, COL1, "v1");
        setCellValue(hTableMimic, existingRowId2, CF1, COL1, "v1");

        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet1));
        assertTrue("sandbox should contain the row",
                hTableSandbox.exists(filledGet1));
        assertTrue("mimic should contain the row",
                hTableMimic.exists(filledGet1));
        assertTrue("original should contain the row",
                hTableOriginal.exists(filledGet2));
        assertTrue("sandbox should contain the newly added row",
                hTableSandbox.exists(filledGet2));
        assertTrue("mimic should contain the newly added row",
                hTableMimic.exists(filledGet2));

        // delete the column
        delCell(hTableSandbox, existingRowId2, CF1, COL1);
        delCell(hTableSandbox, existingRowId2, CF2, COL2);
        delCell(hTableMimic, existingRowId2, CF1, COL1);
        delCell(hTableMimic, existingRowId2, CF2, COL2);

        verifyFinalStateOnFilledOrig(hTableMimic);
        verifyFinalStateOnFilledOrig(hTableSandbox);

        pushSandbox();

        verifyFinalStateOnFilledOrig(hTableOriginal);
    }

    private void verifyFinalStateOnFilledOrig(HTable hTable) throws IOException {
        assertTrue("table should contain the row",
                hTable.exists(filledGet1));
        assertFalse("table should not contain the row",
                hTable.exists(filledGet2));
    }

    @Test
    public void testBatchExistsForEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        assertEquals("original should not contain any row at all", 0,
                countTrue(hTableOriginal.exists(batchGet)));
        assertEquals("sandbox should not contain any row at all", 0,
                countTrue(hTableSandbox.exists(batchGet)));
        assertEquals("mimic should not contain any row at all", 0,
                countTrue(hTableMimic.exists(batchGet)));

        // CASE original empty, sandbox filled
        // add 2 rows to sandbox (and mimic)
        setCellValue(hTableSandbox, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableMimic, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableSandbox, testRowId2, CF1, COL1, "v2");
        setCellValue(hTableMimic, testRowId2, CF1, COL1, "v2");

        assertEquals("original should not contain any row at all", 0,
                countTrue(hTableOriginal.exists(batchGet)));
        assertEquals("sandbox should contain the newly added rows", 2,
                countTrue(hTableSandbox.exists(batchGet)));
        assertEquals("mimic should contain the newly added row", 2,
                countTrue(hTableMimic.exists(batchGet)));

        // delete the column
        delCell(hTableSandbox, testRowId1, CF1, COL1);
        delCell(hTableMimic, testRowId1, CF1, COL1);


        assertEquals("sandbox should contain only 1 row", 1,
                countTrue(hTableSandbox.exists(batchGet)));
        assertEquals("mimic should contain only 1 row", 1,
                countTrue(hTableMimic.exists(batchGet)));


        // delete a col from 2nd row
        delCell(hTableSandbox, testRowId2, CF1, COL1);
        delCell(hTableMimic, testRowId2, CF1, COL1);

        verifyFinalStateForBatchExists(hTableMimic);
        verifyFinalStateForBatchExists(hTableSandbox);

        pushSandbox();

        verifyFinalStateForBatchExists(hTableOriginal);
    }

    private void verifyFinalStateForBatchExists(HTable hTable) throws IOException {
        assertEquals("table should contain no rows", 0,
                countTrue(hTable.exists(batchGet)));
    }

    // TODO do for filled original table
}
