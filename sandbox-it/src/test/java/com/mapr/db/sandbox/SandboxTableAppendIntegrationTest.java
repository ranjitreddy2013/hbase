package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxTableAppendIntegrationTest extends BaseSandboxIntegrationTest {
    final String val = "SUFFIX";

    @Test
    public void testAppendAfterDeleteOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        Append append = new Append(newRowId);
        // append to non existing column in orig
        append.add(CF2, COL1, Bytes.toBytes(val));
        append.add(CF2, COL2, Bytes.toBytes(val));

        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        // verify state
        assertEquals("original should remain intact", null,
                getCellValue(hTableOriginal, newRowId, CF2, COL2));

        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTableSandbox, newRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTableSandbox, newRowId, CF2, COL1));

        assertEquals("value should be inserted in mimic", val,
                getCellValue(hTableMimic, newRowId, CF2, COL2));
        assertEquals("value should be inserted in mimic", val,
                getCellValue(hTableMimic, newRowId, CF2, COL1));


        // delete the row
        delCell(hTableSandbox, newRowId, CF2, COL1);
        delCell(hTableMimic, newRowId, CF2, COL1);

        // re-append
        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableSandbox);
        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableMimic);

        pushSandbox();
        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableOriginal);
    }

    private void verifyFinalStateAppendAfterDeleteOnEmptyOriginal(HTable hTable) throws IOException {
        assertEquals("value should be inserted in sandbox", val+val,
                getCellValue(hTable, newRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTable, newRowId, CF2, COL1));
    }

    @Ignore
    @Test
    public void testAppendAfterDeleteOnFilledOriginal() throws IOException, SandboxException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        // CASE original empty, sandbox empty
        Append append = new Append(existingRowId);
        // append to non existing column in orig
        append.add(CF2, COL1, Bytes.toBytes(val));
        // append to existing column in orig
        append.add(CF2, COL2, Bytes.toBytes(val));

        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        // verify state
        assertEquals("original should remain intact", "someString",
                getCellValue(hTableOriginal, existingRowId, CF2, COL2));

        assertEquals("value should be inserted in sandbox", "someString"+val,
                getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTableSandbox, existingRowId, CF2, COL1));

        assertEquals("value should be inserted in mimic", "someString"+val,
                getCellValue(hTableMimic, existingRowId, CF2, COL2));
        assertEquals("value should be inserted in mimic", val,
                getCellValue(hTableMimic, existingRowId, CF2, COL1));

        // delete the column that exists in original
        delCell(hTableSandbox, existingRowId, CF2, COL2);
        delCell(hTableMimic, existingRowId, CF2, COL2);

        // re-append
        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        verifyFinalStateAppendAfterDeleteOnFilledOriginal(hTableSandbox);
        verifyFinalStateAppendAfterDeleteOnFilledOriginal(hTableMimic); // TODO failing here :O

        pushSandbox();
        verifyFinalStateAppendAfterDeleteOnFilledOriginal(hTableOriginal);
    }

    private void verifyFinalStateAppendAfterDeleteOnFilledOriginal(HTable hTable) throws IOException {
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTable, existingRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val+val,
                getCellValue(hTable, existingRowId, CF2, COL1));
    }




    @Test
    public void testAppendOnFilledOriginal() throws IOException, SandboxException {
        // CASE original filled, sandbox empty
        fillOriginalTable();

        Append append = new Append(existingRowId);
        // append to non existing column in orig
        append.add(CF2, COL1, Bytes.toBytes(val));
        // append to existing column in orig
        append.add(CF2, COL2, Bytes.toBytes(val));
        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        assertEquals("original should remain intact", "someString",
                getCellValue(hTableOriginal, existingRowId, CF2, COL2));
        assertEquals("value should be appended in sandbox", "someString"+val,
                getCellValue(hTableSandbox, existingRowId, CF2, COL2));
        assertEquals("value should be appended in mimic", "someString"+val,
                getCellValue(hTableMimic, existingRowId, CF2, COL2));
        assertEquals("value should be added in sandbox", val,
                getCellValue(hTableSandbox, existingRowId, CF2, COL1));
        assertEquals("value should be added in mimic", val,
                getCellValue(hTableMimic, existingRowId, CF2, COL1));


        // CASE: sandbox diverged from original
        setCellValue(hTableOriginal, existingRowId, CF2, COL2, "changedInOriginal");

        Append append2 = new Append(existingRowId);
        // append to non existing column in orig
        append2.add(CF1, COL2, Bytes.toBytes(val));
        // append to existing column in orig
        append2.add(CF2, COL2, Bytes.toBytes(val));
        hTableSandbox.append(append2);
        hTableMimic.append(append2);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        verifyFinalState(hTableMimic);
        verifyFinalState(hTableSandbox);

        pushSandbox();
        verifyFinalState(hTableOriginal);
    }

    private void verifyFinalState(HTable hTable) throws IOException {
        assertEquals(String.format("value should be appended to table %s", hTable.getName()),
                "someString"+val+val,
                getCellValue(hTable, existingRowId, CF2, COL2));
        assertEquals(String.format("value should be inserted to table %s", hTable.getName()), val,
                getCellValue(hTable, existingRowId, CF1, COL2));
    }

    @Test
    public void testAppendOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty
        Append append = new Append(newRowId);
        // append to non existing column in orig
        append.add(CF2, COL1, Bytes.toBytes(val));
        append.add(CF2, COL2, Bytes.toBytes(val));
        hTableSandbox.append(append);
        hTableMimic.append(append);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        assertEquals("original should remain intact", null,
                getCellValue(hTableOriginal, newRowId, CF2, COL2));

        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTableSandbox, newRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTableSandbox, newRowId, CF2, COL1));

        assertEquals("value should be inserted in mimic", val,
                getCellValue(hTableMimic, newRowId, CF2, COL2));
        assertEquals("value should be inserted in mimic", val,
                getCellValue(hTableMimic, newRowId, CF2, COL1));


        // CASE: original empty, sandbox filled (with previous append)
        Append append2 = new Append(newRowId);
        // append to non existing column in orig
        append2.add(CF1, COL2, Bytes.toBytes(val));
        // append to existing column in sandbox
        append2.add(CF2, COL2, Bytes.toBytes(val));
        hTableSandbox.append(append2);
        hTableMimic.append(append2);
        hTableSandbox.flushCommits();
        hTableMimic.flushCommits();

        verifyFinalState2(hTableMimic);
        verifyFinalState2(hTableSandbox);

        pushSandbox();
        verifyFinalState2(hTableOriginal);
    }

    private void verifyFinalState2(HTable hTable) throws IOException {
        assertEquals(String.format("value should be appended to table %s", hTable.getName()), val+val,
                getCellValue(hTable, newRowId, CF2, COL2));
        assertEquals(String.format("value should be inserted to table %s", hTable.getName()), val,
                getCellValue(hTable, newRowId, CF1, COL2));
    }

}
