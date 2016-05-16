package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableMimic);
        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableSandbox);

        pushSandbox();
        verifyFinalStateAppendAfterDeleteOnEmptyOriginal(hTableOriginal);
    }

    private void verifyFinalStateAppendAfterDeleteOnEmptyOriginal(HTable hTable) throws IOException {
        assertEquals("value should be inserted in sandbox", val+val,
                getCellValue(hTable, newRowId, CF2, COL2));
        assertEquals("value should be inserted in sandbox", val,
                getCellValue(hTable, newRowId, CF2, COL1));
    }


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
        verifyFinalStateAppendAfterDeleteOnFilledOriginal(hTableMimic);

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

    final byte[] colA = "colA".getBytes();
    final byte[] colB = "colB".getBytes();

    @Test
    public void testConcurrentAppend() throws IOException {
        testConcurrentAppendForTable(hTableMimic, 0L);
        testConcurrentAppendForTable(hTableSandbox, 0L);
    }

    @Test
    public void testConcurrentIncrementOnFilledOriginal() throws IOException {
        // fill original
        long initialLength = 4L;
        setCellValue(hTableMimic, newRowId, CF1, colA, "test");
        setCellValue(hTableMimic, newRowId, CF1, colB, "t3st");
        setCellValue(hTableOriginal, newRowId, CF1, colA, "test");
        setCellValue(hTableOriginal, newRowId, CF1, colB, "t3st");

        testConcurrentAppendForTable(hTableMimic, initialLength);
        testConcurrentAppendForTable(hTableSandbox, initialLength);
    }

    private void testConcurrentAppendForTable(HTable hTable, long initialValue) throws IOException {
        final int SIMULATENOUS_TASKS = 6;

        // invoke simultaneous push
        List<Runnable> tasksList = new ArrayList<Runnable>(SIMULATENOUS_TASKS);

        final AtomicInteger errorCount = new AtomicInteger(0);
        final AtomicInteger lengthColA = new AtomicInteger(0);
        final AtomicInteger lengthColB = new AtomicInteger(0);

        // tasks to push sandboxes (will be spawned all at same time)
        for (int t = 0; t < SIMULATENOUS_TASKS; t++) {
            tasksList.add(getAppendTask(t, hTable, lengthColA, lengthColB, errorCount));
        }

        int maxTimeoutSeconds = 300;
        try {
            assertConcurrent("increment should be atomic", tasksList, maxTimeoutSeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        String valueColA = getCellValue(hTable, newRowId, CF1, colA);
        String valueColB = getCellValue(hTable, newRowId, CF1, colB);

        assertTrue("colA only contains 'a'", valueColA.matches("a+"));
        assertTrue("colB only contains 'B'", valueColB.matches("B+"));
        assertEquals(lengthColA.get()+initialValue, valueColA.length());
        assertEquals(lengthColB.get()+initialValue, valueColB.length());
        assertEquals(0, errorCount.get());
    }

    private Runnable getAppendTask(final int seed, final HTable hTable,
                                      final AtomicInteger lengthColA,
                                      final AtomicInteger lengthColB,
                                      final AtomicInteger errorCount) {
        return new Runnable() {
            @Override
            public void run() {
                final Random rnd = new Random(seed);

                for (int i = 0; i < 200; i++) {
                    int lengthValA = rnd.nextInt(10);
                    int lengthValB = rnd.nextInt(20);

                    byte[] valueA = new byte[lengthValA];
                    byte[] valueB = new byte[lengthValB];
                    Arrays.fill(valueA, (byte) 'a');
                    Arrays.fill(valueB, (byte) 'B');

                    Append append = new Append(newRowId);
                    append.add(CF1, colA, valueA);
                    append.add(CF1, colB, valueB);

                    try {
                        hTable.append(append);
                        lengthColA.addAndGet(lengthValA);
                        lengthColB.addAndGet(lengthValB);
                    } catch (IOException e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    }
                }
            }
        };
    }
}
