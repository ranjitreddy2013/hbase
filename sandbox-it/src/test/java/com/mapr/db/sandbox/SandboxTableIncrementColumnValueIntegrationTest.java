package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxTableIncrementColumnValueIntegrationTest extends BaseSandboxIntegrationTest {
    @Test
    public void testIncrementOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty

        // verify there's nothing in the tables
        long result;
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 0L, countRows(mimicResults));

        // increment non-existing cell
        result = incrColValueCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(1, result);
        assertEquals(1, getCellLongValue(hTableMimic, newRowId, CF1, COL1));

        result = incrColValueCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(1, result);
        assertEquals(1, getCellLongValue(hTableSandbox, newRowId, CF1, COL1));

        // CASE original empty, sandbox filled (from previous increment)

        // increment now existing cell
        result = incrColValueCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(2, result);
        assertEquals(2, getCellLongValue(hTableMimic, newRowId, CF1, COL1));

        result = incrColValueCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(2, result);
        assertEquals(2, getCellLongValue(hTableSandbox, newRowId, CF1, COL1));

        // increment non integer cell

        // delete cell
        delCell(hTableMimic, newRowId, CF1, COL1);
        delCell(hTableSandbox, newRowId, CF1, COL1);

        // increment after delete
        result = incrColValueCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(1, result);

        result = incrColValueCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(1, result);

        verifyIncrAfterDeleteState(hTableMimic);
        verifyIncrAfterDeleteState(hTableSandbox);

        pushSandbox();

        verifyIncrAfterDeleteState(hTableOriginal);
    }

    private void verifyIncrAfterDeleteState(HTable hTable) throws IOException {
        assertEquals(1, getCellLongValue(hTable, newRowId, CF1, COL1));
    }

    @Test
    public void testIncrementColumnValue() throws IOException {
        testIncrementColumnValueForTable(hTableMimic);
        testIncrementColumnValueForTable(hTableSandbox);
    }

    private void testIncrementColumnValueForTable(HTable hTable) throws IOException {
        // make sure hTable has empty cells
        hTable.incrementColumnValue(newRowId, CF1, COL1, 2);
        hTable.incrementColumnValue(newRowId, CF1, COL2, 3);

        assertEquals(2, getCellLongValue(hTable, newRowId, CF1, COL1));
        assertEquals(3, getCellLongValue(hTable, newRowId, CF1, COL2));

        setCellValue(hTable, newRowId, CF1, COL2, "sosasd");

        // increment will fail
        try {
            hTable.incrementColumnValue(newRowId, CF1, COL2, 2);
        } catch (Exception ex) {
            ex.getMessage();
            System.out.println();
        }

        // and only int cells should have been updated
        assertEquals(2, getCellLongValue(hTable, newRowId, CF1, COL1));
        assertEquals("sosasd", getCellValue(hTable, newRowId, CF1, COL2));
    }




    final byte[] colA = "colA".getBytes();

    @Test
    public void testConcurrentIncrementColumnValue() throws IOException {
        testConcurrentIncrementColumnValueForTable(hTableMimic, 0L);
        testConcurrentIncrementColumnValueForTable(hTableSandbox, 0L);
    }

    @Test
    public void testConcurrentIncrementColumnValueOnFilledOriginal() throws IOException {
        // fill original
        long initialValue = 4L;
        Put put = new Put(newRowId);
        put.add(CF1, colA, Bytes.toBytes(initialValue));

        hTableMimic.put(put);
        hTableMimic.flushCommits();
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        testConcurrentIncrementColumnValueForTable(hTableMimic, initialValue);
        testConcurrentIncrementColumnValueForTable(hTableSandbox, initialValue);
    }

    private void testConcurrentIncrementColumnValueForTable(HTable hTable, long initialValue) throws IOException {
        final int SIMULATENOUS_TASKS = 6;

        // invoke simultaneous push
        List<Runnable> tasksList = new ArrayList<Runnable>(SIMULATENOUS_TASKS);

        final AtomicInteger errorCount = new AtomicInteger(0);
        final AtomicInteger incrementCounter = new AtomicInteger(0);

        // tasks to push sandboxes (will be spawned all at same time)
        for (int t = 0; t < SIMULATENOUS_TASKS; t++) {
            tasksList.add(getIncrementColumnValueTask(t, hTable, incrementCounter, errorCount));
        }

        int maxTimeoutSeconds = 300;
        try {
            assertConcurrent("increment should be atomic", tasksList, maxTimeoutSeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(incrementCounter.get()+initialValue, getCellLongValue(hTable, newRowId, CF1, colA));
        assertEquals(0, errorCount.get());
    }

    private Runnable getIncrementColumnValueTask(final int seed, final HTable hTable,
                                      final AtomicInteger incrementCounter,
                                      final AtomicInteger errorCount) {
        return new Runnable() {
            @Override
            public void run() {
                final Random rnd = new Random(seed);

                for (int i = 0; i < 200; i++) {
                    int amount = rnd.nextInt(100)-200;

                    try {
                        hTable.incrementColumnValue(newRowId, CF1, colA, amount);
                        incrementCounter.addAndGet(amount);
                    } catch (IOException e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    }
                }
            }
        };
    }
}
