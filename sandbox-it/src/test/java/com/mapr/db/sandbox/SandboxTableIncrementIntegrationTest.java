package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxTableIncrementIntegrationTest extends BaseSandboxIntegrationTest {
    @Test
    public void testIncrementOnEmptyOriginal() throws IOException, SandboxException {
        // CASE original empty, sandbox empty

        // verify there's nothing in the tables
        Result result;
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have no rows", 0L, countRows(origResults));
        assertEquals("sandbox table should have no rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have no rows", 0L, countRows(mimicResults));

        // increment non-existing cell
        result = incrCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(1, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));
        assertEquals(1, getCellLongValue(hTableMimic, newRowId, CF1, COL1));

        result = incrCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(1, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));
        assertEquals(1, getCellLongValue(hTableSandbox, newRowId, CF1, COL1));

        // CASE original empty, sandbox filled (from previous increment)

        // increment now existing cell
        result = incrCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(2, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));
        assertEquals(2, getCellLongValue(hTableMimic, newRowId, CF1, COL1));

        result = incrCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(2, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));
        assertEquals(2, getCellLongValue(hTableSandbox, newRowId, CF1, COL1));

        // increment non integer cell

        // delete cell
        delCell(hTableMimic, newRowId, CF1, COL1);
        delCell(hTableSandbox, newRowId, CF1, COL1);

        // increment after delete
        result = incrCell(hTableMimic, newRowId, CF1, COL1, 1);
        assertEquals(1, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));

        result = incrCell(hTableSandbox, newRowId, CF1, COL1, 1);
        assertEquals(1, Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(CF1, COL1))));

        verifyIncrAfterDeleteState(hTableMimic);
        verifyIncrAfterDeleteState(hTableSandbox);

        pushSandbox();

        verifyIncrAfterDeleteState(hTableOriginal);
    }

    private void verifyIncrAfterDeleteState(HTable hTable) throws IOException {
        assertEquals(1, getCellLongValue(hTable, newRowId, CF1, COL1));
    }

    @Test
    public void testAllOrNothingIncrement() throws IOException {
        testAllOrNothingIncrementForTable(hTableMimic);
        testAllOrNothingIncrementForTable(hTableSandbox);
    }

    private void testAllOrNothingIncrementForTable(HTable hTable) throws IOException {
        // make sure hTable has empty cells
        Increment increment = new Increment(newRowId);
        increment.addColumn(CF1, COL1, 2);
        increment.addColumn(CF1, COL2, 2);
        increment.addColumn(CF2, COL1, 2);
        increment.addColumn(CF2, COL2, 2);
        hTable.increment(increment);

        assertEquals(2, getCellLongValue(hTable, newRowId, CF1, COL1));
        assertEquals(2, getCellLongValue(hTable, newRowId, CF1, COL2));
        assertEquals(2, getCellLongValue(hTable, newRowId, CF2, COL1));
        assertEquals(2, getCellLongValue(hTable, newRowId, CF2, COL2));

        setCellValue(hTable, newRowId, CF1, COL2, "sosasd");

        // increment will fail
        try {
            hTable.increment(increment);
        } catch (Exception ex) {
            ex.getMessage();
            System.out.println();
        }

        // and only int cells should have been updated
        assertEquals(4, getCellLongValue(hTable, newRowId, CF1, COL1));
        assertEquals("sosasd", getCellValue(hTable, newRowId, CF1, COL2));
        assertEquals(4, getCellLongValue(hTable, newRowId, CF2, COL1));
        assertEquals(4, getCellLongValue(hTable, newRowId, CF2, COL2));
    }




    final byte[] colA = "colA".getBytes();
    final byte[] colB = "colB".getBytes();

    @Test
    public void testConcurrentIncrement() throws IOException {
        testConcurrentIncrementForTable(hTableMimic, 0L);
        testConcurrentIncrementForTable(hTableSandbox, 0L);
    }

    @Test
    public void testConcurrentIncrementOnFilledOriginal() throws IOException {
        // fill original
        long initialValue = 4L;
        Put put = new Put(newRowId);
        put.add(CF1, colA, Bytes.toBytes(initialValue));
        put.add(CF1, colB, Bytes.toBytes(initialValue));

        hTableMimic.put(put);
        hTableMimic.flushCommits();
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        testConcurrentIncrementForTable(hTableMimic, initialValue);
        testConcurrentIncrementForTable(hTableSandbox, initialValue);
    }

    private void testConcurrentIncrementForTable(HTable hTable, long initialValue) throws IOException {
        final int SIMULATENOUS_TASKS = 6;

        // invoke simultaneous push
        List<Runnable> tasksList = new ArrayList<Runnable>(SIMULATENOUS_TASKS);

        final AtomicInteger errorCount = new AtomicInteger(0);
        final AtomicInteger incrementColA = new AtomicInteger(0);
        final AtomicInteger incrementColB = new AtomicInteger(0);

        // tasks to push sandboxes (will be spawned all at same time)
        for (int t = 0; t < SIMULATENOUS_TASKS; t++) {
            tasksList.add(getIncrementTask(t, hTable, incrementColA, incrementColB, errorCount));
        }

        int maxTimeoutSeconds = 300;
        try {
            assertConcurrent("increment should be atomic", tasksList, maxTimeoutSeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        assertEquals(incrementColA.get()+initialValue, getCellLongValue(hTable, newRowId, CF1, colA));
        assertEquals(incrementColB.get()+initialValue, getCellLongValue(hTable, newRowId, CF1, colB));
        assertEquals(0, errorCount.get());
    }

    private Runnable getIncrementTask(final int seed, final HTable hTable,
                                      final AtomicInteger incrementColA,
                                      final AtomicInteger incrementColB,
                                      final AtomicInteger errorCount) {
        return new Runnable() {
            @Override
            public void run() {
                final Random rnd = new Random(seed);

                for (int i = 0; i < 200; i++) {
                    int amountA = rnd.nextInt(100)-200;
                    int amountB = rnd.nextInt(100)-200;
                    Increment incr = new Increment(newRowId);
                    incr.addColumn(CF1, colA, amountA);
                    incr.addColumn(CF1, colB, amountB);

                    try {
                        hTable.increment(incr);
                        incrementColA.addAndGet(amountA);
                        incrementColB.addAndGet(amountB);
                    } catch (IOException e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    }
                }
            }
        };
    }
}
