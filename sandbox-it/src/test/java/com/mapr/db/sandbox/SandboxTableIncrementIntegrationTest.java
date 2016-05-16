package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxTableIncrementIntegrationTest extends BaseSandboxIntegrationTest {
    Scan scan = new Scan();

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



    @Ignore
    @Test
    public void testSandboxIncrementNotInProductionNotInSandbox() throws IOException {
        // data not in production and not in sandbox
        final byte[] rowId = Bytes.toBytes("row35");

        // test incrementing the value of an already existing column
        // TODO

        // test incrementing the value of a new column
        Increment increment2 = new Increment(rowId);
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col102"), 1);
        hTableSandbox.increment(increment2);

        Get get2 = new Get(rowId);
        get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
//        long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
        long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
//        assertEquals("row is not in production. should return null.", prod, null);
        assertEquals("row is not in sandbox. value should be initialized to 1", sand, 1L);

        // test deletion marker in meta _shadow column family
        Get get = new Get(Bytes.toBytes("row1"));
        get.addFamily(DEFAULT_META_CF);
        // TODO query the _shadow table for checking deletionMark
        //resultSandbox = hTableSandbox.get(get);
        //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
        //assertEquals("value should be null for production", sand, null);
        //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
        //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
    }

    @Ignore
    @Test
    // data not in production but in sandbox
    public void testSandboxIncrementNotInProductionInSandbox() throws IOException {
        // test incrementing the value of an already existing column
        // TODO

        // test incrementing the value of a new column
        Increment increment2 = new Increment(Bytes.toBytes("row35"));
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col102"), 1);
        hTableSandbox.increment(increment2);
        Get get2 = new Get(Bytes.toBytes("row35"));
        get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
//        long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
        long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
//        assertEquals("row is not in production. should return null.", prod, null);
        assertEquals("row is in sandbox. new column. value should be initialized to 1", sand, 1L);

        // test deletion marker in meta _shadow column family
        Get get = new Get(Bytes.toBytes("row1"));
        get.addFamily(DEFAULT_META_CF);
        // TODO query the _shadow table for checking deletionMark
        //resultSandbox = hTableSandbox.get(get);
        //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
        //assertEquals("value should be null for production", sand, null);
        //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
        //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
    }

    @Ignore
    @Test
    // data in production but not in sandbox
    public void testSandboxIncrementInProductionNotInSandbox() throws IOException {
        // test incrementing the value of an already existing column
        // TODO

        // test incrementing the value of a new column
        Increment increment2 = new Increment(Bytes.toBytes("row35"));
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col102"), 1);
        hTableSandbox.increment(increment2);
        Get get2 = new Get(Bytes.toBytes("row35"));
        get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
//        long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
        long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
//        assertEquals("row is in production, but a new column. should return null.", prod, null);
        assertEquals("row not in sandbox. should be fetched from production and value initialized to 1", sand, 1L);

        // test deletion marker in meta _shadow column family
        Get get = new Get(Bytes.toBytes("row1"));
        get.addFamily(DEFAULT_META_CF);
        // TODO query the _shadow table for checking deletionMark
        //resultSandbox = hTableSandbox.get(get);
        //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
        //assertEquals("value should be null for production", sand, null);
        //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
        //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
    }

    @Ignore
    @Test
    // data in production and in sandbox
    public void testSandboxIncrementInProductionInSandbox() throws IOException {
        // test incrementing the value of an already existing column
    /*long cnt1 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), 1);
    assertEquals("value should be incremented by 1 in sandbox", cnt1, 2L);
    long cnt2 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), 1);
    assertEquals("value should be incremented by 1 more in sandbox", cnt2, 3L);
    long cnt3 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), -1);
    assertEquals("value should be decremented by 1 in sandbox", cnt3, 2L);
    long cnt4 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), 100);
    assertEquals("value should be incremented by 100 in sandbox", cnt4, 102L);
    long cnt5 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), -99);
    assertEquals("value should be decremented by 99 in sandbox", cnt5, 3L);
    long cnt6 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), 0);
    assertEquals("value should remain as is when decremented by 0 in sandbox", cnt6, 3L);


    Get get1 = new Get(Bytes.toBytes("row1"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("value in production should not be affected by any of the sandbox increments", prod, 1L);
    assertEquals("value in sandbox after all increment operations", sand, 1L); */


        // test incrementing the value of a new column
        Increment increment2 = new Increment(Bytes.toBytes("row1"));
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
        increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col102"), 1);
        hTableSandbox.increment(increment2);
        Get get2 = new Get(Bytes.toBytes("row1"));
        get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
//        long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
        long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
//        assertEquals("value not in production. should return null.", prod, null);
        assertEquals("value should initialize to 1", sand, 1L);

        //assertEquals("value should initialize to 1 in sandbox", cnt, 1L);

    /*long cnt = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should initialize to 1 in sandbox", cnt, 1L);
    long cnt1 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should be incremented by 1 in sandbox", cnt1, 2L);
    long cnt2 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should be incremented by 1 more in sandbox", cnt2, 3L);
    long cnt3 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), -1);
    assertEquals("value should be decremented by 1 in sandbox", cnt3, 2L);
    long cnt4 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 100);
    assertEquals("value should be incremented by 100 in sandbox", cnt4, 102L);
    long cnt5 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), -99);
    assertEquals("value should be decremented by 99 in sandbox", cnt5, 3L);
    long cnt6 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 0);
    assertEquals("value should remain as is when decremented by 0 in sandbox", cnt6, 3L);*/
/*
    Get get2 = new Get(Bytes.toBytes("row1"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
    String prod = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    String sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("value in sandbox after all increment operations", sand, 3L);
*/
        // test deletion marker in meta _shadow column family
        Get get = new Get(Bytes.toBytes("row1"));
        get.addFamily(DEFAULT_META_CF);
        // TODO query the _shadow table for checking deletionMark
        //resultSandbox = hTableSandbox.get(get);
        //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
        //assertEquals("value should be null for production", sand, null);
        //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
        //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get)); */
    }



    final byte[] colA = "colA".getBytes();
    final byte[] colB = "colB".getBytes();

    @Test
    public void testConcurrentIncrement() throws IOException {
        testConcurrentIncrementForTable(hTableMimic);
        testConcurrentIncrementForTable(hTableSandbox);
    }

    @Test
    public void testConcurrentIncrementOnFilledOriginal() throws IOException {
        // fill original
        Put put = new Put(newRowId);
        put.add(CF1, colA, Bytes.toBytes(0L));
        put.add(CF1, colB, Bytes.toBytes(0L));

        hTableMimic.put(put);
        hTableMimic.flushCommits();
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        testConcurrentIncrementForTable(hTableMimic);
        testConcurrentIncrementForTable(hTableSandbox);
    }

    private void testConcurrentIncrementForTable(HTable hTable) throws IOException {
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


        assertEquals(incrementColA.get(), getCellLongValue(hTable, newRowId, CF1, colA));
        assertEquals(incrementColB.get(), getCellLongValue(hTable, newRowId, CF1, colB));
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
