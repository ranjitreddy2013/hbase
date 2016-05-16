package com.mapr.db.sandbox;

import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class SandboxTestUtils {
    public static List<String> getColumnFamilies(HTableInterface table) {
        List<String> colFamilies = new ArrayList<String>();
        try {
            Set<byte[]> familySet = table.getTableDescriptor().getFamiliesKeys();
            for (byte[] family : familySet) {
                colFamilies.add(Bytes.toString(family));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return colFamilies;
    }

    public static long countRows(ResultScanner scanner) throws IOException {
        long result = 0L;
        for (Result r : scanner) {
            if (r.isEmpty()) {
                continue;
            }
            ++result;
        }
        return result;
    }

    public static long countCells(ResultScanner scanner) throws IOException {
        long result = 0L;
        for (Result r : scanner) {
            if (r.isEmpty()) {
                continue;
            }

            for (Cell cell : r.rawCells()) {
                ++result;
            }
        }
        return result;
    }

    public static int countTrue(Boolean[] results) {
        int i = 0;
        for (Boolean result : results) {
            if (result) {
                ++i;
            }
        }
        return i;
    }

    public static void assureWorkingDirExists(MapRFileSystem fs, String tablePrefix) throws IOException {
        Path tableDirPath = new Path(tablePrefix);
        if (!fs.exists(tableDirPath)) {
            fs.mkdirs(tableDirPath);
        }
    }

    public static String getCellValue(HTable hTable, byte[] rowId, byte[] family, byte[] qualifier) throws IOException {
        Get get = new Get(rowId);
        get.addColumn(family, qualifier);
        return Bytes.toString(hTable.get(get).getValue(family, qualifier));
    }

    public static void setCellValue(HTable hTable, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(rowId);
        put.add(columnFamily, columnQualifier, Bytes.toBytes(value));
        hTable.put(put);
        hTable.flushCommits();
    }

    public static void delCell(HTable hTable, byte[] rowId, byte[] family, byte[] qualifier) throws IOException {
        Delete delete = new Delete(rowId);
        delete.deleteColumns(family, qualifier);
        hTable.delete(delete);
        hTable.flushCommits();
    }

    public static void delFamily(HTable hTable, byte[] rowId, byte[] family) throws IOException {
        Delete delete = new Delete(rowId);
        delete.deleteFamily(family);
        hTable.delete(delete);
        hTable.flushCommits();
    }

    public static void delRow(HTable hTable, byte[] rowId) throws IOException {
        Delete delete = new Delete(rowId);
        hTable.delete(delete);
        hTable.flushCommits();
    }

    public static long incrColValueCell(HTable hTable, byte[] row, byte[] family, byte[] qualifier, long incValue) throws IOException {
        return hTable.incrementColumnValue(row, family, qualifier, incValue);
    }

    public static Result incrCell(HTable hTable, byte[] row, byte[] family, byte[] qualifier, long incValue) throws IOException {
        Increment increment = new Increment(row);
        increment.addColumn(family, qualifier, incValue);
        Result result = hTable.increment(increment);
        return result;
    }

    public static long getCellLongValue(HTable hTable, byte[] rowId, byte[] family, byte[] qualifier) throws IOException {
        Get get = new Get(rowId);
        get.addColumn(family, qualifier);
        return Bytes.toLong(hTable.get(get).getValue(family, qualifier));
    }

    public static void assertConcurrent(final String message, final List<? extends Runnable> runnables,
                                        final int maxTimeoutSeconds) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(new Runnable() {
                    public void run() {
                        allExecutorThreadsReady.countDown();
                        try {
                            afterInitBlocker.await();
                            submittedTestRunnable.run();
                        } catch (final Throwable e) {
                            exceptions.add(e);
                        } finally {
                            allDone.countDown();
                        }
                    }
                });
            }

            // wait until all threads are ready
            assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent",
                    allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));

            // start all test runners
            afterInitBlocker.countDown();
            assertTrue(message +" timeout! More than " + maxTimeoutSeconds + " seconds",
                    allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
        } finally {
            threadPool.shutdownNow();
        }
        assertTrue(message + " failed with exception(s)" + exceptions, exceptions.isEmpty());
    }
}
