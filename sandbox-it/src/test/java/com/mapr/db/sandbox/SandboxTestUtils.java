package com.mapr.db.sandbox;

import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

    public static String getCellValue(HTable hTable, byte[] rowId, byte[] columnFamily, byte[] columnQualifier) throws IOException {
        Get get = new Get(rowId);
        return Bytes.toString(hTable.get(get).getValue(columnFamily, columnQualifier));
    }

    public static void setCellValue(HTable hTable, byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String value) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(rowId);
        put.add(columnFamily, columnQualifier, Bytes.toBytes(value));
        hTable.put(put);
        hTable.flushCommits();
    }

    public static void delCell(HTable hTable, byte[] rowId, byte[] columnFamily, byte[] columnQualifier) throws IOException {
        Delete delete = new Delete(rowId);
        delete.deleteColumn(columnFamily, columnQualifier);
        hTable.delete(delete);
        hTable.flushCommits();
    }

    public static void delFamily(HTable hTable, byte[] rowId, byte[] columnFamily) throws IOException {
        Delete delete = new Delete(rowId);
        delete.deleteFamily(columnFamily);
        hTable.delete(delete);
        hTable.flushCommits();
    }

    public static void delRow(HTable hTable, byte[] rowId) throws IOException {
        Delete delete = new Delete(rowId);
        hTable.delete(delete);
        hTable.flushCommits();
    }
}
