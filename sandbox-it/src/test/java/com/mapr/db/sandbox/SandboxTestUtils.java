package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
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
            ++result;
        }
        return result;
    }
}
