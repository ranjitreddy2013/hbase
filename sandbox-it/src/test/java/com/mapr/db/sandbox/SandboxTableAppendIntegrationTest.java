package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SandboxTableAppendIntegrationTest extends BaseSandboxIntegrationTest {
    final byte[] rowId = Bytes.toBytes("row900");
    final byte[] col1 = Bytes.toBytes("col1");

    @Test
    public void testAppendDataNotInOriginalNotInSandbox() throws IOException {
        // data not in original and not in sandbox
        final String val = "v900";

        // test appending to the value of a new row
        Append append1 = new Append(rowId);
        append1.add(CF1, col1, Bytes.toBytes(val));
        hTableSandbox.append(append1);
        hTableSandbox.flushCommits();

        Get get1 = new Get(rowId);
        get1.addColumn(CF1, col1);
        String orig = Bytes.toString(hTableOriginal.get(get1).getValue(CF1, col1));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(CF1, col1));
        assertEquals("value not in original. should return null.", orig, null);
        assertEquals("value should be appended as is in sandbox", sand, val);
    }

    @Test
    public void testAppendDataNotInOriginalNotInSandbox2() throws IOException {
        // data not in original and not in sandbox
        final String val = "v900";

        // create existing column
        Put put = new Put(rowId);
        put.add(CF1, Bytes.toBytes("col0"), Bytes.toBytes("some value"));
        hTableSandbox.put(put);

        // test appending to the value of a new column
        Append append1 = new Append(rowId);
        append1.add(CF1, col1, Bytes.toBytes(val));
        hTableSandbox.append(append1);
        hTableSandbox.flushCommits();

        Get get1 = new Get(rowId);
        get1.addColumn(CF1, col1);
        String orig = Bytes.toString(hTableOriginal.get(get1).getValue(CF1, col1));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(CF1, col1));
        assertEquals("value not in original. should return null.", orig, null);
        assertEquals("value should be appended as is in sandbox", sand, val);
    }


    @Test
    // data not in original but in sandbox
    public void testSandboxAppendNotInOriginalInSandbox() throws IOException {
        final String val = "v125";
        final String prefix = "someprefix";

        // insert some data
        Put put = new Put(rowId);
        put.add(CF1, col1, Bytes.toBytes(prefix));
        hTableSandbox.put(put);
        hTableSandbox.flushCommits();

        // test appending to the value of an already existing column
        Append append1 = new Append(rowId);
        append1.add(CF1, col1, Bytes.toBytes(val));
        hTableSandbox.append(append1);
        hTableSandbox.flushCommits();

        Get get1 = new Get(rowId);
        get1.addColumn(CF1, col1);
        String orig = Bytes.toString(hTableOriginal.get(get1).getValue(CF1, col1));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(CF1, col1));
        assertEquals("value not in original. should return null.", orig, null);
        assertEquals("value should be appended to the existing value in sandbox", sand, prefix + val);
    }

    @Test
    public void testSandboxAppendInOriginalNotInSandbox() throws IOException {
        // data in original but not in sandbox
        final String val = "v135";
        final String prefix = "someprefix";

        // populate original
        // insert some data
        Put put = new Put(rowId);
        put.add(CF1, col1, Bytes.toBytes(prefix));
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        // test appending to the value in sandbox
        Append append1 = new Append(rowId);
        append1.add(CF1, col1, Bytes.toBytes(val));
        hTableSandbox.append(append1);
        hTableSandbox.flushCommits();

        Get get1 = new Get(rowId);
        get1.addColumn(CF1, col1);
        String orig = Bytes.toString(hTableOriginal.get(get1).getValue(CF1, col1));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(CF1, col1));
        assertEquals("value should not be appended in original", orig, prefix);
        assertEquals("value should be fetched from original and appended in sandbox", sand, prefix+val);
    }

    @Test
    public void testSandboxAppendInOriginalInSandbox() throws IOException {
        // data in original and in sandbox
        final String val = "v135";
        final String prefix = "someprefix";
        final String prefixS = "tralala";

        // populate original
        Put put = new Put(rowId);
        put.add(CF1, col1, Bytes.toBytes(prefix));
        hTableOriginal.put(put);
        hTableOriginal.flushCommits();

        // populate sandbox
        Put putS = new Put(rowId);
        putS.add(CF1, col1, Bytes.toBytes(prefixS));
        hTableSandbox.put(putS);
        hTableSandbox.flushCommits();

        // test appending to the value in sandbox
        Append append1 = new Append(rowId);
        append1.add(CF1, col1, Bytes.toBytes(val));
        hTableSandbox.append(append1);
        hTableSandbox.flushCommits();

        Get get1 = new Get(rowId);
        get1.addColumn(CF1, col1);
        String orig = Bytes.toString(hTableOriginal.get(get1).getValue(CF1, col1));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(CF1, col1));
        assertEquals("value should not be appended in original", orig, prefix);
        assertEquals("value should be appended to the sandbox", sand, prefixS+val);
    }
}
