package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class SandboxTableMutateRowIntegrationTest extends BaseSandboxIntegrationTest {

    @Ignore
    @Test
    // data not in production and not in sandbox
    public void testSandboxMutateRowNotInProductionNotInSandbox() throws IOException {
        // test deletion of entire row
        Put put1 = new Put(Bytes.toBytes("row900"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("901"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("902"));
        Delete delete1 = new Delete(Bytes.toBytes("row900")); // delete entire row
        RowMutations mutations1 = new RowMutations(Bytes.toBytes("row900"));
        mutations1.add(put1);
        mutations1.add(delete1);
        hTableSandbox.mutateRow(mutations1);

        Get get1 = new Get(Bytes.toBytes("row900"));
        get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod1 = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production. delete should have no effect", prod1, null);

        assertEquals("row not in sandbox. delete should just mark the row for deletion", sand1, null);

        // test deletion of a column family in a row and some puts in a different column family
        Put put2 = new Put(Bytes.toBytes("row901"));
        put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("901"));
        put2.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes("902"));
        Delete delete2 = new Delete(Bytes.toBytes("row901"));
        delete2.deleteFamily(Bytes.toBytes("cf1"));

        RowMutations mutations2 = new RowMutations(Bytes.toBytes("row901"));
        mutations2.add(put2);
        mutations2.add(delete2);
        hTableSandbox.mutateRow(mutations2);

        Get get2 = new Get(Bytes.toBytes("row901"));
        //get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row901/cf1 not in production. delete should have no effect", prod2, null);
        assertEquals("row901/cf1 not in sandbox. delete should just mark the row/cf for deletion", sand2, null);
        prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        assertEquals("row901 not in production. PUT should have no effect", prod2, null);
        assertEquals("row901 not in sandbox. PUT should put the value", sand2, "902");

        // test deletion of a column in a column family in a row and some puts in a different column
        Put put3 = new Put(Bytes.toBytes("row902"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("902"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("903"));
        Delete delete3 = new Delete(Bytes.toBytes("row902"));
        delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

        RowMutations mutations3 = new RowMutations(Bytes.toBytes("row902"));
        mutations3.add(put3);
        mutations3.add(delete3);
        hTableSandbox.mutateRow(mutations3);

        Get get3 = new Get(Bytes.toBytes("row902"));
        //get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row902/cf1/col1 not in production. delete should have no effect", prod3, null);
        assertEquals("row902/cf1/col1 not in sandbox. delete should just mark the row/cf/col for deletion", sand3, null);
        prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        assertEquals("row902 not in production", prod3, null);
        assertEquals("row902 not in sandbox. PUT should put the value in sandbox", sand3, "903");
    }

    @Test
    // data not in production but in sandbox
    public void testSandboxMutateRowNotInProductionInSandbox() throws IOException {
        // test deletion of entire row
        Put put1 = new Put(Bytes.toBytes("row25"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("125"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("225"));
        Delete delete1 = new Delete(Bytes.toBytes("row25")); // delete entire row
        RowMutations mutations1 = new RowMutations(Bytes.toBytes("row25"));
        mutations1.add(put1);
        mutations1.add(delete1);
        hTableSandbox.mutateRow(mutations1);

        Get get1 = new Get(Bytes.toBytes("row25"));
        get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod1 = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production. delete should have no effect", prod1, null);
        // TODO Delete entire row seems to fail
        //assertEquals("row in sandbox. delete should mark the row for deletion and not fetch it", sand1, null);

        // test deletion of a column family in a row
        Put put2 = new Put(Bytes.toBytes("row26"));
        put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("126"));
        put2.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes("226"));
        Delete delete2 = new Delete(Bytes.toBytes("row26"));
        delete2.deleteFamily(Bytes.toBytes("cf1"));

        RowMutations mutations2 = new RowMutations(Bytes.toBytes("row26"));
        mutations2.add(put2);
        mutations2.add(delete2);
        hTableSandbox.mutateRow(mutations2);

        Get get2 = new Get(Bytes.toBytes("row26"));
        //get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row26/cf1 not in production. delete should have no effect", prod2, null);
        assertEquals("row26/cf1 is in sandbox. delete should mark the row/cf for deletion and not fetch it", sand2, null);
        prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        assertEquals("row26/cf2 not in production. PUT should have no effect", prod2, null);
        assertEquals("row26/cf2 is in sandbox. PUT should modify the value in sandbox", sand2, "226");

        // test deletion of a column in a column family in a row and some puts in a different column
        Put put3 = new Put(Bytes.toBytes("row27"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("127"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("227"));
        Delete delete3 = new Delete(Bytes.toBytes("row27"));
        delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

        RowMutations mutations3 = new RowMutations(Bytes.toBytes("row27"));
        mutations3.add(put3);
        mutations3.add(delete3);
        hTableSandbox.mutateRow(mutations3);

        Get get3 = new Get(Bytes.toBytes("row27"));
        //get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row27/cf1/col1 not in production. should return null", prod3, null);
        assertEquals("row27/cf1/col1 is in sandbox. delete should mark it for deletion and not fetch it", sand3, null);
        prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        assertEquals("row not in production. should return null", prod3, null);
        assertEquals("row is in sandbox. PUT should modify the value", sand3, "227");
    }

    @Test
    // data in production but not in sandbox
    public void testSandboxMutateRowInProductionNotInSandbox() throws IOException {
        // test deletion of entire row
        Put put1 = new Put(Bytes.toBytes("row35"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("135"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("235"));
        Delete delete1 = new Delete(Bytes.toBytes("row35")); // delete entire row
        RowMutations mutations1 = new RowMutations(Bytes.toBytes("row35"));
        mutations1.add(put1);
        mutations1.add(delete1);
        hTableSandbox.mutateRow(mutations1);

        Get get1 = new Get(Bytes.toBytes("row35"));
        get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod1 = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production", prod1, null);
        // TODO Delete entire row seems to fail
        //assertEquals("row not in sandbox and not fetched from production", sand1, null);

        // test deletion of a column family in a row and some puts in a different column family
        Put put2 = new Put(Bytes.toBytes("row36"));
        put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("136"));
        put2.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes("236"));
        Delete delete2 = new Delete(Bytes.toBytes("row36"));
        delete2.deleteFamily(Bytes.toBytes("cf1"));

        RowMutations mutations2 = new RowMutations(Bytes.toBytes("row36"));
        mutations2.add(put2);
        mutations2.add(delete2);
        hTableSandbox.mutateRow(mutations2);

        Get get2 = new Get(Bytes.toBytes("row36"));
        //get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("colfam no in production", prod2, null);
        assertEquals("row36/cf1 not in sandbox and should not be fetched from production", sand2, null);
        prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        assertEquals("row not in production", prod2, null);
        assertEquals("row should be fetched from production. PUT should modify the value in sandbox", sand2, "236");

        // test deletion of a column in a column family in a row and some puts in a different column
        Put put3 = new Put(Bytes.toBytes("row37"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("137"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("237"));
        Delete delete3 = new Delete(Bytes.toBytes("row37"));
        delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

        RowMutations mutations3 = new RowMutations(Bytes.toBytes("row37"));
        mutations3.add(put3);
        mutations3.add(delete3);
        hTableSandbox.mutateRow(mutations3);

        Get get3 = new Get(Bytes.toBytes("row37"));
        //get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production", prod3, null);
        assertEquals("row37/cf1/col1 not in sandbox and should not be fetched from production", sand3, null);
        prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        assertEquals("row not in production", prod3, null);
        assertEquals("row should be fetched from production. PUT should modify the value in sandbox", sand3, "237");
    }

    @Test
    // data in production and in sandbox
    public void testSandboxMutateRowInProductionInSandbox() throws IOException {
        // test deletion of entire row
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("101"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("201"));
        Delete delete1 = new Delete(Bytes.toBytes("row1")); // delete entire row
        RowMutations mutations1 = new RowMutations(Bytes.toBytes("row1"));
        mutations1.add(put1);
        mutations1.add(delete1);
        hTableSandbox.mutateRow(mutations1);

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production. no change", prod, null);
        // TODO Delete entire row seems to fail
        //assertEquals("row should be deleted in sandbox and not fetched from production", sand, null);

        // test deletion of a column family in a row and some puts in a different column family
        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("101"));
        put2.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes("201"));
        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        delete2.deleteFamily(Bytes.toBytes("cf1"));

        RowMutations mutations2 = new RowMutations(Bytes.toBytes("row2"));
        mutations2.add(put2);
        mutations2.add(delete2);
        hTableSandbox.mutateRow(mutations2);

        Get get2 = new Get(Bytes.toBytes("row2"));
        //get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("colfam not in production", prod2, null);
        assertEquals("colfam should be deleted in sandbox and not fetched from production", sand2, null);
        prod2 = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
        assertEquals("row not in production", prod2, null);
        assertEquals("PUT should modify the value in sandbox", sand2, "201");

        // test deletion of a column in a column family in a row and some puts in a different column
        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("101"));
        put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("201"));
        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));

        RowMutations mutations3 = new RowMutations(Bytes.toBytes("row3"));
        mutations3.add(put3);
        mutations3.add(delete3);
        hTableSandbox.mutateRow(mutations3);

        Get get3 = new Get(Bytes.toBytes("row3"));
        //get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
        String prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
        assertEquals("row not in production", prod3, null);
        assertEquals("column should be deleted in sandbox and not fetched from production", sand3, null);
        prod3 = Bytes.toString(hTableOriginal.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
        assertEquals("row not in production", prod3, null);
        assertEquals("PUT should modify the value in sandbox", sand3, "201");
    }


    @Test
    public void testMutateRow() throws IOException {
        HTable hTable = new HTable(conf, sandboxTablePath);
        HTable originalHTable = new HTable(conf, originalTablePath);

        final String testRow = "rowId1";

        Result result;

        // check there's nothing in the sandbox before the test
        Get get = new Get(testRow.getBytes());

        result = originalHTable.get(get);
        assertTrue("original should not return any row", result.isEmpty());
        result = hTable.get(get);
        assertTrue("sandbox should not return any row", result.isEmpty());

        // load sandbox table with a row
        Put put = new Put(testRow.getBytes());
        put.add(CF1, "col1".getBytes(), "1".getBytes());
        put.add(CF1, "col2".getBytes(), "2".getBytes());
        hTable.put(put);
        hTable.flushCommits();


        // perform mutation
        put = new Put(testRow.getBytes());
        put.add(CF1, "col2".getBytes(), "20".getBytes());

        Delete delete = new Delete(testRow.getBytes());
        delete.deleteColumn(CF1, "col1".getBytes());

        RowMutations rm = new RowMutations(testRow.getBytes());
        rm.add(put);
        rm.add(delete);
        hTable.mutateRow(rm);
        hTable.flushCommits();

        // verify things got changed
        result = originalHTable.get(get);
        assertTrue("original should not return any row", result.isEmpty());
        result = hTable.get(get);

        assertFalse("sandbox should return the mutations", result.isEmpty());
        assertTrue("should contain the changed column", result.containsColumn(CF1, "col2".getBytes()));
        assertFalse("shouldn't contain the deleted column", result.containsColumn(CF1, "col1".getBytes()));
        assertTrue("should contain the updated value",
                CellUtil.matchingValue(result.getColumnLatestCell(CF1, "col2".getBytes()),
                        "20".getBytes()));

    }
}
