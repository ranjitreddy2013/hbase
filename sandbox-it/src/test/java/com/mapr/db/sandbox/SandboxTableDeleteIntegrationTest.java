package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;

import static com.mapr.db.sandbox.SandboxTestUtils.countRows;
import static com.mapr.db.sandbox.SandboxTestUtils.setCellValue;
import static org.junit.Assert.assertEquals;

public class SandboxTableDeleteIntegrationTest extends BaseSandboxIntegrationTest {
    Scan scan = new Scan();

    final byte[] testRowId1 = "aRowId1".getBytes();
    final byte[] testRowId2 = "aRowId2".getBytes();
    final byte[] testRowId3 = "aRowId3".getBytes();

    final Delete deleteCol, deleteFam, deleteRow;

    public SandboxTableDeleteIntegrationTest() {
        // delete entire col row1
        deleteCol = new Delete(testRowId1);
        deleteCol.deleteColumn(CF1, COL1);

        // delete entire fam row2
        deleteFam = new Delete(testRowId2);
        deleteFam.deleteFamily(CF2);

        // delete entire row row3
        deleteRow = new Delete(testRowId3);
    }


    @Ignore
    @Test
    public void testDeleteOnEmptyOriginal() throws IOException {
        // verify there's nothing on any table
        // scan all rows and count
        ResultScanner origResults, sandResults, mimicResults;

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have rows", 0L, countRows(mimicResults));

        // delete things in the empty sandbox table (and mimic)
        hTableSandbox.delete(deleteCol);
        hTableSandbox.delete(deleteFam);
        hTableSandbox.delete(deleteRow);
        hTableSandbox.flushCommits();

        hTableMimic.delete(deleteCol);
        hTableMimic.delete(deleteFam);
        hTableMimic.delete(deleteRow);
        hTableMimic.flushCommits();

        // assert same result
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 0L, countRows(sandResults));
        assertEquals("mimic table should have rows", 0L, countRows(mimicResults));

        // CASE: original empty, sandbox filled
        // load data into sandbox
        setCellValue(hTableSandbox, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableSandbox, testRowId1, CF1, COL2, "v2");
        setCellValue(hTableSandbox, testRowId2, CF2, COL1, "v3");
        setCellValue(hTableSandbox, testRowId2, CF2, COL1, "v4");
        setCellValue(hTableSandbox, testRowId3, CF1, COL1, "v5");
        setCellValue(hTableSandbox, testRowId3, CF2, COL1, "v6");

        setCellValue(hTableMimic, testRowId1, CF1, COL1, "v1");
        setCellValue(hTableMimic, testRowId1, CF1, COL2, "v2");
        setCellValue(hTableMimic, testRowId2, CF2, COL1, "v3");
        setCellValue(hTableMimic, testRowId2, CF2, COL1, "v4");
        setCellValue(hTableMimic, testRowId3, CF1, COL1, "v5");
        setCellValue(hTableMimic, testRowId3, CF2, COL1, "v6");

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 3L, countRows(sandResults));
        assertEquals("mimic table should have rows", 3L, countRows(mimicResults));

        // repeat the deletions
        hTableSandbox.delete(deleteCol);
        hTableSandbox.delete(deleteFam);
        hTableSandbox.delete(deleteRow);
        hTableSandbox.flushCommits();

        hTableMimic.delete(deleteCol);
        hTableMimic.delete(deleteFam);
        hTableMimic.delete(deleteRow);
        hTableMimic.flushCommits();

        // push
        // TODO finish
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        mimicResults = hTableMimic.getScanner(scan);
        assertEquals("original table should have rows", 0L, countRows(origResults));
        assertEquals("sandbox table should return rows", 3L, countRows(sandResults));
        assertEquals("mimic table should have rows", 3L, countRows(mimicResults));

        // assert it's all clean
    }

    // TODO
    @Ignore
    @Test
    public void testDeleteOnFilledOriginal() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        loadData(hTableOriginal);
        loadData(hTableMimic);

        // verify there's something on original


        // CASe 2 insert things into sandbox
        // delete entire col row1
        // delete entire fam row2
        // delete entire row row3

        // assert results on sandbox and nothing changed on original


        // repeat and assert

        // push

        // assert it's all clean
    }

    // TODO batch delete

//  @Test
//  // data not in production and not in sandbox
//  public void testSandboxDeleteNotInProductionNotInSandbox() throws IOException {
//  // test deletion of entire row
//  Delete delete1 = new Delete(Bytes.toBytes("row900")); // delete entire row
//  hTableSandbox.delete(delete1);
//  Get get1 = new Get(Bytes.toBytes("row900"));
//  get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//  String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  assertEquals("row not in production. delete should have no effect", prod, null);
//  assertEquals("row not in sandbox. delete should just mark the row for deletion", sand, null);
//
//  // test deletion of a column family in a row
//  Delete delete2 = new Delete(Bytes.toBytes("row901"));
//  delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
//  hTableSandbox.delete(delete2);
//  Get get2 = new Get(Bytes.toBytes("row901"));
//  get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//  prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  assertEquals("row901/cf1 not in production. delete should have no effect", prod, null);
//  assertEquals("row901/cf1 not in sandbox. delete should just mark the row/cf for deletion", sand, null);
//
//  // test deletion of a column in a column family in a row
//  Delete delete3 = new Delete(Bytes.toBytes("row902"));
//  delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
//  hTableSandbox.delete(delete3);
//  Get get3 = new Get(Bytes.toBytes("row902"));
//  get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//  prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//  assertEquals("row902/cf1/col1 not in production. delete should have no effect", prod, null);
//  assertEquals("row902/cf1/col1 not in sandbox. delete should just mark the row/cf/col for deletion", sand, null);
//
//  // test deletion marker in meta _shadow column family
//  Get get = new Get(Bytes.toBytes("row1"));
//  get.addFamily(DEFAULT_META_CF);
//  // TODO query the _shadow table for checking deletionMark
//  //resultSandbox = hTableSandbox.get(get);
//  //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//  //assertEquals("value should be null for production", sand, null);
//  //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//  //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//  }
//
//  @Test
//  // data not in production but in sandbox
//  public void testSandboxDeleteNotInProductionInSandbox() throws IOException {
//    // test deletion of entire row
//    Delete delete1 = new Delete(Bytes.toBytes("row25")); // delete entire row
//    hTableSandbox.delete(delete1);
//    Get get1 = new Get(Bytes.toBytes("row25"));
//    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("row not in production. delete should have no effect", prod, null);
//    assertEquals("row in sandbox. delete should mark the row for deletion and not fetch it", sand, null);
//
//    // test deletion of a column family in a row
//    Delete delete2 = new Delete(Bytes.toBytes("row26"));
//    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
//    hTableSandbox.delete(delete2);
//    Get get2 = new Get(Bytes.toBytes("row26"));
//    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("row26/cf1 not in production. delete should have no effect", prod, null);
//    assertEquals("row26/cf1 is in sandbox. delete should mark the row/cf for deletion and not fetch it", sand, null);
//
//    // test deletion of a column in a column family in a row
//    Delete delete3 = new Delete(Bytes.toBytes("row27"));
//    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
//    hTableSandbox.delete(delete3);
//    Get get3 = new Get(Bytes.toBytes("row27"));
//    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("row27/cf1/col1 not in production. delete should have no effect", prod, null);
//    assertEquals("row27/cf1/col1 is in sandbox. delete should mark the row/cf/col for deletion and not fetch it", sand, null);
//
//    // test deletion marker in meta _shadow column family
//    Get get = new Get(Bytes.toBytes("row1"));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//  }
//
//  @Test
//  // data in production but not in sandbox
//  public void testSandboxDeleteInProductionNotInSandbox() throws IOException {
//    // test deletion of entire row
//    Delete delete1 = new Delete(Bytes.toBytes("row35")); // delete entire row
//    hTableSandbox.delete(delete1);
//    Get get1 = new Get(Bytes.toBytes("row35"));
//    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("row should not be deleted in production", prod, "35");
//    assertEquals("row not in sandbox and should not be fetched from production", sand, null);
//
//    // test deletion of a column family in a row
//    Delete delete2 = new Delete(Bytes.toBytes("row36"));
//    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
//    hTableSandbox.delete(delete2);
//    Get get2 = new Get(Bytes.toBytes("row36"));
//    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("colfam should not be deleted in production", prod, "36");
//    assertEquals("row36/cf1 not in sandbox and should not be fetched from production", sand, null);
//
//    // test deletion of a column in a column family in a row
//    Delete delete3 = new Delete(Bytes.toBytes("row37"));
//    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
//    hTableSandbox.delete(delete3);
//    Get get3 = new Get(Bytes.toBytes("row37"));
//    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("column should not be deleted in production", prod, "37");
//    assertEquals("row37/cf1/col1 not in sandbox and should not be fetched from production", sand, null);
//
//    // test deletion marker in meta _shadow column family
//    Get get = new Get(Bytes.toBytes("row1"));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//  }
//
//  @Test
//  // data in production and in sandbox
//  public void testSandboxDeleteInProductionInSandbox() throws IOException {
//    // test deletion of entire row
//    Delete delete1 = new Delete(Bytes.toBytes("row1")); // delete entire row
//    hTableSandbox.delete(delete1);
//    Get get1 = new Get(Bytes.toBytes("row1"));
//    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("row should not be deleted in production", prod, "1");
//    assertEquals("row should be deleted in sandbox and not fetched from production", sand, null);
//
//    // test deletion of a column family in a row
//    Delete delete2 = new Delete(Bytes.toBytes("row2"));
//    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
//    hTableSandbox.delete(delete2);
//    Get get2 = new Get(Bytes.toBytes("row2"));
//    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("colfam should not be deleted in production", prod, "2");
//    assertEquals("colfam should be deleted in sandbox and not fetched from production", sand, null);
//
//    // test deletion of a column in a column family in a row
//    Delete delete3 = new Delete(Bytes.toBytes("row3"));
//    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
//    hTableSandbox.delete(delete3);
//    Get get3 = new Get(Bytes.toBytes("row3"));
//    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    assertEquals("column should not be deleted in production", prod, "3");
//    assertEquals("column should be deleted in sandbox and not fetched from production", sand, null);
//
//    // test deletion marker in meta _shadow column family
//    Get get = new Get(Bytes.toBytes("row1"));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//  }
//
//
//    @Test
//    public void testBatchDelete() throws IOException {
//        HTable hTable = new HTable(conf, sandboxTablePath);
//        HTable originalHTable = new HTable(conf, originalTablePath);
//
//        ResultScanner scanner;
//
//        // check initial state
//        scanner = originalHTable.getScanner(new Scan());
//        assertEquals("original and sandbox tables should be empty", 0L, countCells(scanner));
//        scanner = hTable.getScanner(new Scan());
//        assertEquals("original and sandbox tables should be empty", 0L, countCells(scanner));
//
//
//        // insert some data into sandbox
//        for (int i = 0; i < 25; i++) {
//            Put put = new Put(new String("rowId" + i).getBytes());
//            put.add(CF1, "col1".getBytes(), Integer.toString(i).getBytes());
//            put.add(CF1, "col2".getBytes(), Integer.toString(i * 5).getBytes());
//            hTable.put(put);
//        }
//        hTable.flushCommits();
//
//        // count cells
//        scanner = originalHTable.getScanner(new Scan());
//        assertEquals("original table should be empty", 0L, countCells(scanner));
//        scanner = hTable.getScanner(new Scan());
//        assertEquals("sandbox should contain all inserted cells", 50L, countCells(scanner));
//        scanner = hTable.getScanner(new Scan());
//        assertEquals("sandbox should contain all inserted rows", 25L, countRows(scanner));
//
//        // delete 2nd col from 5 rows
//        List<Delete> deletes = Lists.newArrayList();
//        for (int i = 0; i < 5; i++) {
//            Delete delete = new Delete(new String("rowId" + i).getBytes());
//            delete.deleteColumns(CF1, "col2".getBytes());
//            deletes.add(delete);
//        }
//        hTable.delete(deletes);
//
//        // recheck
//        scanner = originalHTable.getScanner(new Scan());
//        assertEquals("original table should be empty", 0L, countCells(scanner));
//        scanner = hTable.getScanner(new Scan());
//        assertEquals("sandbox should contain all inserted cells", 45L, countCells(scanner));
//        scanner = hTable.getScanner(new Scan());
//        assertEquals("sandbox should contain all inserted rows", 25L, countRows(scanner));
//    }
}
