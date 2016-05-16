package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import static com.mapr.db.sandbox.SandboxTestUtils.countCells;
import static com.mapr.db.sandbox.SandboxTestUtils.countRows;
import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF_NAME;
import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class SandboxTableDeleteIntegrationTest extends BaseSandboxIntegrationTest {
  protected static Configuration conf = HBaseConfiguration.create();
  protected static String productionTablePath;
  protected static String sandboxTablePath;

  protected static HTable hTableProduction;
  protected static HTable hTableSandbox;
  protected static SandboxAdmin sandboxAdmin;

  @BeforeClass
  public static void createProductionAndSandboxTables() throws IOException, SandboxException {
      // create production table
      assureWorkingDirExists();
      hba = new HBaseAdmin(conf);
      String productionTableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
      productionTablePath = String.format("%s%s", TABLE_PREFIX, productionTableName);
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(productionTablePath));
      tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
      tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
      tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
      hba.createTable(tableDescriptor);

      // insert some data to production
      hTableProduction = new HTable(conf, productionTablePath);
      for (int i = 0; i < 25; i++) {
        Put put = new Put(Bytes.toBytes("row" + Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes(Integer.toString(i)));
        hTableProduction.put(put);
      }
      hTableProduction.flushCommits();

      // create sandbox table
      sandboxAdmin = new SandboxAdmin(new Configuration());
      String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
      String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
      sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
      hTableSandbox = new HTable(conf, sandboxTablePath);

      // insert some data only to sandbox
      for (int i = 25; i < 35; i++) {
        Put put = new Put(Bytes.toBytes("row" + Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes(Integer.toString(i)));
        //put.add(Bytes.toBytes("cf4"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        hTableSandbox.put(put);
      }
      hTableSandbox.flushCommits();

      // insert some data only to production
      hTableProduction = new HTable(conf, productionTablePath);
      for (int i = 35; i < 40; i++) {
        Put put = new Put(Bytes.toBytes("row" + Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        //put.add(Bytes.toBytes("cf5"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        hTableProduction.put(put);
      }
      hTableProduction.flushCommits();
  }


  @Test
  // data not in production and not in sandbox
  public void testSandboxDeleteNotInProductionNotInSandbox() throws IOException {
  // test deletion of entire row
  Delete delete1 = new Delete(Bytes.toBytes("row900")); // delete entire row
  hTableSandbox.delete(delete1);
  Get get1 = new Get(Bytes.toBytes("row900"));
  get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
  String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  assertEquals("row not in production. delete should have no effect", prod, null);
  assertEquals("row not in sandbox. delete should just mark the row for deletion", sand, null);

  // test deletion of a column family in a row
  Delete delete2 = new Delete(Bytes.toBytes("row901"));
  delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
  hTableSandbox.delete(delete2);
  Get get2 = new Get(Bytes.toBytes("row901"));
  get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
  prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  assertEquals("row901/cf1 not in production. delete should have no effect", prod, null);
  assertEquals("row901/cf1 not in sandbox. delete should just mark the row/cf for deletion", sand, null);

  // test deletion of a column in a column family in a row
  Delete delete3 = new Delete(Bytes.toBytes("row902"));
  delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
  hTableSandbox.delete(delete3);
  Get get3 = new Get(Bytes.toBytes("row902"));
  get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
  prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
  assertEquals("row902/cf1/col1 not in production. delete should have no effect", prod, null);
  assertEquals("row902/cf1/col1 not in sandbox. delete should just mark the row/cf/col for deletion", sand, null);

  // test deletion marker in meta _shadow column family
  Get get = new Get(Bytes.toBytes("row1"));
  get.addFamily(DEFAULT_META_CF);
  // TODO query the _shadow table for checking deletionMark
  //resultSandbox = hTableSandbox.get(get);
  //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
  //assertEquals("value should be null for production", sand, null);
  //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
  //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));
  }

  @Test
  // data not in production but in sandbox
  public void testSandboxDeleteNotInProductionInSandbox() throws IOException {
    // test deletion of entire row
    Delete delete1 = new Delete(Bytes.toBytes("row25")); // delete entire row
    hTableSandbox.delete(delete1);
    Get get1 = new Get(Bytes.toBytes("row25"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row not in production. delete should have no effect", prod, null);
    assertEquals("row in sandbox. delete should mark the row for deletion and not fetch it", sand, null);

    // test deletion of a column family in a row
    Delete delete2 = new Delete(Bytes.toBytes("row26"));
    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
    hTableSandbox.delete(delete2);
    Get get2 = new Get(Bytes.toBytes("row26"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row26/cf1 not in production. delete should have no effect", prod, null);
    assertEquals("row26/cf1 is in sandbox. delete should mark the row/cf for deletion and not fetch it", sand, null);

    // test deletion of a column in a column family in a row
    Delete delete3 = new Delete(Bytes.toBytes("row27"));
    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
    hTableSandbox.delete(delete3);
    Get get3 = new Get(Bytes.toBytes("row27"));
    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row27/cf1/col1 not in production. delete should have no effect", prod, null);
    assertEquals("row27/cf1/col1 is in sandbox. delete should mark the row/cf/col for deletion and not fetch it", sand, null);

    // test deletion marker in meta _shadow column family
    Get get = new Get(Bytes.toBytes("row1"));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));
  }

  @Test
  // data in production but not in sandbox
  public void testSandboxDeleteInProductionNotInSandbox() throws IOException {
    // test deletion of entire row
    Delete delete1 = new Delete(Bytes.toBytes("row35")); // delete entire row
    hTableSandbox.delete(delete1);
    Get get1 = new Get(Bytes.toBytes("row35"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row should not be deleted in production", prod, "35");
    assertEquals("row not in sandbox and should not be fetched from production", sand, null);

    // test deletion of a column family in a row
    Delete delete2 = new Delete(Bytes.toBytes("row36"));
    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
    hTableSandbox.delete(delete2);
    Get get2 = new Get(Bytes.toBytes("row36"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("colfam should not be deleted in production", prod, "36");
    assertEquals("row36/cf1 not in sandbox and should not be fetched from production", sand, null);

    // test deletion of a column in a column family in a row
    Delete delete3 = new Delete(Bytes.toBytes("row37"));
    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
    hTableSandbox.delete(delete3);
    Get get3 = new Get(Bytes.toBytes("row37"));
    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("column should not be deleted in production", prod, "37");
    assertEquals("row37/cf1/col1 not in sandbox and should not be fetched from production", sand, null);

    // test deletion marker in meta _shadow column family
    Get get = new Get(Bytes.toBytes("row1"));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));
  }

  @Test
  // data in production and in sandbox
  public void testSandboxDeleteInProductionInSandbox() throws IOException {
    // test deletion of entire row
    Delete delete1 = new Delete(Bytes.toBytes("row1")); // delete entire row
    hTableSandbox.delete(delete1);
    Get get1 = new Get(Bytes.toBytes("row1"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row should not be deleted in production", prod, "1");
    assertEquals("row should be deleted in sandbox and not fetched from production", sand, null);

    // test deletion of a column family in a row
    Delete delete2 = new Delete(Bytes.toBytes("row2"));
    delete2.deleteFamily(Bytes.toBytes("cf1")); // delete a cf
    hTableSandbox.delete(delete2);
    Get get2 = new Get(Bytes.toBytes("row2"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("colfam should not be deleted in production", prod, "2");
    assertEquals("colfam should be deleted in sandbox and not fetched from production", sand, null);

    // test deletion of a column in a column family in a row
    Delete delete3 = new Delete(Bytes.toBytes("row3"));
    delete3.deleteColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1")); // delete a column
    hTableSandbox.delete(delete3);
    Get get3 = new Get(Bytes.toBytes("row3"));
    get3.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    prod = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("column should not be deleted in production", prod, "3");
    assertEquals("column should be deleted in sandbox and not fetched from production", sand, null);

    // test deletion marker in meta _shadow column family
    Get get = new Get(Bytes.toBytes("row1"));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));
  }

}
