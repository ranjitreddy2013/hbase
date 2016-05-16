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

public class SandboxTableIncrementIntegrationTest extends BaseSandboxIntegrationTest {
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
  public void testSandboxIncrementNotInProductionNotInSandbox() throws IOException {
    // test incrementing the value of an already existing column
    // TODO

    // test incrementing the value of a new column
    Increment increment2 = new Increment(Bytes.toBytes("row35"));
    increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    increment2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col102"), 1);
    hTableSandbox.increment(increment2);
    Get get2 = new Get(Bytes.toBytes("row35"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col101"));
    long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    assertEquals("row is not in production. should return null.", prod, null);
    assertEquals("row is not in sandbox. value should be initialized to 1", sand, 1L);

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
    long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    assertEquals("row is not in production. should return null.", prod, null);
    assertEquals("row is in sandbox. new column. value should be initialized to 1", sand, 1L);

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
    long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    assertEquals("row is in production, but a new column. should return null.", prod, null);
    assertEquals("row not in sandbox. should be fetched from production and value initialized to 1", sand, 1L);

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
    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
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
    long prod = Bytes.toLong(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    long sand = Bytes.toLong(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
    assertEquals("value not in production. should return null.", prod, null);
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
    String prod = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col101")));
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
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get)); */
  }

}
