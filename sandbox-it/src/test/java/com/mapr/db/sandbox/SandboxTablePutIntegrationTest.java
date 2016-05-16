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

public class SandboxTablePutIntegrationTest extends BaseSandboxIntegrationTest {
  protected static Configuration conf = HBaseConfiguration.create();
  protected static String productionTablePath;
  protected static String sandboxTablePath;

  protected static HTable hTableProduction;
  protected static HTable hTableSandbox;

  @BeforeClass
  public static void createProductionTable() throws IOException {
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
        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes(Integer.toString(i)));
        hTableProduction.put(put);
      }
      hTableProduction.flushCommits();
  }


  @Test
  public void testSandboxPut() throws IOException, SandboxException, Exception {
    String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
    String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
    sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
    hTableSandbox = new HTable(conf, sandboxTablePath);

    // not in production and not in sandbox
    Put put = new Put(Bytes.toBytes(Integer.toString(100)));
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(100)));
    hTableSandbox.put(put);

    Get get = new Get(Bytes.toBytes(Integer.toString(100)));
    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    Result resultProduction = hTableProduction.get(get);
    Result resultSandbox = hTableSandbox.get(get);
    String prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));

    assertEquals("value should be null for production", prod, null);
    assertEquals("value should exist for sandbox", sand, "100");

    get = new Get(Bytes.toBytes(Integer.toString(100)));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));

    // not in production, but in sandbox
    put = new Put(Bytes.toBytes(Integer.toString(100)));
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(200)));
    hTableSandbox.put(put);

    get = new Get(Bytes.toBytes(Integer.toString(100)));
    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    resultProduction = hTableProduction.get(get);
    resultSandbox = hTableSandbox.get(get);
    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));

    assertEquals("value should be null for production", prod, null);
    assertEquals("value should be overwritten for sandbox", sand, "200");

    get = new Get(Bytes.toBytes(Integer.toString(100)));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));

    // in production, but not in sandbox
    // add some additional data only in production to prepare for this test
    hTableProduction = new HTable(conf, productionTablePath);
    for (int i = 35; i < 40; i++) {
      put = new Put(Bytes.toBytes(Integer.toString(i)));
      put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
      hTableProduction.put(put);
    }
    hTableProduction.flushCommits();

    put = new Put(Bytes.toBytes(Integer.toString(35)));
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(200)));
    hTableSandbox.put(put);

    get = new Get(Bytes.toBytes(Integer.toString(35)));
    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    resultProduction = hTableProduction.get(get);
    resultSandbox = hTableSandbox.get(get);
    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));

    assertEquals("value should be as is for production", prod, "35");
    assertEquals("new value in sandbox should take preference", sand, "200");

    get = new Get(Bytes.toBytes(Integer.toString(35)));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));

    // in production and in sandbox. sandbox should take preference
    put = new Put(Bytes.toBytes(Integer.toString(0)));
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(200)));
    hTableSandbox.put(put);

    get = new Get(Bytes.toBytes(Integer.toString(0)));
    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    resultProduction = hTableProduction.get(get);
    resultSandbox = hTableSandbox.get(get);
    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));

    assertEquals("value should be as is for production", prod, "0");
    assertEquals("new value in sandbox should take preference", sand, "200");

    get = new Get(Bytes.toBytes(Integer.toString(0)));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableProduction.exists(get));
  }

  @AfterClass
  public static void cleanupTable() throws IOException {
    String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
    sandboxAdmin.deleteSandbox(sandboxTablePath);
    hba.deleteTable(productionTablePath);
  }
}
