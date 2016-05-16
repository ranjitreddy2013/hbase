package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static org.junit.Assert.assertEquals;

public class SandboxTableIncrementColumnValueIntegrationTest extends BaseSandboxIntegrationTest {
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

/*
  @Test
  // data not in production and not in sandbox
  public void testSandboxIncrementColumnValueNotInProductionNotInSandbox() throws IOException {
    // test appending to the value of a new row
    Append append1 = new Append(Bytes.toBytes("row900"));
    append1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("v900"));
    hTableSandbox.append(append1);
    Get get1 = new Get(Bytes.toBytes("row900"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("value should be appended as is in sandbox", sand, "v900");

    // test appending to the value of a new column
    Append append2 = new Append(Bytes.toBytes("row900"));
    append2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col900"), Bytes.toBytes("v900"));
    hTableSandbox.append(append2);
    Get get2 = new Get(Bytes.toBytes("row900"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col900"));
    prod = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col900")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col900")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("new column. value should be appended as is in sandbox", sand, "v900");

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

  @Test
  // data not in production but in sandbox
  public void testSandboxIncrementColumnValueNotInProductionInSandbox() throws IOException {
    // test appending to the value of an already existing column
    Append append1 = new Append(Bytes.toBytes("row25"));
    append1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("v125"));
    hTableSandbox.append(append1);
    Get get1 = new Get(Bytes.toBytes("row25"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("value should be appended to the existing value in sandbox", sand, "25v125");

    // test appending to the value of a new column
    Append append2 = new Append(Bytes.toBytes("row25"));
    append2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col125"), Bytes.toBytes("v125"));
    hTableSandbox.append(append2);
    Get get2 = new Get(Bytes.toBytes("row25"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col125"));
    prod = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col125")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col125")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("new column. value should be appended as is in sandbox", sand, "v125");

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

  @Test
  // data in production but not in sandbox
  public void testSandboxIncrementColumnValueInProductionNotInSandbox() throws IOException {
    // test appending to the value of an already existing column
    Append append1 = new Append(Bytes.toBytes("row35"));
    append1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("v135"));
    hTableSandbox.append(append1);
    Get get1 = new Get(Bytes.toBytes("row35"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("value should not be appended in production", prod, "35");
    assertEquals("value should be fetched from production and appended in sandbox", sand, "35v135");

    // test appending to the value of a new column
    Append append2 = new Append(Bytes.toBytes("row35"));
    append2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col135"), Bytes.toBytes("v135"));
    hTableSandbox.append(append2);
    Get get2 = new Get(Bytes.toBytes("row35"));
    get2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col135"));
    prod = Bytes.toString(hTableOriginal.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col135")));
    sand = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col135")));
    assertEquals("value not in production. should return null.", prod, null);
    assertEquals("new column. value should be appended as is in sandbox", sand, "v135");

    // test deletion marker in meta _shadow column family
    Get get = new Get(Bytes.toBytes("row1"));
    get.addFamily(DEFAULT_META_CF);
    // TODO query the _shadow table for checking deletionMark
    //resultSandbox = hTableSandbox.get(get);
    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
    //assertEquals("value should be null for production", sand, null);
    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
  } */

  @Test
  // data in production and in sandbox
  public void testSandboxIncrementColumnValueInProductionInSandbox() throws IOException {
    // test incrementing the value of an already existing column
    long cnt1 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col1"), 1);
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

/*
    Get get1 = new Get(Bytes.toBytes("row1"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableOriginal.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("value in production should not be affected by any of the sandbox increments", prod, 1L);
    assertEquals("value in sandbox after all increment operations", sand, 3L);
*/

    // test incrementing the value of a new column
    long cnt = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should initialize to 1 in sandbox", cnt, 1L);
    cnt1 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should be incremented by 1 in sandbox", cnt1, 2L);
    cnt2 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 1);
    assertEquals("value should be incremented by 1 more in sandbox", cnt2, 3L);
    cnt3 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), -1);
    assertEquals("value should be decremented by 1 in sandbox", cnt3, 2L);
    cnt4 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 100);
    assertEquals("value should be incremented by 100 in sandbox", cnt4, 102L);
    cnt5 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), -99);
    assertEquals("value should be decremented by 99 in sandbox", cnt5, 3L);
    cnt6 = hTableSandbox.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("col101"), 0);
    assertEquals("value should remain as is when decremented by 0 in sandbox", cnt6, 3L);
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

}
