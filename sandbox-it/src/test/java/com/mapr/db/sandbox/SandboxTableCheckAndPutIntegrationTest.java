package com.mapr.db.sandbox;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import java.io.IOException;
//
//import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;

public class SandboxTableCheckAndPutIntegrationTest extends BaseSandboxIntegrationTest {
//  protected static Configuration conf = HBaseConfiguration.create();
//  protected static String productionTablePath;
//  protected static String sandboxTablePath;
//
//  protected static HTable hTableProduction;
//  protected static HTable hTableSandbox;
//
//  @BeforeClass
//  public static void createProductionTable() throws IOException {
//      assureWorkingDirExists();
//      hba = new HBaseAdmin(conf);
//      String productionTableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
//      productionTablePath = String.format("%s%s", TABLE_PREFIX, productionTableName);
//      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(productionTablePath));
//      tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
//      tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
//      tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
//      hba.createTable(tableDescriptor);
//
//      // insert some data to production
//      hTableProduction = new HTable(conf, productionTablePath);
//      for (int i = 0; i < 25; i++) {
//        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
//        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
//        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
//        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
//        put.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes(Integer.toString(i)));
//        hTableProduction.put(put);
//      }
//      hTableProduction.flushCommits();
//  }
//
//
//  @Test
//  public void testSandboxCheckAndPut() throws IOException, SandboxException, Exception {
//    String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
//    String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
//    sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
//    hTableSandbox = new HTable(conf, sandboxTablePath);
//
//    // not in production and not in sandbox
//    Put put1 = new Put(Bytes.toBytes("row100"));
//    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("100"));
//    boolean res1 = hTableSandbox.checkAndPut(Bytes.toBytes("row100"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), null, put1); // non-existent-row
//    boolean res2 = hTableSandbox.checkAndPut(Bytes.toBytes("row1"),
//      Bytes.toBytes("cf100"), Bytes.toBytes("col1"), null, put1); // row-exists + non-existent-cf
//    boolean res3 = hTableSandbox.checkAndPut(Bytes.toBytes("row1"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col100"), null, put1); // row-exists + cf-exists + non-existent-qual
//    boolean res4 = hTableSandbox.checkAndPut(Bytes.toBytes("row1"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("1"), put1); // row-exists + cf-exists + qual-exists
//
//
//    Get get = new Get(Bytes.toBytes("row100"));
//    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    Result resultProduction = hTableProduction.get(get);
//    Result resultSandbox = hTableSandbox.get(get);
//    String prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    String sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//
//    assertFalse("checkAndPut should be False for checking a non-existent-row in sandbox", res1);
//    //TODO check the below two. they should throw exception
//    //assertTrue("checkAndPut should be True for existent-row + non-existent-cf in sandbox", res2);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + non-existent-qual in sandbox", res3);
//    assertEquals("value should be null for production", prod, null);
//    //assertEquals("value should be 100 for sandbox", sand, 100L);
//
//    get = new Get(Bytes.toBytes("row100"));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//
//    // not in production, but in sandbox
//    put1 = new Put(Bytes.toBytes("row100"));
//    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(100)));
//    hTableSandbox.put(put1); // row100 is only in sandbox
//
//    Put put2 = new Put(Bytes.toBytes("row100"));
//    put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("200"));
//    res1 = hTableSandbox.checkAndPut(Bytes.toBytes("row999"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), null, put2); // non-existent-row
//    res2 = hTableSandbox.checkAndPut(Bytes.toBytes("row100"),
//      Bytes.toBytes("cf100"), Bytes.toBytes("col1"), null, put2); // row-exists + non-existent-cf
//    res3 = hTableSandbox.checkAndPut(Bytes.toBytes("row100"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col100"), null, put2); // row-exists + cf-exists + non-existent-qual
//    res4 = hTableSandbox.checkAndPut(Bytes.toBytes("row100"),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("100"), put2); // row-exists + cf-exists + qual-exists
//
//    get = new Get(Bytes.toBytes("row100"));
//    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    resultProduction = hTableProduction.get(get);
//    resultSandbox = hTableSandbox.get(get);
//    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//
//    assertFalse("checkAndPut should be False for checking a non-existent-row in sandbox", res1);
//    //TODO check the below two. they should throw exception
//    //assertTrue("checkAndPut should be True for existent-row + non-existent-cf in sandbox", res2);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + non-existent-qual in sandbox", res3);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + existent-qual in sandbox", res4);
//    assertEquals("value should be null for production", prod, null);
//    //assertEquals("value should be overwritten for sandbox", sand, "200");
//
//    get = new Get(Bytes.toBytes(Integer.toString(100)));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//
//
//    // in production, but not in sandbox
//    // add some additional data only in production to prepare for this test
//    hTableProduction = new HTable(conf, productionTablePath);
//    for (int i = 35; i < 40; i++) {
//      Put put = new Put(Bytes.toBytes(Integer.toString(i)));
//      put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
//      hTableProduction.put(put);
//    }
//    hTableProduction.flushCommits();
//
//    Put put3 = new Put(Bytes.toBytes(Integer.toString(35)));
//    put3.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(200)));
//    res1 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(35)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(35)), put3); // non-existent-row
//    res2 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(35)),
//      Bytes.toBytes("cf100"), Bytes.toBytes("col1"), null, put3); // row-exists + non-existent-cf
//    res3 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(35)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col100"), null, put3); // row-exists + cf-exists + non-existent-qual
//    res4 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(35)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("100"), put3); // row-exists + cf-exists + qual-exists
//
//    get = new Get(Bytes.toBytes(Integer.toString(35)));
//    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    resultProduction = hTableProduction.get(get);
//    resultSandbox = hTableSandbox.get(get);
//    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//
//    assertFalse("checkAndPut should be False for checking a non-existent-row in sandbox", res1);
//    //TODO check the below two. they should throw exception
//    //assertTrue("checkAndPut should be True for existent-row + non-existent-cf in sandbox", res2);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + non-existent-qual in sandbox", res3);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + existent-qual in sandbox", res4);
//    assertEquals("value should be as is for production", prod, "35");
//    //assertEquals("new value in sandbox should take preference", sand, "200");
//
//    get = new Get(Bytes.toBytes(Integer.toString(100)));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get));
//
//
//    // in production and in sandbox. sandbox should take preference
//    Put put4 = new Put(Bytes.toBytes(Integer.toString(0)));
//    put4.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(200)));
//    res1 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(999)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), null, put4); // non-existent-row
//    res2 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(0)),
//      Bytes.toBytes("cf100"), Bytes.toBytes("col1"), null, put4); // row-exists + non-existent-cf
//    res3 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(0)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col100"), null, put4); // row-exists + cf-exists + non-existent-qual
//    res4 = hTableSandbox.checkAndPut(Bytes.toBytes(Integer.toString(0)),
//      Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("100"), put4); // row-exists + cf-exists + qual-exists
//
//    get = new Get(Bytes.toBytes(Integer.toString(0)));
//    get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
//    resultProduction = hTableProduction.get(get);
//    resultSandbox = hTableSandbox.get(get);
//    prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//    sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
//
//    assertFalse("checkAndPut should be False for checking a non-existent-row in sandbox", res1);
//    //TODO check the below two. they should throw exception
//    //assertTrue("checkAndPut should be True for existent-row + non-existent-cf in sandbox", res2);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + non-existent-qual in sandbox", res3);
//    //assertTrue("checkAndPut should be True for existent-row + existent-cf + existent-qual in sandbox", res4);
//    assertEquals("value should be as is for production", prod, "0");
//    assertEquals("new value in sandbox should take preference", sand, "200");
//
//    get = new Get(Bytes.toBytes(Integer.toString(0)));
//    get.addFamily(DEFAULT_META_CF);
//    // TODO query the _shadow table for checking deletionMark
//    //resultSandbox = hTableSandbox.get(get);
//    //sand = Bytes.toString(resultSandbox.getValue(DEFAULT_META_CF, Bytes.toBytes("cf1:col1")));
//    //assertEquals("value should be null for production", sand, null);
//    //assertTrue("shadow cf should be present", hTableSandbox.getTableDescriptor().hasFamily(DEFAULT_META_CF));
//    //assertFalse("deletionMark should be removed if present", hTableOriginal.exists(get)); */
//  }
//
//  @AfterClass
//  public static void cleanupTable() throws IOException {
//    String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
//    sandboxAdmin.deleteSandbox(sandboxTablePath);
//    hba.deleteTable(productionTablePath);
//  }
}
