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

public class SandboxTableMutateRowIntegrationTest extends BaseSandboxIntegrationTest {
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
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        //put.add(Bytes.toBytes("cf5"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        hTableProduction.put(put);
      }
      hTableProduction.flushCommits();
  }


  @Test
  // data not in production and not in sandbox
  public void testSandboxMutateRowNotInProductionNotInSandbox() throws IOException {
    // test deletion of entire row
    Put put1 =  new Put(Bytes.toBytes("row900"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("901"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("902"));
    Delete delete1 = new Delete(Bytes.toBytes("row900")); // delete entire row
    RowMutations mutations1 = new RowMutations(Bytes.toBytes("row900"));
    mutations1.add(put1);
    mutations1.add(delete1);
    hTableSandbox.mutateRow(mutations1);

    Get get1 = new Get(Bytes.toBytes("row900"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod1 = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row not in production. delete should have no effect", prod1, null);
    assertEquals("row not in sandbox. delete should just mark the row for deletion", sand1, null);

    // test deletion of a column family in a row and some puts in a different column family
    Put put2 =  new Put(Bytes.toBytes("row901"));
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
    String prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row901/cf1 not in production. delete should have no effect", prod2, null);
    assertEquals("row901/cf1 not in sandbox. delete should just mark the row/cf for deletion", sand2, null);
    prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
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
    String prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row902/cf1/col1 not in production. delete should have no effect", prod3, null);
    assertEquals("row902/cf1/col1 not in sandbox. delete should just mark the row/cf/col for deletion", sand3, null);
    prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    assertEquals("row902 not in production", prod3, null);
    assertEquals("row902 not in sandbox. PUT should put the value in sandbox", sand3, "903");

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
  public void testSandboxMutateRowNotInProductionInSandbox() throws IOException {
    // test deletion of entire row
    Put put1 =  new Put(Bytes.toBytes("row25"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("125"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("225"));
    Delete delete1 = new Delete(Bytes.toBytes("row25")); // delete entire row
    RowMutations mutations1 = new RowMutations(Bytes.toBytes("row25"));
    mutations1.add(put1);
    mutations1.add(delete1);
    hTableSandbox.mutateRow(mutations1);

    Get get1 = new Get(Bytes.toBytes("row25"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod1 = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row not in production. delete should have no effect", prod1, null);
    assertEquals("row in sandbox. delete should mark the row for deletion and not fetch it", sand1, null);

    // test deletion of a column family in a row
    Put put2 =  new Put(Bytes.toBytes("row26"));
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
    String prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row26/cf1 not in production. delete should have no effect", prod2, null);
    assertEquals("row26/cf1 is in sandbox. delete should mark the row/cf for deletion and not fetch it", sand2, null);
    prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
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
    String prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row27/cf1/col1 not in production. should return null", prod3, null);
    assertEquals("row27/cf1/col1 is in sandbox. delete should mark it for deletion and not fetch it", sand3, null);
    prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    assertEquals("row not in production. should return null", prod3, null);
    assertEquals("row is in sandbox. PUT should modify the value", sand3, "227");

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
  public void testSandboxMutateRowInProductionNotInSandbox() throws IOException {
    // test deletion of entire row
    Put put1 =  new Put(Bytes.toBytes("row35"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("135"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("235"));
    Delete delete1 = new Delete(Bytes.toBytes("row35")); // delete entire row
    RowMutations mutations1 = new RowMutations(Bytes.toBytes("row35"));
    mutations1.add(put1);
    mutations1.add(delete1);
    hTableSandbox.mutateRow(mutations1);

    Get get1 = new Get(Bytes.toBytes("row35"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod1 = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand1 = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row should not be deleted in production", prod1, "35");
    assertEquals("row not in sandbox and not fetched from production", sand1, null);

    // test deletion of a column family in a row and some puts in a different column family
    Put put2 =  new Put(Bytes.toBytes("row36"));
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
    String prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("colfam should not be deleted in production", prod2, "36");
    assertEquals("row36/cf1 not in sandbox and should not be fetched from production", sand2, null);
    prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
    sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
    assertEquals("PUT should not modify the value in production", prod2, "36");
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
    String prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("column should not be deleted in production", prod3, "37");
    assertEquals("row37/cf1/col1 not in sandbox and should not be fetched from production", sand3, null);
    prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    assertEquals("PUT should not modify the value in production", prod3, "37");
    assertEquals("row should be fetched from production. PUT should modify the value in sandbox", sand3, "237");

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
  public void testSandboxMutateRowInProductionInSandbox() throws IOException {
    // test deletion of entire row
    Put put1 =  new Put(Bytes.toBytes("row1"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("101"));
    put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("201"));
    Delete delete1 = new Delete(Bytes.toBytes("row1")); // delete entire row
    RowMutations mutations1 = new RowMutations(Bytes.toBytes("row1"));
    mutations1.add(put1);
    mutations1.add(delete1);
    hTableSandbox.mutateRow(mutations1);

    Get get1 = new Get(Bytes.toBytes("row1"));
    get1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("col1"));
    String prod = Bytes.toString(hTableProduction.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand = Bytes.toString(hTableSandbox.get(get1).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("row should not be deleted in production", prod, "1");
    assertEquals("row should be deleted in sandbox and not fetched from production", sand, null);

    // test deletion of a column family in a row and some puts in a different column family
    Put put2 =  new Put(Bytes.toBytes("row2"));
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
    String prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("colfam should not be deleted in production", prod2, "2");
    assertEquals("colfam should be deleted in sandbox and not fetched from production", sand2, null);
    prod2 = Bytes.toString(hTableProduction.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
    sand2 = Bytes.toString(hTableSandbox.get(get2).getValue(Bytes.toBytes("cf2"), Bytes.toBytes("col1")));
    assertEquals("PUT should not modify the value in production", prod2, "2");
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
    String prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    String sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
    assertEquals("column should not be deleted in production", prod3, "3");
    assertEquals("column should be deleted in sandbox and not fetched from production", sand3, null);
    prod3 = Bytes.toString(hTableProduction.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    sand3 = Bytes.toString(hTableSandbox.get(get3).getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col2")));
    assertEquals("PUT should not modify the value in production", prod3, "3");
    assertEquals("PUT should modify the value in sandbox", sand3, "201");

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
