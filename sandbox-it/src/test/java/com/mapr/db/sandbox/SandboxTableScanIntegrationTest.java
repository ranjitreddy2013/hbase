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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class SandboxTableScanIntegrationTest extends BaseSandboxIntegrationTest {

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
    public void testSandboxScan() throws IOException, SandboxException, Exception {

      String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
      String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
      sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
      hTableSandbox = new HTable(conf, sandboxTablePath);

      // add some additional data to sandbox
      for (int i = 25; i < 35; i++) {
        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf4"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(i)));
        hTableSandbox.put(put);
      }
      hTableSandbox.flushCommits();

      // scan all rows and count
      Scan s = new Scan();
      ResultScanner resultProductionScanner = hTableProduction.getScanner(s);
      ResultScanner resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should have the 25 scanner rows", countRows(resultProductionScanner), 25L);
      assertEquals("sandbox should have the 35 scanner rows", countRows(resultSandboxScanner), 35L);

      // scan a specific range of rows
      s = new Scan(Bytes.toBytes(Integer.toString(11)), Bytes.toBytes(Integer.toString(20)));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      //TODO - check why the below method fails
      //Result.compareResults(resultProduction, resultSandbox); // this test should not throw any exception
      //
      assertEquals("production should have the 10 scanner rows", countRows(resultProductionScanner), 10L);
      assertEquals("sandbox should have the 10 scanner rows", countRows(resultSandboxScanner), 10L);


      // scan specific column families
      s = new Scan(Bytes.toBytes(Integer.toString(11)), Bytes.toBytes(Integer.toString(20)));
      s.addFamily(Bytes.toBytes("cf1"));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should have 20 cells", countCells(resultProductionScanner), 20L);
      assertEquals("sandbox should have the 20 cells", countCells(resultSandboxScanner), 20L);

      // scan specific columns
      s = new Scan();
      s.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("col3"));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should have 25 cells", countCells(resultProductionScanner), 25L);
      assertEquals("sandbox should have the 35 cells", countCells(resultSandboxScanner), 35L);

      // same data in production and sandbox
      // String prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      // String sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      // assertEquals("value should be same for sandbox and production", prod, sand);

      // not in production and not in sandbox
      s = new Scan();
      s.addColumn(Bytes.toBytes("cf100"), Bytes.toBytes("col80"));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should return null", resultProductionScanner, null);
      assertEquals("sandbox should return null", resultSandboxScanner, null);

      // not in production, but in sandbox
      s = new Scan();
      s.addColumn(Bytes.toBytes("cf4"), Bytes.toBytes("col2"));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should return null", resultProductionScanner, null);
      assertEquals("sandbox should have the 10 rows", countRows(resultSandboxScanner), 10L);

      // in production, but not in sandbox
      hTableProduction = new HTable(conf, productionTablePath);
      for (int i = 35; i < 40; i++) {
        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf5"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(i)));
        hTableProduction.put(put);
      }
      hTableProduction.flushCommits();

      s = new Scan();
      s.addColumn(Bytes.toBytes("cf5"), Bytes.toBytes("col1"));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should return 5 rows", countRows(resultProductionScanner), 5L);
      assertEquals("sandbox should also return 5 rows", countRows(resultSandboxScanner), 10L);

      s = new Scan(Bytes.toBytes(Integer.toString(35)), Bytes.toBytes(Integer.toString(40)));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      assertEquals("production should return 5 rows", countRows(resultProductionScanner), 5L);
      assertEquals("sandbox should also return 5 rows", countRows(resultSandboxScanner), 10L);

      // in production and in sandbox. sandbox should take preference
      for (int i = 0; i < 25; i+=2) {
        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(1000 * i)));
        put.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes(Integer.toString(2000 * i)));
        put.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes(Integer.toString(3000 * i)));
        hTableSandbox.put(put);
      }
      hTableSandbox.flushCommits();

      s = new Scan(Bytes.toBytes(Integer.toString(4)), Bytes.toBytes(Integer.toString(7)));
      resultProductionScanner = hTableProduction.getScanner(s);
      resultSandboxScanner = hTableSandbox.getScanner(s);
      List<String> productionKeyValues = new ArrayList<String>();
      List<String> sandboxKeyValues = new ArrayList<String>();
      for (Result result = resultProductionScanner.next(); (result != null); result = resultProductionScanner.next()) {
        for(KeyValue keyValue : result.list()) {
          productionKeyValues.add(keyValue.getKeyString() + " " + Bytes.toString(keyValue.getValue()));
          //System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
        }
      }

      for (Result result = resultSandboxScanner.next(); (result != null); result = resultProductionScanner.next()) {
        for(KeyValue keyValue : result.list()) {
          productionKeyValues.add(keyValue.getKeyString() + " " + Bytes.toString(keyValue.getValue()));
          //System.out.println("Qualifier : " + keyValue.getKeyString() + " : Value : " + Bytes.toString(keyValue.getValue()));
        }
      }

      assertEquals("production scanner should return original data", productionKeyValues, 5L);
      assertEquals("sandbox should also return new modified data", sandboxKeyValues, 10L);
    }

    @AfterClass
    public static void cleanupTable() throws IOException {
      String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
      sandboxAdmin.deleteSandbox(sandboxTablePath);
      hba.deleteTable(productionTablePath);
    }
}
