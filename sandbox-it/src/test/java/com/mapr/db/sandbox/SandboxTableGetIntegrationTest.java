package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class SandboxTableGetIntegrationTest extends BaseSandboxIntegrationTest {

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

        // insert some data
        hTableProduction = new HTable(conf, productionTablePath);

        Put put1 = new Put(Bytes.toBytes("r1"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("val1"));
        put1.add(Bytes.toBytes("cf1"), Bytes.toBytes("col2"), Bytes.toBytes("val2"));
        put1.add(Bytes.toBytes("cf2"), Bytes.toBytes("col1"), Bytes.toBytes("val3"));
        hTableProduction.put(put1);

        Put put2 = new Put(Bytes.toBytes("r2"));
        put2.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("val4"));
        put2.add(Bytes.toBytes("cf3"), Bytes.toBytes("col1"), Bytes.toBytes("val5"));
        hTableProduction.put(put2);
        hTableProduction.flushCommits();
    }


    @Test
    public void testSandboxGet() throws IOException, SandboxException, Exception {

      String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
      String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
      sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
      hTableSandbox = new HTable(conf, sandboxTablePath);

      // same data in production and sandbox
      Get get = new Get(Bytes.toBytes("r1"));
      get.addFamily(Bytes.toBytes("cf1"));
      Result resultProduction = hTableProduction.get(get);
      Result resultSandbox = hTableSandbox.get(get);
      //TODO - check why the below method fails
      //Result.compareResults(resultProduction, resultSandbox); // this test should not throw any exception

      String prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      String sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      assertEquals("value should be same for sandbox and production", prod, sand);

      // not in production and not in sandbox
      prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col100")));
      sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col100")));
      assertEquals("value should be empty for production", prod, null);
      assertEquals("value should be empty for sandbox", sand, null);

      // not in production, but in sandbox
      Put put3 = new Put(Bytes.toBytes("r2"));
      put3.add(Bytes.toBytes("cf3"), Bytes.toBytes("col2"), Bytes.toBytes("val6"));
      hTableSandbox.put(put3);
      hTableSandbox.flushCommits();

      get = new Get(Bytes.toBytes("r2"));
      resultProduction = hTableProduction.get(get);
      resultSandbox = hTableSandbox.get(get);
      prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("col2")));
      sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("col2")));
      assertEquals("value should be null for production", prod, null);
      assertEquals("value should exist for sandbox", sand, "val6");

      // in production, but not in sandbox
      Put put4 = new Put(Bytes.toBytes("r2"));
      put4.add(Bytes.toBytes("cf3"), Bytes.toBytes("col3"), Bytes.toBytes("val7"));
      hTableProduction.put(put4);
      hTableProduction.flushCommits();

      get = new Get(Bytes.toBytes("r2"));
      resultProduction = hTableProduction.get(get);
      resultSandbox = hTableSandbox.get(get);
      prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("col3")));
      sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf3"), Bytes.toBytes("col3")));
      assertEquals("value should exist for production", prod, "val7");
      assertEquals("value should exist for sandbox", sand, "val7");

      // in production and in sandbox. sandbox should take preference
      Put put5 = new Put(Bytes.toBytes("r1"));
      put5.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("val100"));
      hTableSandbox.put(put5);
      hTableSandbox.flushCommits();

      get = new Get(Bytes.toBytes("r1"));
      resultProduction = hTableProduction.get(get);
      resultSandbox = hTableSandbox.get(get);
      prod = Bytes.toString(resultProduction.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      sand = Bytes.toString(resultSandbox.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("col1")));
      assertEquals("value should exist for production", prod, "val1");
      assertEquals("value should exist for sandbox", sand, "val100");

    }


    @AfterClass
    public static void cleanupTable() throws IOException {
      String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
      sandboxAdmin.deleteSandbox(sandboxTablePath);
      hba.deleteTable(productionTablePath);
    }
}
