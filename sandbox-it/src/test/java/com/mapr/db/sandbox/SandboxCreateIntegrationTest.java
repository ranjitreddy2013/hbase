package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SandboxCreateIntegrationTest extends BaseSandboxIntegrationTest {

	protected static Configuration conf = HBaseConfiguration.create();
	protected static HBaseAdmin admin;
	
	protected static String productionTablePath;

    @BeforeClass
    public static void createproductionTable() {
        //sandboxAdmin = new SandboxAdmin(new Configuration());
        //String productiontableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
        //productionTablePath = String.format("%s%s", TABLE_PREFIX, tableName);
        //sandboxAdmin.createTable(productionTablePath, "cf");
        try {
          admin = new HBaseAdmin(conf);
          String productionTableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
          productionTablePath = String.format("%s%s", TABLE_PREFIX, productionTableName);
          HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(productionTablePath));
          tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
          tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
          tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
          admin.createTable(tableDescriptor);
        } catch (Exception e) {
            return;
        }
    }

    @AfterClass
    public static void cleanupproductionTable() {
        try {
          admin.deleteTable(productionTablePath);
        } catch (Exception e) {
            return;
        }

    }

    @Test
    public void testSandboxCreate() throws IOException {
    	String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
    	String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
    	sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
    	HTable hTableproduction = new HTable(conf, productionTablePath);
        
        HTable hTableSandbox = new HTable(conf, sandboxTablePath);

        // check if sandbox is created
        assertEquals("sandbox table exists", hba.tableExists(sandboxTablePath), true);
        //assertEquals("sandbox meta file exists", fs.exists(new Path(sandboxTableMetaFile)), true);

        // check if column families of production and sanbox are the sandboxTableMetaFile
        List<String> production_colfam = getColumnFamilies(hTableproduction);
        List<String> sandbox_colfam = getColumnFamilies(hTableSandbox);
        production_colfam.add(0, new String("_shadow")); // sandbox has an additional cf named _shadow

        assertEquals("column families same in production and sandbox",
                        production_colfam, sandbox_colfam);

        // clean up the sandbox
        sandboxAdmin.deleteSandbox(sandboxTablePath);

        // check if sandbox has been removed
        //assertEquals("sandbox table deleted", hba.tableExists(sandboxTablePath), false);
        //assertEquals("sandbox meta file deleted", fs.exists(new Path(sandboxTableMetaFile)), false);
    }

    private static List<String> getColumnFamilies(HTableInterface table) {
      List<String> colFamilies = new ArrayList<String>();
      try {
        Set<byte[]> familySet = table.getTableDescriptor().getFamiliesKeys();
        for (byte[] family : familySet) {
          colFamilies.add(Bytes.toString(family));
        }
      } catch (Exception e) {
      System.err.println(e.getMessage());
		}
		return colFamilies;
	}

}
