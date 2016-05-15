package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.util.List;

public class SandboxCreateIntegrationTest extends BaseSandboxIntegrationTest {

	protected static Configuration conf = HBaseConfiguration.create();
	
	protected static String productionTablePath;
	
	// TODO remove unnecessary comments
    @BeforeClass
    public static void createproductionTable() throws IOException {
    	assureWorkingDirExists();
        //sandboxAdmin = new SandboxAdmin(new Configuration());
        //String productiontableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
        //productionTablePath = String.format("%s%s", TABLE_PREFIX, tableName);
        //sandboxAdmin.createTable(productionTablePath, "cf");
    	hba = new HBaseAdmin(conf);
    	String productionTableName = Long.toHexString(Double.doubleToLongBits(Math.random()));
    	productionTablePath = String.format("%s%s", TABLE_PREFIX, productionTableName);
    	HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(productionTablePath));
    	tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
    	tableDescriptor.addFamily(new HColumnDescriptor("cf2"));
    	tableDescriptor.addFamily(new HColumnDescriptor("cf3"));
    	hba.createTable(tableDescriptor);
    }

    @AfterClass
    public static void cleanupproductionTable() throws IOException {
    	hba.deleteTable(productionTablePath);
    }

    // TODO remove unnecessary comments
    @Test
    public void testSandboxCreate() throws IOException, SandboxException {
    	String sandboxTablePath = String.format("%s_new_sand", productionTablePath);
    	String sandboxTableMetaFile = String.format("%s_new_meta", sandboxTablePath);
    	sandboxAdmin.createSandbox(sandboxTablePath, productionTablePath);
    	HTable hTableproduction = new HTable(conf, productionTablePath);
        
        HTable hTableSandbox = new HTable(conf, sandboxTablePath);

        // check if sandbox is created
        assertEquals("sandbox table exists", hba.tableExists(sandboxTablePath), true);
        //assertEquals("sandbox meta file exists", fs.exists(new Path(sandboxTableMetaFile)), true);

        // check if column families of production and sanbox are the sandboxTableMetaFile
        List<String> production_colfam = SandboxTestUtils.getColumnFamilies(hTableproduction);
        List<String> sandbox_colfam = SandboxTestUtils.getColumnFamilies(hTableSandbox);
        // TODO load the metadata CF name from constant
        production_colfam.add(0, new String("_shadow")); // sandbox has an additional cf named _shadow

        assertEquals("column families same in production and sandbox",
                        production_colfam, sandbox_colfam);

        // clean up the sandbox
        sandboxAdmin.deleteSandbox(sandboxTablePath);

        // check if sandbox has been removed
        //assertEquals("sandbox table deleted", hba.tableExists(sandboxTablePath), false);
        //assertEquals("sandbox meta file deleted", fs.exists(new Path(sandboxTableMetaFile)), false);
    }
}
