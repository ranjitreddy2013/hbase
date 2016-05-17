package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.rest.MapRRestClient;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;

public abstract class BaseSandboxIntegrationTest {
    static final String CF1_NAME = "cf1";
    static final String CF2_NAME = "cf2";
    static final byte[] CF1 = CF1_NAME.getBytes();
    static final byte[] CF2 = CF2_NAME.getBytes();

    static final String COL1_NAME = "col_x";
    static final String COL2_NAME = "col_y";
    static final byte[] COL1 = COL1_NAME.getBytes();
    static final byte[] COL2 = COL2_NAME.getBytes();

    protected static String TEST_WORKING_DIR_PREFIX;
    protected static Configuration conf;
    protected static HBaseAdmin hba;
    protected static MapRFileSystem fs;
    protected static SandboxAdmin sandboxAdmin;
    protected static CompositeConfiguration testConfig = new CompositeConfiguration();

    final Scan scan = new Scan();
    final MapRRestClient restClient;

    static {
        conf = new Configuration();
        try {
            hba = new HBaseAdmin(conf);
            fs = (MapRFileSystem) FileSystem.get(conf);
            sandboxAdmin = new SandboxAdmin(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SandboxException e) {
            throw new RuntimeException(e);
        }

        // load configuration
        try {
            testConfig.addConfiguration(new SystemConfiguration());
            testConfig.addConfiguration(new PropertiesConfiguration("sandbox-integration-tests.properties"));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public BaseSandboxIntegrationTest() {
        TEST_WORKING_DIR_PREFIX = testConfig.getString("sandbox.test.working_dir_prefix", "/sandbox-it");
        String[] restUrls = testConfig.getStringArray("sandbox.test.rest_urls");
        String username = testConfig.getString("sandbox.test.username", "mapr");
        String password = testConfig.getString("sandbox.test.password", "mapr");


        try {
            restClient = new MapRRestClient(restUrls, username, password);
        } catch (SandboxException e) {
            throw new RuntimeException(e);
        }
    }

    protected String testWorkingDir;

    protected String originalTablePath;
    protected String mimicTablePath;
    protected String sandboxTablePath;

    protected HTable hTableOriginal;
    protected HTable hTableSandbox;
    protected HTable hTableMimic;


    private static String randomName() {
        return Long.toHexString(Double.doubleToLongBits(Math.random()));
    }

    @Before
    public void setupTest() throws SandboxException, IOException {
        testWorkingDir = String.format("%s/%s", TEST_WORKING_DIR_PREFIX, randomName());
        SandboxTestUtils.assureWorkingDirExists(fs,testWorkingDir);

        // create original table
        originalTablePath = String.format("%s/%s", testWorkingDir, "table");
        HTableDescriptor origTableDesc = new HTableDescriptor(TableName.valueOf(originalTablePath));
        origTableDesc.addFamily(new HColumnDescriptor(CF1_NAME));
        origTableDesc.addFamily(new HColumnDescriptor(CF2_NAME));
        hba.createTable(origTableDesc);

        // create mimic table
        mimicTablePath = String.format("%s/%s", testWorkingDir, "mimic");
        HTableDescriptor mimicTableDesc = new HTableDescriptor(TableName.valueOf(mimicTablePath));
        mimicTableDesc.addFamily(new HColumnDescriptor(CF1_NAME));
        mimicTableDesc.addFamily(new HColumnDescriptor(CF2_NAME));
        hba.createTable(mimicTableDesc);

        // create sandbox
        sandboxTablePath = String.format("%s_sand", originalTablePath);
        sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);

        // initialize handlers
        conf.unset(SandboxTable.SANDBOX_ENABLED);
        hTableOriginal = new HTable(conf, originalTablePath);
        hTableSandbox = new HTable(conf, sandboxTablePath);
        hTableMimic = new HTable(conf, mimicTablePath);
    }

    @After
    public void cleanupSandboxTable() throws IOException, SandboxException {
        // delete sandbox table if it still exists
        if (fs.exists(new Path(sandboxTablePath))) {
            sandboxAdmin.deleteSandbox(sandboxTablePath);
        }

        // delete original table and cleanup test directory
        SandboxAdminUtils.deleteTable(restClient, originalTablePath);
        SandboxAdminUtils.deleteTable(restClient, mimicTablePath);

        // recursive = true  because proxy tables might still exist
        // TODO we might need to address this
        fs.delete(new Path(testWorkingDir), true);
    }

    protected static final byte[] existingRowId = "row1".getBytes();
    protected static final byte[] existingRowId2 = "row2".getBytes();
    protected static final byte[] existingRowId3 = "row3".getBytes();
    protected static final byte[] newRowId = "row30".getBytes();

    protected void loadData(HTable hTable) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        for (int i = 0; i < 20; i++) {
            byte[] rowId = String.format("row%d", i).getBytes();
            Put put = new Put(rowId);
            put.add(CF1, COL1, Integer.toString(i).getBytes());
            put.add(CF2, COL2, "someString".getBytes());
            hTable.put(put);
        }

        hTable.flushCommits();
    }

    protected void fillOriginalTable() {
        try {
            loadData(hTableOriginal);
            loadData(hTableMimic);
        } catch (Exception e) {
            e.printStackTrace();
            // TODO do something about it
        }
    }

    protected void pushSandbox() throws IOException, SandboxException {
        sandboxAdmin.pushSandbox(sandboxTablePath, false, false);
    }
}