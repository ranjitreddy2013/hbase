package com.mapr.db.sandbox;

import com.mapr.fs.MapRFileSystem;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.rest.MapRRestClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class BaseSandboxIntegrationTest {
	static final String CF1_NAME = "cf1";
	static final String CF2_NAME = "cf2";
	static final byte[] CF1 = CF1_NAME.getBytes();
	static final byte[] CF2 = CF2_NAME.getBytes();

	
	static final String COL1_NAME = "col_x";
	static final String COL2_NAME = "col_y";
	static final byte[] COL1 = COL1_NAME.getBytes();
	static final byte[] COL2 = COL2_NAME.getBytes();
	
    Scan scan = new Scan();

    // TODO load from settings as env specific
	final static String TEST_WORKING_DIR_PREFIX = "/philips_sandbox_it_tmp/";

    protected static Configuration conf;
    protected static HBaseAdmin hba;
    protected static MapRFileSystem fs;
    protected static SandboxAdmin sandboxAdmin;
    protected static MapRRestClient restClient;

    static {
        conf = new Configuration();
        try {
            hba = new HBaseAdmin(conf);
            fs = (MapRFileSystem) FileSystem.get(conf);
            // TODO grab this from configuration
            restClient = new MapRRestClient("localhost:8443", "mapr", "mapr");
            sandboxAdmin = new SandboxAdmin(conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SandboxException e) {
            e.printStackTrace();
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
        testWorkingDir = TEST_WORKING_DIR_PREFIX + randomName();
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
