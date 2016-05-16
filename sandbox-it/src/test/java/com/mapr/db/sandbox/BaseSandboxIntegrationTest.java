package com.mapr.db.sandbox;

import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.fs.MapRFileSystem;
import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public abstract class BaseSandboxIntegrationTest {
    static final String FAMILY = "cf";
    static final byte[] FAMILY_BYTES = FAMILY.getBytes();

    // TODO load from settings as env specific
    static String TABLE_PREFIX = "/philips_sandbox_it_tmp/";


    protected static Configuration conf;
    protected static HBaseAdmin hba;
    protected static MapRFileSystem fs;
    protected static CLICommandFactory cmdFactory;

    static {
        conf = new Configuration();
        try {
            hba = new HBaseAdmin(conf);
            fs = (MapRFileSystem) FileSystem.get(conf);
            cmdFactory = CLICommandFactory.getInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    protected static SandboxAdmin sandboxAdmin;

    protected static String originalTablePath;
    protected String sandboxTablePath;


    protected static void assureWorkingDirExists() throws IOException {
        Path tableDirPath = new Path(TABLE_PREFIX);
        if (!fs.exists(tableDirPath)) {
            fs.mkdirs(tableDirPath);
        }
    }

    private static String randomName() {
        return Long.toHexString(Double.doubleToLongBits(Math.random()));
    }

    @Before
    public void setupTest() throws SandboxException, IOException {
        TABLE_PREFIX += randomName();

        assureWorkingDirExists();

        // create original
        sandboxAdmin = new SandboxAdmin(new Configuration());
        originalTablePath = String.format("%s/%s", TABLE_PREFIX, "table");
        SandboxAdminUtils.createTable(cmdFactory, originalTablePath);
        SandboxAdminUtils.createTableCF(cmdFactory, originalTablePath, "cf");

        // sandbox
        sandboxTablePath = String.format("%s_sand", originalTablePath);
        sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);
    }

    @After
    public void cleanupSandboxTable() throws IOException {
        // delete sandbox table if it still exists
        if (fs.exists(new Path(sandboxTablePath))) {
            sandboxAdmin.deleteSandbox(sandboxTablePath);
        }

        // delete original table and cleanup test directory
        sandboxAdmin.deleteTable(originalTablePath);

        // recursive = true  because proxy tables might still exist
        fs.delete(new Path(TABLE_PREFIX), true);
    }
}
