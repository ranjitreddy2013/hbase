package com.mapr.db.sandbox;

import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.db.sandbox.utils.SandboxUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.db.sandbox.utils.SandboxUtils;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class BaseSandboxIntegrationTest {
    static final String FAMILY = "cf";
    static final byte[] FAMILY_BYTES = FAMILY.getBytes();

    // TODO load from settings as env specific
    static final String TABLE_PREFIX = "/philips_sandbox_it_tmp/" + randomName();


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

    @BeforeClass
    public static void setupOriginalTable() throws IOException {
        assureWorkingDirExists();

        sandboxAdmin = new SandboxAdmin(new Configuration());
        originalTablePath = String.format("%s/%s", TABLE_PREFIX, "table");
        SandboxUtils.createTable(cmdFactory, originalTablePath);
        SandboxUtils.createTableCF(cmdFactory, originalTablePath, "cf");
    }

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
    public void setupSandbox() throws SandboxException, IOException {
        sandboxTablePath = String.format("%s_sand", originalTablePath);
        sandboxAdmin.createSandbox(sandboxTablePath, originalTablePath);
    }

    @After
    public void cleanupSandboxTable() throws IOException {
        if (fs.exists(new Path(sandboxTablePath))) {
            sandboxAdmin.deleteSandbox(sandboxTablePath);
        }
    }

    @AfterClass
    public static void cleanupOriginalTable() throws IOException {
        sandboxAdmin.deleteTable(originalTablePath);
        fs.delete(new Path(TABLE_PREFIX), false);
    }
}
