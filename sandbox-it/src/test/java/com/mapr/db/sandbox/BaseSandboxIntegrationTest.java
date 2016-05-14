package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.*;

import java.io.IOException;

public abstract class BaseSandboxIntegrationTest {
    static final String FAMILY = "cf";
    static final byte[] FAMILY_BYTES = FAMILY.getBytes();

    // TODO load from settings as env specific
    static final String TABLE_PREFIX = "/philips_sandbox_it_tmp/" + randomName();


    protected static Configuration conf;
    protected static HBaseAdmin hba;
    protected static FileSystem fs;

    static {
        conf = new Configuration();
        try {
            hba = new HBaseAdmin(conf);
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    protected static SandboxAdmin sandboxAdmin;

    protected static String originalTablePath;
    protected String sandboxTablePath;

    @BeforeClass
    public static void setupOriginalTable() throws IOException {
        Path tableDirPath = new Path(TABLE_PREFIX);
        if (!fs.exists(tableDirPath)) {
            fs.mkdirs(tableDirPath);
        }

        sandboxAdmin = new SandboxAdmin(new Configuration());
        originalTablePath = String.format("%s/%s", TABLE_PREFIX, "table");
        sandboxAdmin.createTable(originalTablePath, "cf");
    }

    private static String randomName() {
        return Long.toHexString(Double.doubleToLongBits(Math.random()));
    }

    @Before
    public void setupSandbox() {
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

    // Utils
    static long countRows(ResultScanner scanner) throws IOException {
        long result = 0L;
        for (Result r : scanner) {
            ++result;
        }
        return result;
    }
}
