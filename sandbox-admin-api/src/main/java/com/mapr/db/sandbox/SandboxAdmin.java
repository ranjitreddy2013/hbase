package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.rest.MapRRestClient;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mapr.db.sandbox.utils.SandboxAdminUtils.replicationBytesPending;

public class SandboxAdmin {
    private static final Log LOG = LogFactory.getLog(SandboxAdmin.class);

    private static final long REPLICA_WAIT_POLL_INTERVAL;
    private static final long REPLICA_TO_PROXY_WAIT_TIME;
    static final String LOCK_ACQ_FAIL_MSG = "Sandbox Push Lock could not be acquired";
    public static final String SANDBOX_PUSH_SNAPSHOT_FORMAT = "sandbox_push_%s";

    public static final String CONF_DRILL_JDBC_CONN_STR = "sandbox.drill.jdbc_conn_str";
    public static final String CONF_WAIT_POLL_INTERVAL = "sandbox.wait_poll_interval";
    public static final String CONF_PUSH_EXTRA_WAIT_INTERVAL = "sandbox.push_extra_wait_interval";


    protected static CompositeConfiguration toolConfig = new CompositeConfiguration();

    static {
        // load configuration
        try {
            toolConfig.addConfiguration(new SystemConfiguration());
            toolConfig.addConfiguration(new PropertiesConfiguration("sandbox-tool.properties"));
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }

        REPLICA_WAIT_POLL_INTERVAL = toolConfig.getLong(CONF_WAIT_POLL_INTERVAL, 3000L);
        REPLICA_TO_PROXY_WAIT_TIME = toolConfig.getLong(CONF_PUSH_EXTRA_WAIT_INTERVAL, 6000L);
    }

    MapRFileSystem fs;
    MapRRestClient restClient;
    DrillViewConverter drillViewConverter;
    SandboxTablesListManager globalSandboxListManager;

    private String username;
    private String password;

    public SandboxAdmin(Configuration configuration) throws SandboxException {
        this(configuration,
                toolConfig.getString("sandbox.username", "mapr"),
                toolConfig.getString("sandbox.password", "mapr"));
    }

    public SandboxAdmin(Configuration configuration, String username, String password) throws SandboxException {
        this.username = username;
        this.password = password; // be careful what you keep in memory

        try {
            fs = (MapRFileSystem) FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }

        globalSandboxListManager = SandboxTablesListManager.global(fs);
        this.restClient = new MapRRestClient(toolConfig.getStringArray("sandbox.rest_urls"), username, password);

        if (restClient != null) {
            restClient.testCredentials();
        }
    }


    public void createSandbox(String sandboxTablePath, String originalTablePath) throws SandboxException, IOException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);

        createEmptySandboxTable(sandboxTablePath, originalTablePath);

        // creates paused replication from sand to original; original doesn't incl sand in the upstream
        SandboxAdminUtils.addTableReplica(restClient, sandboxTablePath, originalTablePath, true);
        writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.ENABLED);

        // update lists
        SandboxTablesListManager origTableSandboxListManager = SandboxTablesListManager
                .forOriginalTable(fs, new Path(originalTablePath), originalFid);
        globalSandboxListManager.moveToTop(sandboxTablePath);
        origTableSandboxListManager.moveToTop(sandboxTablePath);
    }


    public void pushSandbox(String sandboxTablePath, boolean snapshot, boolean forcePush) throws IOException, SandboxException {
        pushSandbox(sandboxTablePath, snapshot, forcePush, false);
    }

    /**
     *  @param sandboxTablePath
     * @param snapshot
     * @param forcePush
     * @param deleteSandboxOnFinish
     */
    public void pushSandbox(String sandboxTablePath, boolean snapshot, boolean forcePush, boolean deleteSandboxOnFinish) throws IOException, SandboxException {
        final String LOG_MSG_PREFIX = "Sandbox " + sandboxTablePath + " > ";

        if (!SandboxAdminUtils.isServiceRunningOnCluster(restClient, "gateway")) {
            throw new SandboxException("Gateway service is required for sandbox table push.", null);
        }

        // read sandbox metadata
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);
        final String originalFid = info.get(SandboxTable.InfoType.ORIGINAL_FID);
        final Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);
        String originalTablePath = originalPath.toUri().toString();

        final Path lockFile = SandboxTableUtils.lockFilePath(fs, originalFid, originalPath);
        createLockFile(fs, lockFile);

        try {
            if (forcePush) {
                boolean forcePushSuccess = false;
                try {
                    forcePushSuccess = TouchSandboxChangesJob.touchSandboxChanges(sandboxTablePath,
                            this.restClient.getUsername(), true);
                } catch (Exception e) {
                    throw new SandboxException(LOG_MSG_PREFIX + "Error updating sandbox changes to latest timestamp", e);
                }

                System.out.println(forcePushSuccess);
            }

            LOG.info(LOG_MSG_PREFIX + "Checking if all sandbox CFs are present in original table");
            Set<String> sandboxCFs = SandboxAdminUtils.getTableCFSet(restClient, sandboxTablePath);
            Set<String> originalCFs = SandboxAdminUtils.getTableCFSet(restClient, originalTablePath);

            // check if original table have all CF created
            sandboxCFs.remove(SandboxTable.DEFAULT_DIRTY_CF_NAME);
            sandboxCFs.remove(SandboxTable.DEFAULT_META_CF_NAME);
            if (!originalCFs.containsAll(sandboxCFs)) {
                sandboxCFs.removeAll(originalCFs);
                throw new SandboxException(String.format("Original Table %s does not contain all column families marked for replication: %s ." +
                        "Please create them first and re-attempt push operation.",
                        originalTablePath, StringUtils.join(sandboxCFs, ",")), null);
            }

            String sandboxCfStr = StringUtils.join(sandboxCFs, ",");
            // edit the replication link to limit to those columns (all except meta/dirty)
            LOG.info(LOG_MSG_PREFIX + "Limiting replication to columns = " + sandboxCfStr);
            SandboxAdminUtils.limitColumnsOnTableReplica(restClient,
                    sandboxTablePath, originalTablePath, sandboxCfStr);

            // prevent any kind of editing to the sandbox table
            LOG.info(LOG_MSG_PREFIX + "Lock up sandbox CFs for changes");
            SandboxAdminUtils.lockEditsForTable(restClient, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);
            SandboxAdminUtils.lockEditsForTable(restClient, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);
            for (String sandboxCF : sandboxCFs) {
                SandboxAdminUtils.lockEditsForTable(restClient, sandboxTablePath, sandboxCF);
            }

            // disable sandbox table
            writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.SNAPSHOT_CREATE);

            if (snapshot) {
                String snapshotName = String.format(SANDBOX_PUSH_SNAPSHOT_FORMAT, info.get(SandboxTable.InfoType.SANDBOX_FID));
                Pair<String, Path> volumeInfo = SandboxAdminUtils.getVolumeInfoForPath(restClient, originalPath);
                String origTableVolumeName = volumeInfo.getFirst();
                LOG.info(LOG_MSG_PREFIX + "Creating snapshot " + snapshotName + " in volume " + origTableVolumeName);
                SandboxAdminUtils.createSnapshot(restClient, origTableVolumeName, snapshotName);
                LOG.info(LOG_MSG_PREFIX + "Snapshot created in volume " + origTableVolumeName);
            }

            writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.PUSH_STARTED);

            // TODO do something about keeping the meta cf without affecting the current replication
            // Delete sandbox specific CFs
            SandboxAdminUtils.deleteCF(restClient, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);
            SandboxAdminUtils.deleteCF(restClient, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);

            // add sandbox as upstream of original
            LOG.info(LOG_MSG_PREFIX + "Adding sandbox table to original's upstream list");
            SandboxAdminUtils.addUpstreamTable(restClient, originalTablePath, sandboxTablePath);
            LOG.info(LOG_MSG_PREFIX + "Sandbox added to original's upstream list");

            // Resume repl
            SandboxAdminUtils.resumeReplication(restClient, sandboxTablePath, originalTablePath);
            LOG.info(LOG_MSG_PREFIX + "Sandbox replication to original table resumed");


            // the application will periodically monitor the state of the replication. this method
            // will return when the replication has completed
            LOG.info(LOG_MSG_PREFIX + "Waiting for sandbox table to finish replication");
            int bytesPending = replicationBytesPending(restClient, sandboxTablePath);

            while (bytesPending > 0) {
                try {
                    Thread.sleep(REPLICA_WAIT_POLL_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                bytesPending = replicationBytesPending(restClient, sandboxTablePath);
            }
            LOG.info(LOG_MSG_PREFIX + "Replication to original complete – 0 bytes pending");

            try {
                Thread.sleep(REPLICA_TO_PROXY_WAIT_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // pause replication
            SandboxAdminUtils.pauseReplication(restClient, sandboxTablePath, originalTablePath);
            LOG.info(LOG_MSG_PREFIX + "Sandbox replication link paused after push");

            // remove sandbox as upstream of original
            SandboxAdminUtils.removeUpstreamTable(restClient, originalTablePath, sandboxTablePath);
            LOG.info(LOG_MSG_PREFIX + "Sandbox table removed from original upstream list");

            if (deleteSandboxOnFinish) {
                LOG.info(LOG_MSG_PREFIX + "Deleting sandbox table...");
                deleteSandbox(sandboxTablePath);
                LOG.info(LOG_MSG_PREFIX + "Sandbox table deleted.");
            }
        } finally {
            // remove lock file
            try {
                fs.delete(lockFile);
            } catch (IOException e) {
                LOG.error(LOG_MSG_PREFIX + "Could not delete push lockfile " + lockFile.toString());
            }
        }
        LOG.info(LOG_MSG_PREFIX + "Push sandbox complete.");
    }

    private void createLockFile(MapRFileSystem fs, Path lockFile) throws SandboxException {
        try {
            if (!fs.exists(lockFile)) {
                // create right away
                fs.create(lockFile, false);
                LOG.info(String.format("Lock acquired (file = %s)", lockFile.toString()));
            } else {
                LOG.info(String.format("Failed to acquire lock. File already exists. (file = %s)",
                        lockFile.toString()));
                throw new SandboxException(LOCK_ACQ_FAIL_MSG, null);
            }
        } catch (IOException e) {
            LOG.info(String.format("Error while acquiring lock (file = %s)", lockFile.toString()));
            throw new SandboxException(LOCK_ACQ_FAIL_MSG, e);
        }
    }



    public void deleteSandbox(String sandboxTablePath) throws IOException, SandboxException {
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);

        // delete metadata file FIRST
        Path metadataFilePath = new Path(info.get(SandboxTable.InfoType.METAFILE_PATH));
        fs.delete(metadataFilePath, false);

        // deletes sandbox
        SandboxAdminUtils.deleteTable(restClient, sandboxTablePath);

        // update lists
        String originalFid = info.get(SandboxTable.InfoType.ORIGINAL_FID);
        Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);

        SandboxTablesListManager origTableSandboxListManager = SandboxTablesListManager
                .forOriginalTable(fs, originalPath, originalFid);
        globalSandboxListManager.delete(sandboxTablePath);
        origTableSandboxListManager.delete(sandboxTablePath);
    }

    @VisibleForTesting
    void writeSandboxMetadataFile(String sandboxTablePath, String originalFid, SandboxTable.SandboxState sandboxState) throws IOException {
        Path sandboxMetadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);

        StringBuffer sb = new StringBuffer()
                .append(originalFid).append("\n")
                .append(sandboxState);

        // content contains FID to original table
        SandboxAdminUtils.writeToDfsFile(fs, sandboxMetadataFilePath, sb.toString());
    }

    @VisibleForTesting
    void createEmptySandboxTable(String sandboxTablePath, String originalTablePath) throws SandboxException {
        Path sandboxPath = new Path(sandboxTablePath);

        try {
            if (fs.exists(sandboxPath)) {
                throw new SandboxException(String.format("Sandbox table %s already exists.",
                        sandboxTablePath), null);
            }
        } catch (IOException e) {
            throw new SandboxException(String.format("Could not determine if sandbox table %s already exists.",
                    sandboxTablePath), e);
        }

        SandboxAdminUtils.createSimilarTable(restClient, sandboxTablePath, originalTablePath);

        // create metadata CF
        SandboxAdminUtils.createTableCF(restClient, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);

        // create dirty CF
        SandboxAdminUtils.createTableCF(restClient, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);
    }

    public void info(String originalTablePath) throws IOException, SandboxException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);
        Map<String, Object> props = Maps.newHashMap();
        props.put("fid", originalFid);
        props.put("path", originalTablePath);

        //TODO include the sandboxes - how?

        ObjectMapper om = new ObjectMapper();
        om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        System.out.println(om.writeValueAsString(props));
    }

    public List<String> listRecent(String originalTablePath) throws IOException {
        if (originalTablePath == null) {
            return globalSandboxListManager.getListFromFile();
        }

        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);
        SandboxTablesListManager originalSandboxListManager = SandboxTablesListManager
                .forOriginalTable(fs, new Path(originalTablePath), originalFid);

        return originalSandboxListManager.getListFromFile();
    }

    public void convertDrill(String sandboxTablePath, String drillConnectionString) throws SandboxException, IOException {
        if (drillConnectionString == null) {
            drillConnectionString = toolConfig.getString(CONF_DRILL_JDBC_CONN_STR,"");
        }

        final String LOG_MSG_PREFIX = "Sandbox " + sandboxTablePath + " > ";

        if (!SandboxAdminUtils.isServiceRunningOnCluster(restClient, "drill-bits")) {
            throw new SandboxException("Drill service is required for sandbox table push.", null);
        }

        // read sandbox metadata
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);
        final String originalFid = info.get(SandboxTable.InfoType.ORIGINAL_FID);
        final Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);
        String originalTablePath = originalPath.toUri().toString();

        if (drillViewConverter == null) {
            LOG.info(LOG_MSG_PREFIX + "Connecting to Drill with connStr = " + drillConnectionString);
            drillViewConverter = new DrillViewConverter(
                    drillConnectionString,
                    username, password,
                    originalTablePath, sandboxTablePath);
        }

        LOG.info(LOG_MSG_PREFIX + "Drill Views conversion started.");
        drillViewConverter.convertViews();
        LOG.info(LOG_MSG_PREFIX + "Drill Views conversion ended.");
    }
}
