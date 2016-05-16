package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.mapr.cli.*;
import com.mapr.cliframework.base.*;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class SandboxAdmin {
    private static final Log LOG = LogFactory.getLog(SandboxAdmin.class);

    private static final long REPLICA_WAIT_POLL_INTERVAL = 3000L;
    private static final long REPLICA_TO_PROXY_WAIT_TIME = 6000L;
    static final String LOCK_ACQ_FAIL_MSG = "Sandbox Push Lock could not be acquired";
    public static final String SANDBOX_PUSH_SNAPSHOT_FORMAT = "sandbox_push_%s";

    MapRFileSystem fs;
    CLICommandFactory cmdFactory = CLICommandFactory.getInstance();
    CLIShim cliShim = new CLIShim();
    RecentSandboxTablesListManager recentSandboxManager = RecentSandboxTablesListManagers
            .getRecentSandboxTablesListManagerForUser(cliShim.getUserLoginId());

    public void createSandbox(String sandboxTablePath, String originalTablePath) throws SandboxException, IOException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);

        createEmptySandboxTable(sandboxTablePath, originalTablePath);

        // creates paused replication from sand to original; original doesn't incl sand in the upstream
        SandboxAdminUtils.addTableReplica(cmdFactory, sandboxTablePath, originalTablePath, true);
        writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.ENABLED);
        recentSandboxManager.moveToTop(sandboxTablePath);
    }

    /**
     *  @param sandboxTablePath
     * @param snapshot
     * @param forcePush
     */
    public void pushSandbox(String sandboxTablePath, boolean snapshot, boolean forcePush) throws IOException, SandboxException {
        // read sandbox metadata
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);
        final String originalFid = info.get(SandboxTable.InfoType.ORIGINAL_FID);
        final Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);
        String originalTablePath = originalPath.toUri().toString();

        final Path lockFile = SandboxTableUtils.lockFilePath(fs, originalFid, originalPath);
        createLockFile(fs, lockFile);

        // TODO add flag 'force' to make sure all the modifications have a recent timestamp? (pending on testing scenarios)
        if (forcePush) {

        }


        // prevent any kind of editing to the sandbox table // TODO work in progress
//        SandboxAdminUtils.lockEditsForTable(sandboxTablePath);


        // disable sandbox table
        writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.SNAPSHOT_CREATE);

        if (snapshot) {
            String snapshotName = String.format(SANDBOX_PUSH_SNAPSHOT_FORMAT, info.get(SandboxTable.InfoType.SANDBOX_FID));
            Pair<String, Path> volumeInfo = SandboxAdminUtils.getVolumeInfoForPath(fs, originalPath.getParent());
            String origTableVolumeName = volumeInfo.getFirst();
            SandboxAdminUtils.createSnapshot(cmdFactory, origTableVolumeName, snapshotName);
        }

        writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.PUSH_STARTED);

        // TODO do something about keeping the meta cf without affecting the current replication
        // Delete sandbox specific CFs
        SandboxAdminUtils.deleteCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);
        SandboxAdminUtils.deleteCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);

        // add sandbox as upstream of original
        SandboxAdminUtils.addUpstreamTable(cmdFactory, originalTablePath, sandboxTablePath);

        // Resume repl
        SandboxAdminUtils.resumeReplication(cmdFactory, sandboxTablePath, originalTablePath);


        // the application will periodically monitor the state of the replication. this method
        // will return when the replication has completed
        LOG.info("Waiting for sandbox table to finish replication");
        int bytesPending = _getReplicationBytesPending(sandboxTablePath);

        while (bytesPending > 0) {
            try {
                Thread.sleep(REPLICA_WAIT_POLL_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            bytesPending = _getReplicationBytesPending(sandboxTablePath);
        }

        try {
            Thread.sleep(REPLICA_TO_PROXY_WAIT_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // pause replication
        SandboxAdminUtils.pauseReplication(cmdFactory, sandboxTablePath, originalTablePath);

        // remove sandbox as upstream of original
        SandboxAdminUtils.removeUpstreamTable(cmdFactory, originalTablePath, sandboxTablePath);

        // remove lock file
        fs.delete(lockFile); // TODO might be worth handle exception here
    }

    private void createLockFile(MapRFileSystem fs, Path lockFile) throws SandboxException {

        try {
            if (!fs.exists(lockFile)) {
                // create right away
                fs.create(lockFile, false); // TODO might be worth to handle exception here
            } else {
                throw new SandboxException(LOCK_ACQ_FAIL_MSG, null);
            }
        } catch (IOException e) {
            throw new SandboxException(LOCK_ACQ_FAIL_MSG, e);
        }
    }

    private int _getReplicationBytesPending(String table) throws SandboxException {
        ProcessedInput replicaStatusInput = new ProcessedInput(new String[] {
                "table", "replica", "list",
                "-path", table,
                "-refreshnow", "true"
        });

        DbReplicaCommands replicaStatusCmd = null;
        try {
            replicaStatusCmd = (DbReplicaCommands) cmdFactory.getCLI(replicaStatusInput);
        } catch (Exception e) {
            throw new SandboxException("Could not retrieve repl bytes pending", e);
        }

        CommandOutput commandOutput = null;
        try {
            commandOutput = replicaStatusCmd.executeRealCommand();
        } catch (CLIProcessingException e) {
            e.printStackTrace(); // TODO proper error handling
        }

        JSONObject jsonOutput = null;
        try {
            jsonOutput = new JSONObject(commandOutput.toJSONString());
            return jsonOutput.getJSONArray("data").getJSONObject(0).getInt("bytesPending");
        } catch (JSONException e) {
            throw new SandboxException("Could not retrieve repl bytes pending", e);
        }
    }

    public void deleteSandbox(String sandboxTablePath) throws IOException {
        // delete metadata file FIRST
        Path metadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);
        fs.delete(metadataFilePath, false);

        // deletes sandbox
        SandboxAdminUtils.deleteTable(cmdFactory, sandboxTablePath);

        recentSandboxManager.deleteIfNotExist(sandboxTablePath, fs);
    }


    @VisibleForTesting
    SandboxAdmin() {
        setupCommands();
    }

    public SandboxAdmin(Configuration configuration) {
        setupCommands();
        try {
            fs = (MapRFileSystem) FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

        SandboxAdminUtils.createSimilarTable(cmdFactory, sandboxTablePath, originalTablePath);

        // create metadata CF
        SandboxAdminUtils.createTableCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);

        // create dirty CF
        SandboxAdminUtils.createTableCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);
    }

    static void setupCommands() {
        CLICommandRegistry.getInstance().register(DbCommands.tableCommands);
        CLICommandRegistry.getInstance().register(DbCfCommands.cfCommands);
        CLICommandRegistry.getInstance().register(DbReplicaCommands.replicaCommands);
        CLICommandRegistry.getInstance().register(DbUpstreamCommands.upstreamCommands);
        CLICommandRegistry.getInstance().register(SnapshotCommands.snapshotCommands);
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

    public List<String> listRecent() {
        return recentSandboxManager.getListFromFile();
    }

    class CLIShim extends CLIBaseClass {
        public CLIShim() {
            super(null, null);
        }

        @Override
        public CommandOutput executeRealCommand() throws CLIProcessingException {
            return null; // not to be called
        }
    }
}
