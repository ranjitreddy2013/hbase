package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.rest.MapRRestClient;
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

import static com.mapr.db.sandbox.utils.SandboxAdminUtils.replicationBytesPending;

public class SandboxAdmin {
    private static final Log LOG = LogFactory.getLog(SandboxAdmin.class);

    private static final long REPLICA_WAIT_POLL_INTERVAL = 3000L;
    private static final long REPLICA_TO_PROXY_WAIT_TIME = 6000L;
    static final String LOCK_ACQ_FAIL_MSG = "Sandbox Push Lock could not be acquired";
    public static final String SANDBOX_PUSH_SNAPSHOT_FORMAT = "sandbox_push_%s";

    MapRFileSystem fs;
    MapRRestClient restClient;
    RecentSandboxTablesListManager recentSandboxManager;

    public SandboxAdmin(Configuration configuration) throws SandboxException {
        try {
            fs = (MapRFileSystem) FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        restClient = new MapRRestClient("localhost:8443", "mapr", "mapr");
        recentSandboxManager = new RecentSandboxTablesListManager(fs);
    }


    public void createSandbox(String sandboxTablePath, String originalTablePath) throws SandboxException, IOException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);

        createEmptySandboxTable(sandboxTablePath, originalTablePath);

        // creates paused replication from sand to original; original doesn't incl sand in the upstream
        SandboxAdminUtils.addTableReplica(restClient, sandboxTablePath, originalTablePath, true);
        writeSandboxMetadataFile(sandboxTablePath, originalFid, SandboxTable.SandboxState.ENABLED);
        recentSandboxManager.moveToTop(sandboxTablePath);
    }

    /**
     *  @param sandboxTablePath
     * @param snapshot
     * @param forcePush
     */
    public void pushSandbox(String sandboxTablePath, boolean snapshot, boolean forcePush) throws IOException, SandboxException {
        final String LOG_MSG_PREFIX = "Sandbox " + sandboxTablePath + " > ";

        // read sandbox metadata
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);
        final String originalFid = info.get(SandboxTable.InfoType.ORIGINAL_FID);
        final Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);
        String originalTablePath = originalPath.toUri().toString();

        final Path lockFile = SandboxTableUtils.lockFilePath(fs, originalFid, originalPath);
        createLockFile(fs, lockFile);

        try {
            // TODO add flag 'force' to make sure all the modifications have a recent timestamp? (pending on testing scenarios)
            if (forcePush) {
                boolean forcePushSuccess = false;
                try {
                    forcePushSuccess = TouchSandboxChangesJob.touchSandboxChanges(sandboxTablePath, true);
                } catch (Exception e) {
                    throw new SandboxException(LOG_MSG_PREFIX + "Error updating sandbox changes to latest timestamp", e);
                }

                System.out.println(forcePushSuccess);
            }


            // prevent any kind of editing to the sandbox table // TODO work in progress
//        SandboxAdminUtils.lockEditsForTable(sandboxTablePath);


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
                fs.create(lockFile, false); // TODO might be worth to handle exception here
            } else {
                throw new SandboxException(LOCK_ACQ_FAIL_MSG, null);
            }
        } catch (IOException e) {
            throw new SandboxException(LOCK_ACQ_FAIL_MSG, e);
        }
    }



    public void deleteSandbox(String sandboxTablePath) throws IOException, SandboxException {
        // delete metadata file FIRST
        Path metadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);
        fs.delete(metadataFilePath, false);

        // deletes sandbox
        SandboxAdminUtils.deleteTable(restClient, sandboxTablePath);
        recentSandboxManager.deleteIfNotExist(sandboxTablePath, fs);
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

    public List<String> listRecent() {
        return recentSandboxManager.getListFromFile();
    }

}
