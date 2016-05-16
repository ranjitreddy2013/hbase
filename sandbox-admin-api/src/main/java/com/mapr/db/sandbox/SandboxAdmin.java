package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.mapr.cli.DbCfCommands;
import com.mapr.cli.DbCommands;
import com.mapr.cli.DbReplicaCommands;
import com.mapr.cli.DbUpstreamCommands;
import com.mapr.cliframework.base.*;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeSet;

public class SandboxAdmin {
    private static final Log LOG = LogFactory.getLog(SandboxAdmin.class);

    private static final long REPLICA_WAIT_POLL_INTERVAL = 3000L;
    private static final long REPLICA_TO_PROXY_WAIT_TIME = 12000L;

    MapRFileSystem fs;
    ProxyManager pm;
    CLICommandFactory cmdFactory = CLICommandFactory.getInstance();

    public void createSandbox(String sandboxTablePath, String originalTablePath) throws SandboxException, IOException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);

        createEmptySandboxTable(sandboxTablePath, originalTablePath);

        // creates proxy if needed, selects best proxy and creates all replication flows (sand -> proxy -> original)
        ProxyManager.ProxyInfo proxyInfo = setupProxy(sandboxTablePath, originalFid);
        writeSandboxMetadataFile(sandboxTablePath, originalFid, proxyInfo);
    }

    /**
     * creates proxy if needed, selects best proxy and
     * creates all replication flows (sand -> proxy -> original)
     * @param sandboxTablePath
     * @param originalFid
     * @throws IOException
     * @throws SandboxException
     */
    @VisibleForTesting
    ProxyManager.ProxyInfo setupProxy(String sandboxTablePath, String originalFid) throws IOException, SandboxException {
        Path originalPath = SandboxTableUtils.pathFromFid(fs, originalFid);

        TreeSet<ProxyManager.ProxyInfo> proxies = pm.loadProxyInfo(cmdFactory, originalFid, originalPath);

        ProxyManager.ProxyInfo selectedProxy = pm.createProxy(proxies, originalPath, cmdFactory);

        if (!proxies.contains(selectedProxy)) {
            proxies.add(selectedProxy);
            // write proxy list file
            pm.saveProxyInfo(originalFid, originalPath, proxies);
        }

        // wire proxy
        // wire proxy
        SandboxAdminUtils.addTableReplica(cmdFactory, sandboxTablePath, selectedProxy.proxyTablePath, true);
        SandboxAdminUtils.addUpstreamTable(cmdFactory, selectedProxy.proxyTablePath, sandboxTablePath);

        return selectedProxy;
    }

    /**
     *  @param sandboxTablePath
     * @param wait
     * @param snapshot
     * @param forcePush
     */
    public void pushSandbox(String sandboxTablePath, boolean wait, boolean snapshot, boolean forcePush) throws IOException, SandboxException {
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);

        String proxyFid = info.get(SandboxTable.InfoType.PROXY_FID);
        Path proxyPath = SandboxTableUtils.pathFromFid(fs, proxyFid);
        String proxyTablePath = proxyPath.toUri().toString();

        // TODO add flag 'force' to make sure all the modifications have a recent timestamp? (pending on testing scenarios)


        // Delete metadata CF
        SandboxAdminUtils.deleteCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_META_CF_NAME);
        SandboxAdminUtils.deleteCF(cmdFactory, sandboxTablePath, SandboxTable.DEFAULT_DIRTY_CF_NAME);


        // Resume repl
        SandboxAdminUtils.resumeReplication(cmdFactory, sandboxTablePath, proxyTablePath);


        // if wait is set, the application will periodically monitor the state of the replication. this method
        // will return when the replication has completed
        if (wait) {
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

            LOG.info("Waiting for proxy table to finish replication");
            bytesPending = _getReplicationBytesPending(proxyTablePath);
            while (bytesPending > 0) {
                try {
                    Thread.sleep(REPLICA_WAIT_POLL_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                bytesPending = _getReplicationBytesPending(proxyTablePath);
            }
        }

        // delete sandbox TODO revise this
        try {
            deleteSandbox(sandboxTablePath);
        } catch (IOException e) {
            e.printStackTrace(); // TODO handling?
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
        EnumMap<SandboxTable.InfoType, String> info = SandboxTableUtils.readSandboxInfo(fs, sandboxTablePath);

        String proxyFid = info.get(SandboxTable.InfoType.PROXY_FID);
        Path proxyTablePath = SandboxTableUtils.pathFromFid(fs, proxyFid);

        SandboxAdminUtils.removeUpstreamTable(cmdFactory, proxyTablePath.toUri().toString(), sandboxTablePath);

        // delete metadata file FIRST
        Path metadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);
        fs.delete(metadataFilePath, false);

        // deletes sandbox
        SandboxAdminUtils.deleteTable(cmdFactory, sandboxTablePath);

        // TODO do something with proxy?!
//        String originalTablePath = info.get(SandboxTable.InfoType.ORIGINAL_FID);
    }

    // TODO put this in common project
    public static String originalTablePathForSandbox(MapRFileSystem fs, String sandboxTablePath) throws IOException {
        Path metadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(metadataFilePath)));
        // TODO ^^^ this might file as the metadataFile might not exist... should yield an error
        String originalPath = reader.readLine();



        if (!fs.exists(new Path(originalPath))) {
            throw new IOException(String.format("Original table %s does not exist for sandbox %s.",
                    originalPath, sandboxTablePath));
        }

        return originalPath;
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
        pm = new ProxyManager(fs);
    }

    @VisibleForTesting
    void writeSandboxMetadataFile(String sandboxTablePath, String originalFid, ProxyManager.ProxyInfo proxyInfo) throws IOException {
        Path sandboxMetadataFilePath = SandboxTableUtils.metafilePath(fs, sandboxTablePath);

        // content contains FID to original table in the 1st line and FID of the proxy in the second line
        StringBuffer sb = new StringBuffer()
                .append(originalFid).append("\n")
                .append(proxyInfo.getProxyFid());

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
    }

    public void info(String originalTablePath) throws IOException, SandboxException {
        String originalFid = SandboxTableUtils.getFidFromPath(fs, originalTablePath);
        Map<String, Object> props = Maps.newHashMap();
        props.put("fid", originalFid);
        props.put("path", originalTablePath);

        TreeSet<ProxyManager.ProxyInfo> proxies = pm.loadProxyInfo(cmdFactory, originalFid, new Path(originalTablePath));
        props.put("proxyTables", proxies);

        //TODO include the sandboxes

        ObjectMapper om = new ObjectMapper();
        om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        System.out.println(om.writeValueAsString(props));
    }
}
