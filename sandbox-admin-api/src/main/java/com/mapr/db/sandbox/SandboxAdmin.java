package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.mapr.cli.DbCfCommands;
import com.mapr.cli.DbCommands;
import com.mapr.cli.DbReplicaCommands;
import com.mapr.cli.DbUpstreamCommands;
import com.mapr.cliframework.base.*;
import com.mapr.db.sandbox.utils.SandboxUtils;
import com.mapr.fs.MapRFileSystem;
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
import java.util.Map;
import java.util.TreeSet;

public class SandboxAdmin {
    private static final long REPLICA_WAIT_POLL_INTERVAL = 3000L;

    MapRFileSystem fs;
    ProxyManager pm;
    CLICommandFactory cmdFactory = CLICommandFactory.getInstance();

    public void createSandbox(String sandboxTablePath, String originalTablePath) throws SandboxException, IOException {
        String originalFid = SandboxUtils.getFidFromPath(fs, originalTablePath);

        createEmptySandboxTable(sandboxTablePath, originalTablePath);

        // creates proxy if needed, selects best proxy and creates all replication flows (sand -> proxy -> original)
        setupProxy(sandboxTablePath, originalFid);
        writeSandboxMetadataFile(sandboxTablePath, originalTablePath);
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
    void setupProxy(String sandboxTablePath, String originalFid) throws IOException, SandboxException {
        Path originalPath = SandboxUtils.pathFromFid(fs, originalFid);

        TreeSet<ProxyManager.ProxyInfo> proxies = pm.loadProxyInfo(cmdFactory, originalFid, originalPath);

        ProxyManager.ProxyInfo selectedProxy = pm.createProxy(proxies, originalPath, cmdFactory);

        if (!proxies.contains(selectedProxy)) {
            proxies.add(selectedProxy);
            // write proxy list file
            pm.saveProxyInfo(originalFid, originalPath, proxies);
        }

        // wire proxy
        SandboxUtils.setupReplication(cmdFactory, sandboxTablePath, selectedProxy.proxyTablePath, true);
    }

    /**
     *
     * @param sandboxTablePath
     * @param wait
     */
    public void pushSandbox(String sandboxTablePath, boolean wait) throws IOException, SandboxException {
        String originalTablePath = originalTablePathForSandbox(fs, sandboxTablePath);

        // TODO add flag 'force' to make sure all the modifications have a recent timestamp? (pending on testing scenarios)

        CommandOutput commandOutput = null;

        // Delete metadata CF
        ProcessedInput delMetadataCFInput = new ProcessedInput(new String[] {
                "table", "cf", "delete",
                "-path", sandboxTablePath,
                "-cfname", "_shadow" // TODO repl this with constant
        });

        try {
            DbCfCommands delMetadataCFCmd = (DbCfCommands) cmdFactory.getCLI(delMetadataCFInput);
            commandOutput = delMetadataCFCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }

        // Delete metadata CF
        ProcessedInput resumeReplicaInput = new ProcessedInput(new String[] {
                "table", "replica", "resume",
                "-path", sandboxTablePath,
                "-replica", originalTablePath
        });

        try {
            DbReplicaCommands resumeReplicaCmd = (DbReplicaCommands) cmdFactory.getCLI(resumeReplicaInput);
            commandOutput = resumeReplicaCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }


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
        String originalTablePath = originalTablePathForSandbox(fs, sandboxTablePath);

        CommandOutput commandOutput = null;

        // Remove upstream
        ProcessedInput removeUpstreamInput = new ProcessedInput(new String[] {
                "table", "upstream", "remove",
                "-path", originalTablePath,
                "-upstream", sandboxTablePath
        });

        try {
            DbUpstreamCommands removeUpstreamCmd = (DbUpstreamCommands) cmdFactory.getCLI(removeUpstreamInput);
            commandOutput = removeUpstreamCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }


        deleteTable(sandboxTablePath);

        // delete metadata file
        Path metadataFilePath = metadataFilePathForSandboxTable(sandboxTablePath);
        fs.delete(metadataFilePath, false);
    }

    // TODO put this in common project
    public static String originalTablePathForSandbox(FileSystem fs, String sandboxTablePath) throws IOException {
        Path metadataFilePath = metadataFilePathForSandboxTable(sandboxTablePath);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(metadataFilePath)));
        // TODO ^^^ this might file as the metadataFile might not exist... should yield an error
        String originalPath = reader.readLine();

        if (!fs.exists(new Path(originalPath))) {
            throw new IOException(String.format("Original table %s does not exist for sandbox %s.",
                    originalPath, sandboxTablePath));
        }

        return originalPath;
    }

    public static Path metadataFilePathForSandboxTable(String sandboxTablePath) {
        Path sandboxPath = new Path(sandboxTablePath);
        Path sandboxDirectoryPath = sandboxPath.getParent();
        // TODO path should be returned by the same function as the client, to make sure same logic is used
        return new Path(sandboxDirectoryPath, sandboxPath.getName() + "_meta");
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
    void writeSandboxMetadataFile(String sandboxTablePath, String originalTablePath) {
        Path sandboxMetadataFilePath = metadataFilePathForSandboxTable(sandboxTablePath);
        // content contains path to the original table
        SandboxUtils.writeToDfsFile(fs, sandboxMetadataFilePath, originalTablePath);
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

        SandboxUtils.createSimilarTable(cmdFactory, sandboxTablePath, originalTablePath);

        // create metadata CF
        SandboxUtils.createTableCF(cmdFactory, sandboxTablePath, "_shadow");
    }

    public void deleteTable(String tablePath) {
        ProcessedInput tableDeleteInput = new ProcessedInput(new String[] {
                "table", "delete",
                "-path", tablePath
        });

        // Create sandbox table
        CommandOutput commandOutput;
        try {
            DbCommands tableDeleteCmd = (DbCommands) cmdFactory.getCLI(tableDeleteInput);
            commandOutput = tableDeleteCmd.executeRealCommand();
            System.out.println(commandOutput.toPrettyString());
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    static void setupCommands() {
        CLICommandRegistry.getInstance().register(DbCommands.tableCommands);
        CLICommandRegistry.getInstance().register(DbCfCommands.cfCommands);
        CLICommandRegistry.getInstance().register(DbReplicaCommands.replicaCommands);
        CLICommandRegistry.getInstance().register(DbUpstreamCommands.upstreamCommands);
    }

    public void info(String originalTablePath) throws IOException, SandboxException {
        String originalFid = SandboxUtils.getFidFromPath(fs, originalTablePath);
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
