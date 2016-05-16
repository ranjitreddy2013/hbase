package com.mapr.db.sandbox;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mapr.cli.DbUpstreamCommands;
import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.cliframework.base.CommandOutput;
import com.mapr.cliframework.base.ProcessedInput;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import com.mapr.fs.MapRFileSystem;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Manages proxy tables
 */
public class ProxyManager {
    private static final int LIMIT_UPSTREAM_TABLES = 64;
    private static final String PROXYLIST_FILENAME_FORMAT = ".proxylist_%s"; // suffix is FID of original table

    MapRFileSystem fs;
    static String PROXY_TABLENAME_F = ".proxyt_%s_%d";

    public ProxyManager(MapRFileSystem fs) {
        this.fs = fs;
    }

    /**
     * Returns a selected proxy for the sandbox creation. If require, it creates the proxy.
     * @param proxies
     * @param originalPath
     * @param cmdFactory
     * @return
     */
    public ProxyInfo createProxy(TreeSet<ProxyInfo> proxies, Path originalPath, CLICommandFactory cmdFactory) throws SandboxException, IOException {
        // gather how many upstream connections can the original table handle
        int upstreamCount = calcUpstreamCount(cmdFactory, originalPath);

        if (upstreamCount < LIMIT_UPSTREAM_TABLES) {
            ProxyInfo proxyInfo = new ProxyInfo();
            proxyInfo.originalTablePath = originalPath.toUri().toString();
            proxyInfo.originalFid = SandboxAdminUtils.getFidFromPath(fs, proxyInfo.originalTablePath);

            String proxyTableName = String.format(PROXY_TABLENAME_F, proxyInfo.originalFid, proxies.size());
            Path proxyTable = new Path(originalPath.getParent(), proxyTableName);
            proxyInfo.proxyTablePath = proxyTable.toUri().toString();

            // create proxy table
            SandboxAdminUtils.createSimilarTable(cmdFactory, proxyInfo.proxyTablePath, proxyInfo.originalTablePath);

            proxyInfo.proxyFid = SandboxAdminUtils.getFidFromPath(fs, proxyInfo.proxyTablePath);

            // setup replication to original
            SandboxAdminUtils.setupReplication(cmdFactory, proxyInfo.proxyTablePath, proxyInfo.originalTablePath, false);

            return proxyInfo;
        } else {
            if (proxies.size() == 0) {
                throw new SandboxException("Cannot create sandbox table as limit upstream tables were reached in the original table", null);
            }

            // pick one good one from the list of proxies
            return pickBestProxy(proxies);
        }
    }

    @VisibleForTesting
    ProxyInfo pickBestProxy(Set<ProxyInfo> proxies) throws SandboxException {
        TreeSet<ProxyInfo> proxiesByAvailableUpstreamSlots = Sets.newTreeSet(ORDER_BY_UPSTREAM_COUNT);
        proxiesByAvailableUpstreamSlots.addAll(proxies);

        for (ProxyInfo proxyInfo : proxiesByAvailableUpstreamSlots) {
            if (proxyInfo.upstreamCount < LIMIT_UPSTREAM_TABLES) {
                return proxyInfo;
            }
        }

        throw new SandboxException("Cannot create or use any of the proxies - limit of upstream tables reached", null);
    }

    private int calcUpstreamCount(CLICommandFactory cmdFactory, Path tablePath) throws SandboxException {
        try {
            return getUpstreamTables(cmdFactory, tablePath).size();
        } catch (SandboxException e) {
            throw new SandboxException(
                    String.format("Could not determine the number of upstream tables from %s", tablePath.toUri()), e);
        }
    }

    private List<String> getUpstreamTables(CLICommandFactory cmdFactory, Path tablePath) throws SandboxException {
        List<String> result = Lists.newArrayList();

        // count upstream from original table
        ProcessedInput listUpstreamInput = new ProcessedInput(new String[] {
                "table", "upstream", "list",
                "-path", tablePath.toUri().toString()
        });

        CommandOutput commandOutput = null;
        try {
            DbUpstreamCommands listUpstreamCmd = (DbUpstreamCommands) cmdFactory.getCLI(listUpstreamInput);
            commandOutput = listUpstreamCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            throw new SandboxException(
                    String.format("Could not determine the list of upstream tables from %s", tablePath.toUri()), e);
        }

        JSONObject jsonOutput = null;
        try {
            ObjectMapper om = new ObjectMapper();
            Map<String, Object> props = om.readValue(commandOutput.toJSONString(), Map.class);
            List<Map<String,String>> upStreamTables = (List<Map<String,String>>) props.get("data");

            for (Map<String, String> upStreamTable : upStreamTables) {
                result.add(upStreamTable.get("table"));
            }
        } catch (IOException e) {
            throw new SandboxException(
                    String.format("Could not determine the list of upstream tables from %s", tablePath.toUri()), e);
        }

        return result;
    }


    public void saveProxyInfo(String originalFid, Path originalPath, TreeSet<ProxyInfo> proxies) {
        String proxyListFilename = String.format(PROXYLIST_FILENAME_FORMAT, originalFid);
        Path proxyListPath = new Path(originalPath.getParent(), proxyListFilename);

        StringBuilder sb = new StringBuilder();

        for (ProxyInfo proxyInfo : proxies) {
            sb.append(proxyInfo.proxyFid).append("\n");
        }

        SandboxAdminUtils.writeToDfsFile(fs, proxyListPath, sb.toString());
    }


    /**
     * Grabs information about existing proxies for a given original table
     * @param originalFid
     * @param originalPath
     * @return
     */
    public TreeSet<ProxyInfo> loadProxyInfo(CLICommandFactory cmdFactory, String originalFid, Path originalPath) throws IOException, SandboxException {
        String proxyFileName = String.format(PROXYLIST_FILENAME_FORMAT, originalFid);
        Path proxyListPath = new Path(originalPath.getParent(), proxyFileName);
        TreeSet<ProxyInfo> result = Sets.newTreeSet();

        try {
            if (fs.exists(proxyListPath)) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(proxyListPath)));

                String line = null;
                while ((line = br.readLine()) != null) {
                    ProxyInfo proxyInfo = new ProxyInfo();
                    proxyInfo.proxyFid = line;
                    Path proxyPath = SandboxAdminUtils.pathFromFid(fs, proxyInfo.proxyFid);
                    proxyInfo.proxyTablePath = proxyPath.toUri().toString();
                    proxyInfo.upstreamCount = calcUpstreamCount(cmdFactory, proxyPath);

                    // fill with rest of original's info
                    proxyInfo.originalFid = originalFid;
                    proxyInfo.originalTablePath = originalPath.toUri().toString();

                    result.add(proxyInfo);
                }
            }
        } catch (IOException e) {
            throw new SandboxException("Error reading proxy list file", e);
        }

        return result;
    }


    public void deleteProxy(String proxyTable) {
        // TODO
    }

    static class ProxyInfo implements Comparable {
        String proxyFid;
        String proxyTablePath;
        String originalFid;
        String originalTablePath;
        int upstreamCount; // number of sandboxes attached to it
        Map<String, String> sandboxes = Maps.newHashMap();

        @Override
        public int compareTo(Object o) {
            ProxyInfo other = (ProxyInfo) o;
            return new CompareToBuilder()
                    .append(this.proxyFid, other.proxyFid)
                    .toComparison();
        }

        @Override
        public boolean equals(Object o) {
            ProxyInfo other = (ProxyInfo) o;
            return new EqualsBuilder()
                    .append(proxyFid, other.proxyFid)
                    .isEquals();
        }

        public String getProxyFid() {
            return proxyFid;
        }

        public String getProxyTablePath() {
            return proxyTablePath;
        }

        public String getOriginalFid() {
            return originalFid;
        }

        public String getOriginalTablePath() {
            return originalTablePath;
        }

        public int getUpstreamCount() {
            return upstreamCount;
        }
    }

    static Comparator<ProxyInfo> ORDER_BY_UPSTREAM_COUNT = new Comparator<ProxyInfo>() {
        @Override
        public int compare(ProxyInfo o1, ProxyInfo o2) {
            return Integer.compare(o1.upstreamCount, o2.upstreamCount);
        }
    };
}
