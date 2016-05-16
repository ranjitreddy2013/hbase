package com.mapr.db.sandbox.utils;

import com.mapr.db.sandbox.SandboxException;
import com.mapr.fs.MapRFileSystem;
import com.mapr.rest.MapRRestClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SandboxAdminUtils {
    private static final Log LOG = LogFactory.getLog(SandboxAdminUtils.class);

    public static void createTableCF(MapRRestClient restClient, String tablePath, String cf) throws SandboxException {
        final String urlPath =  String.format("/table/cf/create?path=%s&cfname=%s",
                tablePath, cf);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error creating CF in table path = %s, cf= %s",
                    tablePath, cf), e);
        }
    }

    public static void createSimilarTable(MapRRestClient restClient, String tablePath, String similarToTablePath) throws SandboxException {
        StringBuilder sb = new StringBuilder(String.format("/table/create?path=%s", tablePath));

        if (!StringUtils.isBlank(similarToTablePath)) {
            sb.append("&copymetatype=all").append("&copymetafrom=").append(similarToTablePath);
        }

        final String urlPath = sb.toString();

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error creating table path = %s, similarTable = %s",
                    tablePath, StringUtils.defaultIfBlank(similarToTablePath, "")), e);
        }
    }


    public static void addTableReplica(MapRRestClient restClient, String fromTablePath, String toTablePath, boolean paused) throws SandboxException {
        final String urlPath =  String.format("/table/replica/add?path=%s&replica=%s&synchronous=true&paused=%s",
                fromTablePath, toTablePath, Boolean.toString(paused));

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error adding replica path = %s, replica = %s, paused = %s",
                    fromTablePath, toTablePath, Boolean.toString(paused)), e);
        }
    }

    public static void addUpstreamTable(MapRRestClient restClient, String toTablePath, String fromTablePath) throws SandboxException {
        final String urlPath =  String.format("/table/upstream/add?path=%s&upstream=%s",
                toTablePath, fromTablePath);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error adding upstream in table path = %s with upstream = %s",
                    toTablePath, fromTablePath), e);
        }
    }

    public static void removeUpstreamTable(MapRRestClient restClient, String toTablePath, String fromTablePath) throws SandboxException {
        final String urlPath =  String.format("/table/upstream/remove?path=%s&upstream=%s",
                toTablePath, fromTablePath);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error deleting upstream in table path = %s with upstream = %s",
                    toTablePath, fromTablePath), e);
        }
    }

    public static void deleteTable(MapRRestClient restClient, String tablePath) throws SandboxException {
        final String urlPath =  String.format("/table/delete?path=%s", tablePath);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error deleting table %s", tablePath), e);
        }
    }

    public static void resumeReplication(MapRRestClient restClient, String fromTablePath, String toTablePath) throws SandboxException {
        final String urlPath =  String.format("/table/replica/resume?path=%s&replica=%s",
                fromTablePath, toTablePath);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error resuming replication from path %s to replica %s",
                    fromTablePath, toTablePath), e);
        }
    }

    public static void pauseReplication(MapRRestClient restClient, String fromTablePath, String toTablePath) throws SandboxException {
        final String urlPath =  String.format("/table/replica/pause?path=%s&replica=%s",
                fromTablePath, toTablePath);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error pausing replication from path %s to replica %s",
                    fromTablePath, toTablePath), e);
        }
    }

    public static void deleteCF(MapRRestClient restClient, String tablePath, String cfName) throws SandboxException {
        final String urlPath =  String.format("/table/cf/delete?path=%s&cfname=%s",
                tablePath, cfName);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error deleting CF table = %s, cfname = %s",
                    tablePath, cfName), e);
        }
    }

    public static void writeToDfsFile(MapRFileSystem fs, Path destinationPath, String content) {
        File temp = null;
        try {
            temp = File.createTempFile("tmp_file", ".tmp");
            BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (temp != null) {
            try {
                fs.copyFromLocalFile(true, true, new Path(temp.toURI()), destinationPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Could not create temp file");
            System.exit(-1);
        }

    }

    /**
     * Returns volume Name and volume mount path for the volume where a given file lives
     * @param restClient the rest client
     * @param path path
     * @return a pair with volume name and mount path
     * @throws IOException
     */
    public static Pair<String,Path> getVolumeInfoForPath(MapRRestClient restClient, Path path) throws IOException, SandboxException {
        String volumeName = "mapr.cluster.root";
        Path currentPath =  path;
        while (currentPath.depth() > 0) {
            String name = volumeInfo(restClient, currentPath);

            if (name != null) {
                volumeName = name;
                break;
            }

            currentPath = currentPath.getParent();
        }

        return new Pair<String, Path>(volumeName, currentPath);
    }

    public static void createSnapshot(MapRRestClient restClient, String volumeName, String snapshotName) throws SandboxException {
        final String urlPath =  String.format("/volume/snapshot/create?volume=%s&snapshotname=%s",
                volumeName, snapshotName);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error creating snapshot volume = %s, snapshotname = %s",
                    volumeName, snapshotName), e);
        }
    }

    public static void removeSnapshot(MapRRestClient restClient, String volumeName, String snapshotName) throws SandboxException {
        final String urlPath =  String.format("/volume/snapshot/remove?volume=%s&snapshotname=%s",
                volumeName, snapshotName);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException("Error removing snapshot", e);
        }
    }

    public static void lockEditsForTable(MapRRestClient restClient, String tablePath, String cf) throws SandboxException {
        final String urlPath =  String.format("/table/cf/edit?path=%s&cfname=%s&writeperm=",
                tablePath, cf);

        try {
            restClient.callCommand(urlPath, false);
        } catch (SandboxException e) {
            throw new SandboxException(String.format("Error locking CF %s on table %s ", cf, tablePath), e);
        }
    }

    private static String volumeInfo(MapRRestClient restClient, Path path) throws SandboxException {
        final String urlPath =  String.format("/volume/info?path=%s&columns=volumename", path.toString());

        try {
            JSONObject result = restClient.callCommand(urlPath, true);
            JSONArray data = result.has("data") ? result.getJSONArray("data") : null;

            if (data != null) {
                return data.getJSONObject(0).getString("volumename");
            }
        } catch (SandboxException e) {
            throw new SandboxException("Error getting volume info for path " + path.toString(), e);
        } catch (JSONException e) {
            throw new SandboxException("Error parsing volume info for path " + path.toString(), e);
        }

        return null;
    }

    public static int replicationBytesPending(MapRRestClient restClient, String tablePath) throws SandboxException {
        // TODO columns = bytesPending ?
        final String urlPath =  String.format("/table/replica/list?path=%s&refreshnow=true", tablePath);

        try {
            JSONObject result = restClient.callCommand(urlPath, true);
            JSONArray data = result.has("data") ? result.getJSONArray("data") : null;

            if (data != null) {
                return data.getJSONObject(0).getInt("bytesPending");
            }

            throw new SandboxException("Error getting replication bytes pending for table " + tablePath, null);
        } catch (SandboxException e) {
            throw new SandboxException("Error getting replication bytes pending for table " + tablePath, e);
        } catch (JSONException e) {
            throw new SandboxException("Error parsing replication bytes pending for table " + tablePath, e);
        }
    }
}
