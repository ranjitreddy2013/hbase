package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Manages the tables list
 */
public class SandboxTablesListManager {
    private static final Logger LOG = Logger.getLogger(SandboxTablesListManager.class);

    private static SandboxTablesListManager GLOBAL_INSTANCE;
    private static Map<String, SandboxTablesListManager> registry = Maps.newHashMap();

    public static final String SBOX_TABLE_LIST_PREFIX_FORMAT = "/user/%s";
    public static final String GLOBAL_SANDBOX_TABLES_LIST_PATH = ".sandbox_tables";
    public static final String ORIGINAL_SBOX_LIST_FILENAME_FORMAT = ".sandbox_list_%s"; // original fid

    private final Path listFilePath;
    private final MapRFileSystem fs;
    private final String ownerUsername;

    public SandboxTablesListManager(MapRFileSystem fs, Path listFilePath, String ownerUsername) {
        this.fs = fs;
        this.listFilePath = listFilePath;
        this.ownerUsername = ownerUsername;
    }



    /**
     * Adds {@param newTable} to the top of tables list.
     *
     * @param newTable
     */
    public synchronized void add(String newTable) {
        List<String> recentTables = getListFromFile();
        if (recentTables.indexOf(newTable) == -1) {
            recentTables.add(0, newTable);
            this.writeListToFile(recentTables);
        }
    }

    /**
     * Move up the {@param mostRecentTable} to the top of the recent tables list, if it exists.
     * Else, add it to the top of the list.
     *
     * @param mostRecentTable
     */
    public synchronized void moveToTop(String mostRecentTable) {
        List<String> recentTables = getListFromFile();
        if (recentTables.indexOf(mostRecentTable) != -1) {
            recentTables.remove(mostRecentTable);
        }
        recentTables.add(0, mostRecentTable);
        this.writeListToFile(recentTables);
    }

    /**
     * Deletes {@param tablePath} from the recent tables list.
     *
     * @param tablePath
     */
    public synchronized void delete(String tablePath) {
        List<String> recentTablesList = getListFromFile();
        if (recentTablesList.remove(tablePath)) {
            writeListToFile(recentTablesList);
        }
    }


    public void deleteIfNotExist(String tablePath, MapRFileSystem mfs) {
        try {
            if (!mfs.exists(new Path(tablePath)) || !mfs.getMapRFileStatus(new Path(tablePath)).isTable()) {
                this.delete(tablePath);
            }
        } catch (IOException e) {
            // ignore the exception
        }
    }


    public List<String> getListFromFile() {
        List<String> tablePaths = Lists.newArrayList();
        try {
            if (fs.isFile(listFilePath)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        fs.open(listFilePath)));
                String path;
                while ((path = reader.readLine()) != null) {
                    tablePaths.add(path);
                }
                reader.close();
            }
        } catch (Exception e) {
            LOG.error(e);
        }
        return tablePaths;
    }

    private void writeListToFile(List<String> recentTablesList) {
        try {
            // First write to a temp file...
            String tempFilePath = listFilePath.toString() + new Random().nextInt();
            Path tmpFile = new Path(tempFilePath);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    fs.create(tmpFile, true).getWrappedStream()));
            for (String tablePath : recentTablesList) {
                writer.write(tablePath, 0, tablePath.length());
                writer.newLine();
            }
            writer.close();

            // set the permissions and then rename
            fs.setPermission(tmpFile, FsPermission.createImmutable((short)00700));

            if (ownerUsername != null) {
                fs.setOwner(tmpFile, ownerUsername, null);
            }

            fs.rename(tmpFile, listFilePath);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    public static SandboxTablesListManager global(MapRFileSystem fs, String username) {
        if (GLOBAL_INSTANCE == null) {
            Path sboxGlobalListFile = new Path(String.format("%s/%s",
                    String.format(SBOX_TABLE_LIST_PREFIX_FORMAT, username),
                    GLOBAL_SANDBOX_TABLES_LIST_PATH));
            GLOBAL_INSTANCE = new SandboxTablesListManager(fs, sboxGlobalListFile, username);
        }

        return GLOBAL_INSTANCE;
    }

    public static SandboxTablesListManager forOriginalTable(MapRFileSystem fs, String originalPath, String originalFid, String username) {
        if (!registry.containsKey(originalPath)) {
            Path originalSboxListFilePath = new Path(String.format("%s/%s",
                    String.format(SBOX_TABLE_LIST_PREFIX_FORMAT, username),
                    String.format(ORIGINAL_SBOX_LIST_FILENAME_FORMAT, originalFid)));

            registry.put(originalPath, new SandboxTablesListManager(fs, originalSboxListFilePath, username));
        }

        return registry.get(originalPath);
    }
}
