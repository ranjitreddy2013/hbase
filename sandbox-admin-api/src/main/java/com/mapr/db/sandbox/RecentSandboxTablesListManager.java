package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.mapr.cli.MapRCliUtil;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.List;
import java.util.Random;

/**
 * - Manages the recent tables list of a user.
 * - Persists the list to /user/<user_name>/.recent_sandbox_tables file.
 * - Limits the number of entries in this file to {@link #MAX_RECENT_TABLES_LIST_SIZE}
 *
 */
public class RecentSandboxTablesListManager {
    private static final Logger LOG = Logger.getLogger(RecentSandboxTablesListManager.class);
    private static final int MAX_RECENT_TABLES_LIST_SIZE = 50;
    private static final String RECENT_TABLES_FILE_NAME = ".recent_sandbox_tables";
    private final String user;

    public RecentSandboxTablesListManager(String user) {
        this.user = user;
    }

    /**
     * Check if {@link #user} has a home directory or not.
     * @return
     */
    public boolean hasHomeDir() {
        try {
            return MapRCliUtil.getMapRFileSystem().exists(new Path(getHomeDir())) &&
                    !MapRCliUtil.getMapRFileSystem().isFile(new Path(getHomeDir()));
        } catch (Exception e) {
            LOG.error(e);
        }
        return false;
    }

    /**
     * Adds {@param newTable} to the top of recent tables list.
     * Keeps the list size to {@link #MAX_RECENT_TABLES_LIST_SIZE}
     *
     * @param newTable
     */
    public synchronized void add(String newTable) {
        List<String> recentTables = getListFromFile();
        if (recentTables.indexOf(newTable) == -1) {
            recentTables.add(0, newTable);
            while (recentTables.size() > MAX_RECENT_TABLES_LIST_SIZE) {
                recentTables.remove(recentTables.size() - 1);
            }
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
            Path recentTablesFilePath = new Path(getPathForRecentTablesFile());
            MapRFileSystem mfs = MapRCliUtil.getMapRFileSystem();
            if (mfs.isFile(recentTablesFilePath)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        mfs.open(new Path(getPathForRecentTablesFile()))));
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
            MapRFileSystem mfs = MapRCliUtil.getMapRFileSystem();
            String filePath = getPathForRecentTablesFile();

            // First write to a temp file...
            String tempFilePath = filePath + new Random().nextInt();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    mfs.create(new Path(tempFilePath), true).getWrappedStream()));
            for (String tablePath : recentTablesList) {
                writer.write(tablePath, 0, tablePath.length());
                writer.newLine();
            }
            writer.close();

            // ...then do a rename
            mfs.rename(new Path(tempFilePath), new Path(filePath));
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    private String getPathForRecentTablesFile() {
        return getHomeDir() + "/" + RECENT_TABLES_FILE_NAME;
    }

    private String getHomeDir() {
        return "/user/" + user;
    }
}
