package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTable.METADATA_FILENAME_FORMAT;

public class SandboxTableUtils {
    private final static byte[] ONE = new byte[] { 1 };
    public static final String FAMILY_QUALIFIER_SEPARATOR = ":";

    public static Path metafilePath(MapRFileSystem mfs, String sandboxTablePath) throws IOException {
        String sandboxFid = getFidFromPath(mfs, sandboxTablePath);
        Path sandboxPath = new Path(sandboxTablePath);
        return new Path(sandboxPath.getParent(), String.format(METADATA_FILENAME_FORMAT, sandboxFid));
    }

    public static Path pathFromFid(MapRFileSystem mfs, String originalFid) throws IOException {
        try {
            return new Path(mfs.getMountPathFid(originalFid));
        } catch (IOException e) {
            throw new IOException(String.format("Could not resolve path for original table FID %s", originalFid), e);
        }
    }

    public static String getFidFromPath(MapRFileSystem fs, String originalTablePath) throws IOException {
        try {
            return fs.getMapRFileStatus(new Path(originalTablePath)).getFidStr();
        } catch (IOException e) {
            throw new IOException("Could not grab FID from original table", e);
        }
    }

    public static EnumMap<SandboxTable.InfoType, String> readSandboxInfo(MapRFileSystem fs, AbstractHTable sandboxHTable) throws IOException {
        String sandboxTablePath = new String(sandboxHTable.getTableName(), StandardCharsets.UTF_8);
        return readSandboxInfo(fs, sandboxTablePath);
    }

    public static EnumMap<SandboxTable.InfoType, String> readSandboxInfo(MapRFileSystem fs, String sandboxTablePath) throws IOException {
        Path metaFilePath = metafilePath(fs, sandboxTablePath);

        EnumMap<SandboxTable.InfoType, String> info = Maps.newEnumMap(SandboxTable.InfoType.class);

        if (fs.exists(metaFilePath)) {
            // parse sandbox metadata file
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(metaFilePath)));

                info.put(SandboxTable.InfoType.ORIGINAL_FID, br.readLine());
                info.put(SandboxTable.InfoType.PROXY_FID, br.readLine());
                return info;
            } catch (IOException ex) {
                throw new IOException(String.format("Error reading/parsing metadata file for sandbox table %s",
                        sandboxTablePath), ex);
            }
        }

        return null;
    }


    static String buildAnnotatedColumnStr(String family, String column) {
        return family + FAMILY_QUALIFIER_SEPARATOR + column;
    }

    /**
     * Retrieves row's columns from the original table
     * @param originalTable the original table
     * @param row the row
     * @param family the family
     * @return list of columns present in that row -> family
     * @throws IOException
     */
    static List<String> getRowColumns(AbstractHTable originalTable, byte[] row, byte[] family) throws IOException {
        Get get = new Get(row);
        get.addFamily(family);
        Result getResult = originalTable.get(get);

        List<String> result = Lists.newArrayList();
        for (Cell cell : getResult.listCells()) {
            result.add(Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
        }

        return result;
    }

    static Put markForDeletionPut(SandboxTable sandboxTable, Delete delete) throws IOException {
        Put markDeletionPut = new Put(delete.getRow());

        for (List<Cell> cells : delete.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                String family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qualif = Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                List<String> columns = Lists.newArrayList();


                if (qualif.isEmpty()) {
                    // delete all the columns in this column family
                    columns.addAll(getRowColumns(sandboxTable.originalTable, delete.getRow(), family.getBytes()));
                } else {
                    columns.add(qualif);
                }

                for (String column : columns) {
                    String annotatedColumn = buildAnnotatedColumnStr(family, column);
                    markDeletionPut.add(DEFAULT_META_CF, annotatedColumn.getBytes(), ONE);
                }
            }
        }

        return markDeletionPut;
    }
}
