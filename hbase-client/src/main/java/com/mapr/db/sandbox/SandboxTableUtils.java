package com.mapr.db.sandbox;

import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTable.METADATA_FILENAME_FORMAT;

public class SandboxTableUtils {
    private final static HashFunction hashF = Hashing.murmur3_128();

    private final static byte[] ONE = new byte[] { 1 };
    public static final byte[] FAMILY_QUALIFIER_SEPARATOR = ":".getBytes();

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
//                info.put(SandboxTable.InfoType.PROXY_FID, br.readLine()); // TODO remove this
                return info;
            } catch (IOException ex) {
                throw new IOException(String.format("Error reading/parsing metadata file for sandbox table %s",
                        sandboxTablePath), ex);
            }
        }

        return null;
    }


    static byte[] buildAnnotatedColumn(byte[] family, byte[] qualif) {
        byte[] result = new byte[family.length + FAMILY_QUALIFIER_SEPARATOR.length + qualif.length];
        System.arraycopy(family, 0, result, 0, family.length);
        System.arraycopy(FAMILY_QUALIFIER_SEPARATOR, 0, result, family.length, FAMILY_QUALIFIER_SEPARATOR.length);
        System.arraycopy(qualif, 0, result, family.length + FAMILY_QUALIFIER_SEPARATOR.length, qualif.length);

        return result;
    }


    /**
     * Retrieves row's columns from the table for the specified row
     * @return map of family -> list ( column qualifiers )
     */
    static CellSet getRowColumns(AbstractHTable hTable, byte[] row, boolean filterOutMetaCF) throws IOException {
        return getRowColumns(hTable, row, null, filterOutMetaCF);
    }

    /**
     * Retrieves row's columns from the table for the specified row and column family
     * @param hTable the table
     * @param row the row
     * @param family the family
     * @return map of family -> list ( column qualifiers )
     * @throws IOException
     */
    static CellSet getRowColumns(AbstractHTable hTable, byte[] row, byte[] family, boolean filterOutMetaCF) throws IOException {
        Get get = new Get(row);

        if (family != null) {
            get.addFamily(family);
        }

        Result getResult = hTable.get(get);
        
        CellSet cellSet = new CellSet();
        for (Cell cell : getResult.rawCells()) {
            final byte[] cellFamily = CellUtil.cloneFamily(cell);
            final byte[] cellQualifier = CellUtil.cloneQualifier(cell);

            if (filterOutMetaCF && Arrays.equals(cellFamily, SandboxTable.DEFAULT_META_CF)) {
                continue;
            }

            cellSet.add(row, cellFamily, cellQualifier);
        }

        return cellSet;
    }

    static CellSet getCellsToDelete(SandboxTable sandboxTable, Delete delete) throws IOException {
        // list of families / columns to delete
        byte[] rowId = delete.getRow();
        Collection<List<Cell>> cellsByCF = delete.getFamilyCellMap().values();

        CellSet cellSet = new CellSet();

        if (cellsByCF.size() == 0) {
            // delete the whole row
            cellSet.addAll(getRowColumns(sandboxTable.originalTable, rowId, false));
            cellSet.addAll(getRowColumns(sandboxTable.table, rowId, true));
        } else {
            for (List<Cell> cells : cellsByCF) {
                for (Cell cell : cells) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualif = CellUtil.cloneQualifier(cell);

                    // add whole qualifiers existing in the original table for this family
                    if (qualif == null || qualif.length == 0) {
                        // delete all the columns in this column family
                        cellSet.addAll(getRowColumns(sandboxTable.originalTable, rowId, family, false));
                    } else {
                        cellSet.add(rowId, family, qualif);
                    }
                }
            }
        }

        return cellSet;
    }

    static Put markForDeletionPut(final byte[] rowId , final CellSet cellsToMarkDeletion) throws IOException {
        Put markDeletionPut = new Put(rowId);

        for (Cell cell : cellsToMarkDeletion) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            markDeletionPut.add(DEFAULT_META_CF, buildAnnotatedColumn(family, qualifier), ONE);
        }

        return markDeletionPut;
    }

    /**
     * On a full row deletion, the markAsDeleted columns will also be removed.
     * This method returns a transformed Delete operation with limited scope to the existing columns in the row
     * @param delete
     * @param cellsToDelete
     * @return
     */
    static Delete restrictColumnsForDeletion(Delete delete, CellSet cellsToDelete) {
        // full row deletion detect
        if (delete.getFamilyCellMap().size() == 0) {
            Delete limitedDel = new Delete(delete.getRow());

            for (Cell cell : cellsToDelete) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                limitedDel.deleteColumns(family, qualifier);
            }

            return limitedDel;
        }
        return delete;
    }

    public static boolean hasValueForColumn(Result result, byte[] family, byte[] qualifier) {
        return result != null &&
                !result.isEmpty() &&
                result.containsColumn(family, qualifier);
    }

    public static boolean hasDeletionMarkForColumn(Result result, byte[] family, byte[] qualifier) {
        return result != null &&
                !result.isEmpty() &&
                result.containsColumn(DEFAULT_META_CF, buildAnnotatedColumn(family, qualifier));
    }

    public static Path lockFilePath(MapRFileSystem fs, String originalFid, Path originalPath) {
        return new Path(originalPath.getParent(), String.format(".pushlock_%s", originalFid));
    }

    public static byte[] generateTransactionId(Mutation mutation) {
        return hashF.hashString(mutation.toString()).asBytes();
    }

    public static byte[] generateTransactionId(final byte[] rowId, final byte[] family, final byte[] qualifier) {
        StringBuffer sb = new StringBuffer(Arrays.toString(rowId)).append("\n")
                .append(Arrays.toString(family)).append("\n")
                .append(Arrays.toString(qualifier));
        return hashF.hashString(sb.toString()).asBytes();
    }

    public static Get enrichGet(Get get) {
        // add only if there is a specific set of columns, otherwise it will retrieve all
        if (get.hasFamilies()) {
            Get result = new Get(get.getRow());
            result.addFamily(DEFAULT_META_CF);

            final Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
            for (byte[] family : familyMap.keySet()) {
                NavigableSet<byte[]> qualifiers = familyMap.get(family);

                if (qualifiers != null) {
                    for (byte[] qualifier : qualifiers) {
                        result.addColumn(family, qualifier);
                    }
                } else {
                    result.addFamily(family);
                }
            }

            return result;
        }

        return get;
    }
}
