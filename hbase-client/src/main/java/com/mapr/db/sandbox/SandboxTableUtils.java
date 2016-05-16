package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTable.METADATA_FILENAME_FORMAT;

public class SandboxTableUtils {
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
                info.put(SandboxTable.InfoType.PROXY_FID, br.readLine());
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
    static Map<ByteBuffer, List<byte[]>> getRowColumns(AbstractHTable hTable, byte[] row) throws IOException {
        return getRowColumns(hTable, row, null);
    }

    /**
     * Retrieves row's columns from the table for the specified row and column family
     * @param hTable the table
     * @param row the row
     * @param family the family
     * @return map of family -> list ( column qualifiers )
     * @throws IOException
     */
    static Map<ByteBuffer, List<byte[]>> getRowColumns(AbstractHTable hTable, byte[] row, byte[] family) throws IOException {
        Get get = new Get(row);

        if (family != null) {
            get.addFamily(family);
        }

        Result getResult = hTable.get(get);

        Map<ByteBuffer, List<byte[]>> result = Maps.newHashMap();
        for (Cell cell : getResult.rawCells()) {
            final byte[] cellFamily = CellUtil.cloneFamily(cell);
            final byte[] cellQualif = CellUtil.cloneQualifier(cell);

            List<byte[]> familyQualifiers = result.get(ByteBuffer.wrap(cellFamily));
            if (familyQualifiers == null) {
                familyQualifiers = Lists.newArrayList();
                result.put(ByteBuffer.wrap(cellFamily), familyQualifiers);
            }

            familyQualifiers.add(cellQualif);
        }

        return result;
    }

    static Put markForDeletionPut(SandboxTable sandboxTable, Delete delete) throws IOException {
        Put markDeletionPut = new Put(delete.getRow());

        // list of families / columns to delete
        Collection<List<Cell>> cellsByCF = delete.getFamilyCellMap().values();

        Map<ByteBuffer, List<byte[]>> cellsToMarkDeletion = Maps.newHashMap();

        if (cellsByCF.size() == 0) {
            // delete the whole row
            cellsToMarkDeletion.putAll(getRowColumns(sandboxTable.originalTable, delete.getRow()));
        } else {
            for (List<Cell> cells : cellsByCF) {
                for (Cell cell : cells) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualif = CellUtil.cloneQualifier(cell);


                    List<byte[]> qualifiersMarkedForDeletion = cellsToMarkDeletion.get(ByteBuffer.wrap(family));
                    if (qualifiersMarkedForDeletion == null) {
                        qualifiersMarkedForDeletion = Lists.newArrayList();
                        cellsToMarkDeletion.put(ByteBuffer.wrap(family), qualifiersMarkedForDeletion);
                    }

                    // add whole qualifiers existing in the original table for this family
                    if (qualif == null || qualif.length == 0) {
                        // delete all the columns in this column family
                        Map<ByteBuffer, List<byte[]>> familyQualifiersMap = getRowColumns(sandboxTable.originalTable, delete.getRow(), family);
                        List<byte[]> familyQualifiers = familyQualifiersMap.get(ByteBuffer.wrap(family));

                        if (familyQualifiers != null) {
                            qualifiersMarkedForDeletion.addAll(familyQualifiers);
                        }
                    } else {
                        qualifiersMarkedForDeletion.add(qualif);
                    }
                }
            }
        }

        for (Map.Entry<ByteBuffer, List<byte[]>> entry : cellsToMarkDeletion.entrySet()) {
            final byte[] family = entry.getKey().array();
            for (byte[] qualif : entry.getValue()) {
                markDeletionPut.add(DEFAULT_META_CF, buildAnnotatedColumn(family, qualif), ONE);
            }
        }

        return markDeletionPut;
    }

    public static boolean hasValueForColumn(Result result, byte[] family, byte[] qualifier) {
        if (result != null && !result.isEmpty()) {
            return !result.containsColumn(DEFAULT_META_CF, buildAnnotatedColumn(family, qualifier)) &&
                    result.containsColumn(family, qualifier);
        }

        return false;
    }
}
