package com.mapr.db.shadow;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;

import java.util.*;

public class ShadowTableUtils {
    private static final Log LOG = LogFactory.getLog(ShadowTableUtils.class);
    public static final byte[] DEFAULT_META_CF = new String("_shadow").getBytes();

    private static final String FAMILY_QUALIFIER_SEPARATOR = ":";

    // assumes cells belong to the same cell
    public final static Comparator<Cell> SAME_ROW_CELL_COMPARATOR = new Comparator<Cell>() {
        @Override
        public int compare(Cell left, Cell right) {
            int compare = Bytes.compareTo(
                    left.getFamilyArray(),  left.getFamilyOffset(),  left.getFamilyLength(),
                    right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());

            if (compare != 0) {
                return compare;
            }

            // Compare qualifier
            compare = Bytes.compareTo(
                    left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(),
                    right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());

            return compare;
        }
    };

    public static String originalTablePathFor(AbstractHTable shadowTable) throws IOException {
        // detect if there's a metadata file
        String shadowTablePath = new String(shadowTable.getTableName(), StandardCharsets.UTF_8);

        FileSystem fs = FileSystem.get(new Configuration());
        Path metaFilePath = new Path(String.format("%s_meta", shadowTablePath)); // TODO change this

        if (fs.exists(metaFilePath)) {
            // parse shadow file
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(metaFilePath)));
                String line = br.readLine();

                // TODO potential sec issue?
                if (fs.exists(new Path(line))) {
                    LOG.info("Detected shadow table " + shadowTablePath + " -> original: " + line);
                    return line;
                }
            } catch (IOException ex) {
                LOG.error("Error reading/parsing metadata file for shadow table " + shadowTablePath, ex);
            }
        } else {
            LOG.debug("There is not shadow information for table " + shadowTablePath);
        }

        return null;
    }



    public static Result[] get(ShadowTable shadowTable, List<Get> gets) throws IOException {
        // TODO return two sets of GET requests – some for shadow some for original
        Result[] results = new Result[gets.size()];

        // ignore uncomplete requests

        // merge results :)

        try {
            shadowTable.table.batch(gets, results);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return results;
    }

    public static Result get(ShadowTable shadowTable, Get get) throws IOException {
        Result originalResult = shadowTable.originalTable.get(get);

        try {
            LOG.debug("get " + get.toMap());
            Result shadowResult = shadowTable.table.get(enrichGet(get));

            // if result exists on shadow...
            if (shadowResult != null && !shadowResult.isEmpty()) {
                return mergeResult(shadowResult, originalResult);
            }
        } catch (Exception ex) {
            LOG.error("Error GET", ex);
        }

        return originalResult;
    }

    public static Result mergeResult(Result shadowResult, Result originalResult) {
        Set<Cell> cells = Sets.newTreeSet(SAME_ROW_CELL_COMPARATOR);
        byte[] row = shadowResult.getRow();

        if (originalResult != null && !originalResult.isEmpty()) {
            cells.addAll(originalResult.listCells());
        }

        //
        Set<Cell> markedForDeletion = Sets.newTreeSet(SAME_ROW_CELL_COMPARATOR);
        List<Cell> extendingCells = Lists.newArrayList();

        for (Cell cell : shadowResult.rawCells()) {
            // is cell marked for deletion?
            if (Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    DEFAULT_META_CF, 0, DEFAULT_META_CF.length) == 0) {

                String deleted = Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String[] deletedCol = deleted.split(FAMILY_QUALIFIER_SEPARATOR);

                try {
                    Cell deletedCell = new KeyValue(row, deletedCol[0].getBytes(), deletedCol[1].getBytes());
                    markedForDeletion.add(deletedCell);
                } catch (ArrayIndexOutOfBoundsException ex) {
                    // doesn't matter... it's a parsing error on the metadata notation
                }
            } else {
                // if not, cell is extending / overriding current original row's value
                extendingCells.add(cell);
            }
        }

        cells.removeAll(markedForDeletion);

        for (Cell cell : extendingCells) {
            if (markedForDeletion.contains(cell)) {
                continue;
            }

            cells.remove(cell); // takes out the old one
            cells.add(cell); // adds the new one (replacing)
        }

        Cell[] result = new Cell[cells.size()];
        cells.toArray(result);
        return Result.create(result);
    }


    private static Get enrichGet(Get get) {
        // add only if there is a specific set of columns, otherwise it will retrieve all
        if (get.hasFamilies()) {
            get.addFamily(DEFAULT_META_CF);
        }

        return get;
    }

    public static ResultScanner getScanner(ShadowTable shadowTable, Scan scan) throws IOException {
        ResultScanner originalScanner = shadowTable.originalTable.getScanner(scan);

        // inject retrieval of shadow record metadata
        Scan shadowScan = new Scan(scan);
        if (shadowScan.hasFamilies()) {
            shadowScan.addFamily(DEFAULT_META_CF);
        }

        ResultScanner shadowScanner = shadowTable.table.getScanner(shadowScan);

        return new MergedResultScanner(shadowScanner, originalScanner, scan);
    }

    private final static byte[] ONE = new byte[] { 1 };
    public static void delete(ShadowTable shadowTable, Delete delete) throws IOException {
        shadowTable.table.delete(delete);
        markAsDeleted(shadowTable, delete);
    }

    private static void markAsDeleted(ShadowTable shadowTable, Delete delete) throws IOException {
        Put markDeletionPut = new Put(delete.getRow());

        for (List<Cell> cells : delete.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                String family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qualif = Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                List<String> columns = Lists.newArrayList();


                if (qualif.isEmpty()) {
                    // delete all the columns in this column family
                    columns.addAll(getRowColumns(shadowTable.originalTable, delete.getRow(), family.getBytes()));
                } else {
                    columns.add(qualif);
                }

                for (String column : columns) {
                    String annotatedColumn = buildAnnotatedColumnStr(family, column);
                    markDeletionPut.add(DEFAULT_META_CF, annotatedColumn.getBytes(), ONE);
                }
            }
        }

        shadowTable.table.put(markDeletionPut);
        shadowTable.table.flushCommits();
    }

    private static String buildAnnotatedColumnStr(String family, String column) {
        return new StringBuilder().append(family)
                .append(FAMILY_QUALIFIER_SEPARATOR)
                .append(column).toString();
    }

    /**
     * Retrieves row's columns from the original table
     * @param originalTable
     * @param row
     * @param family
     * @return
     * @throws IOException
     */
    private static List<String> getRowColumns(AbstractHTable originalTable, byte[] row, byte[] family) throws IOException {
        Get get = new Get(row);
        get.addFamily(family);
        Result getResult = originalTable.get(get);

        List<String> result = Lists.newArrayList();
        for (Cell cell : getResult.listCells()) {
            result.add(Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
        }

        return result;
    }

    /**
     * When performing a PUT in the shadow table, it makes sure any delete flag for the column
     * is removed.
     *
     * @param shadowTable
     * @param put
     */
    public static void put(ShadowTable shadowTable, Put put) throws InterruptedIOException {
        Delete delete = new Delete(put.getRow());
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                String family = Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                String qualif = Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                String annotatedColumn = buildAnnotatedColumnStr(family, qualif);
                delete.deleteColumn(DEFAULT_META_CF, annotatedColumn.getBytes());
            }
        }
        // is there a better way than this?
        try {
            shadowTable.table.delete(delete);
            shadowTable.table.put(put);
        } catch (IOException e) {
            throw new InterruptedIOException(e.toString());
        }
    }
}
