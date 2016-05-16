package com.mapr.db.sandbox;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTableUtils.buildAnnotatedColumn;
import static com.mapr.db.sandbox.SandboxTableUtils.restrictColumnsForDeletion;

public class SandboxHTable {
    private static final Log LOG = LogFactory.getLog(SandboxHTable.class);
    private static final String CANNOT_MUTATE_WHEN_DISABLED_MSG = "Cannot mutate sandbox table when disabled";

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

    public static Result get(SandboxTable sandboxTable, final Get get) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            return sandboxTable.table.get(get);
        }

        Result originalResult = sandboxTable.originalTable.get(get);
        Result shadowResult = sandboxTable.table.get(enrichGet(get));

        // if result exists on sandbox...
        if (shadowResult != null && !shadowResult.isEmpty()) {
            return mergeResult(shadowResult, originalResult);
        }

        return originalResult;
    }


    public static Result[] get(SandboxTable sandboxTable, List<Get> gets) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            return sandboxTable.table.get(gets);
        }

        Result[] originalResults = sandboxTable.originalTable.get(gets);

        // enrich Gets
        List<Get> enrichedGets = FluentIterable.from(gets).transform(new Function<Get, Get>() {
            @Nullable
            @Override
            public Get apply(@Nullable Get get) {
                return enrichGet(get);
            }
        }).toImmutableList();

        Result[] sandboxResults = sandboxTable.table.get(enrichedGets);

        Result[] result = new Result[gets.size()];

        // TODO check this very well to avoid index out of bounds exceptions
        for (int i = 0; i < result.length; i++) {
            result[i] = mergeResult(sandboxResults[i], originalResults[i]);
        }

        return result;
    }

    public static Result mergeResult(Result shadowResult, Result originalResult) {
        // TODO test with nulls
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

                Pair<byte[], byte[]> cellColumn = SandboxTableUtils.getCellFromMarkedForDeletionCell(cell);

                if (cellColumn != null) {
                    Cell deletedCell = new KeyValue(row, cellColumn.getFirst(), cellColumn.getSecond());
                    markedForDeletion.add(deletedCell);
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

    public static ResultScanner getScanner(SandboxTable sandboxTable, Scan scan) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            return sandboxTable.table.getScanner(scan);
        }

        ResultScanner originalScanner = sandboxTable.originalTable.getScanner(scan);

        // inject retrieval of shadow record metadata
        Scan shadowScan = new Scan(scan);
        if (shadowScan.hasFamilies()) {
            shadowScan.addFamily(DEFAULT_META_CF);
        }

        ResultScanner shadowScanner = sandboxTable.table.getScanner(shadowScan);

        return new MergedResultScanner(shadowScanner, originalScanner, scan);
    }

    public static void delete(final SandboxTable sandboxTable, final List<Delete> deletes) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        HashMap<ByteBuffer, RowMutations> mutationsByRow = Maps.newHashMap();

        for (Delete delete : deletes) {
            final byte[] rowId = delete.getRow();

            if (!mutationsByRow.containsKey(ByteBuffer.wrap(rowId))) {
                mutationsByRow.put(ByteBuffer.wrap(rowId), new RowMutations(rowId));
            }

            final RowMutations rowMutations = mutationsByRow.get(ByteBuffer.wrap(rowId));
            _rowMutationsForDelete(rowMutations, sandboxTable, delete);
        }


        for (ByteBuffer byteBuffer : mutationsByRow.keySet()) {
            byte[] rowId = byteBuffer.array();
            final byte[] transactionId = SandboxTableUtils.generateTransactionId(rowId, null, null);;
            try {
                List<RowMutations> mutations = Lists.newArrayList(mutationsByRow.values());

                AtomicSandboxOp.acquireLock(sandboxTable, rowId, transactionId);
                try {
                    sandboxTable.table.batch(mutations);
                } catch (InterruptedException e) {
                    throw new IOException(e.getMessage(), e);
                }
                sandboxTable.table.flushCommits();
            } finally {
                AtomicSandboxOp.releaseLock(sandboxTable, rowId, transactionId);
            }
        }
    }

    public static void delete(SandboxTable sandboxTable, Delete delete) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        final byte[] rowId = delete.getRow();

        try {
            RowMutations rm = _rowMutationsForDelete(new RowMutations(rowId), sandboxTable, delete);
            // TODO retry?
            boolean result = sandboxTable.table
                    .checkAndMutate(rowId,  SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL,
                            CompareFilter.CompareOp.EQUAL, null, rm);
            System.out.println();
        } catch (IOException e) {
            throw new InterruptedIOException(e.toString());
        }
    }

    private static RowMutations _rowMutationsForDelete(RowMutations rm, SandboxTable sandboxTable, Delete delete) throws IOException {
        byte[] rowId = delete.getRow();
        CellSet cellsToDelete = SandboxTableUtils.getCellsToDelete(sandboxTable, delete);
        Put put = SandboxTableUtils.markForDeletionPut(rowId, cellsToDelete);

        rm.add(restrictColumnsForDeletion(delete, cellsToDelete));
        rm.add(put);
        return rm;
    }

    /**
     * When performing a PUT in the shadow table, it makes sure any delete flag for the column
     * is removed.
     *
     * @param sandboxTable
     * @param put
     */
    public static void put(SandboxTable sandboxTable, Put put) throws InterruptedIOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        final byte[] rowId = put.getRow();
        try {
            RowMutations rm = _rowMutationsForPut(new RowMutations(rowId), put);

            // TODO retry?
            sandboxTable.table
                    .checkAndMutate(rowId,  SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL,
                            CompareFilter.CompareOp.EQUAL, null, rm);
        } catch (IOException e) {
            throw new InterruptedIOException(e.toString());
        }
    }

    private static RowMutations _rowMutationsForPut(RowMutations rm, Put put) throws IOException {
        Delete delete = removeDeletionMarkForPut(put);
        rm.add(delete);
        rm.add(put);
        return rm;
    }

    /**
     * Generates a Delete to remove the deletion mark of the column or columns
     * of the passed put
     * @param put the Put
     * @return the Delete
     */
    static Delete removeDeletionMarkForPut(Put put) {
        Delete delete = new Delete(put.getRow());
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                removeMarkToDeleteCell(delete, cell);
            }
        }

        return delete;
    }

    static Delete removeDeletionMark(byte[] rowId, CellSet cells) {
        Delete delete = new Delete(rowId);

        for (Cell cell : cells) {
            removeMarkToDeleteCell(delete, cell);
        }

        return delete;
    }

    private static void removeMarkToDeleteCell(Delete delete, Cell cell) {
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] qualif = CellUtil.cloneQualifier(cell);

        byte[] annotatedColumn = buildAnnotatedColumn(family, qualif);
        delete.deleteColumn(DEFAULT_META_CF, annotatedColumn);
    }

    public static void put(SandboxTable sandboxTable, final List<Put> puts) throws InterruptedIOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        HashMap<ByteBuffer, RowMutations> mutationsByRow = Maps.newHashMap();

        for (Put put : puts) {
            final byte[] rowId = put.getRow();

            if (!mutationsByRow.containsKey(ByteBuffer.wrap(rowId))) {
                mutationsByRow.put(ByteBuffer.wrap(rowId), new RowMutations(rowId));
            }

            final RowMutations rowMutations = mutationsByRow.get(ByteBuffer.wrap(rowId));
            try {
                _rowMutationsForPut(rowMutations, put);
            } catch (IOException e) {
                new InterruptedIOException(e.getMessage());
            }
        }

        for (ByteBuffer byteBuffer : mutationsByRow.keySet()) {
            byte[] rowId = byteBuffer.array();
            final byte[] transactionId = SandboxTableUtils.generateTransactionId(rowId, null, null);;
            try {
                List<RowMutations> mutations = Lists.newArrayList(mutationsByRow.values());

                try {
                    AtomicSandboxOp.acquireLock(sandboxTable, rowId, transactionId);
                    sandboxTable.table.batch(mutations);
                    sandboxTable.table.flushCommits();
                } catch (IOException e) {
                    new InterruptedIOException(e.getMessage());
                } catch (InterruptedException e) {
                    new InterruptedIOException(e.getMessage());
                }
            } finally {
                try {
                    AtomicSandboxOp.releaseLock(sandboxTable, rowId, transactionId);
                } catch (IOException e) {
                    new InterruptedIOException(e.getMessage());
                }
            }
        }
    }

    public static Result getRowOrBefore(SandboxTable sandboxTable, byte[] row, byte[] family) throws IOException {
        throw new UnsupportedOperationException("This is not supported for MapR table");
    }

    public static void mutateRow(SandboxTable sandboxTable, RowMutations rm) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        final byte[] rowId = rm.getRow();
        RowMutations adaptRM = adaptRowMutations(sandboxTable, rm);

        try {
            // TODO retry?
            sandboxTable.table
                    .checkAndMutate(rowId,  SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL,
                            CompareFilter.CompareOp.EQUAL, null, adaptRM);
        } catch (IOException e) {
            throw new InterruptedIOException(e.toString());
        }
    }

    private static RowMutations adaptRowMutations(SandboxTable sandboxTable, RowMutations rm) throws IOException {
        final byte[] rowId = rm.getRow();
        RowMutations finalRm = new RowMutations(rowId);

        for (Mutation mutation : rm.getMutations()) {
            Class clz = mutation.getClass();
            RowMutations opRm = new RowMutations(rowId);

            // if it is a delete, add the put to metadata table
            if (clz.equals(Delete.class)) {
                _rowMutationsForDelete(finalRm, sandboxTable, (Delete) mutation);
            }  else if (clz.equals(Put.class)) {
                _rowMutationsForPut(finalRm, (Put) mutation);
            }
        }

        return finalRm;
    }

    /**
     * Append might be executed distributedly and it is very important for this operation to
     * happen on the server side. For this reason, the sandbox implementation only makes sure if the column
     * to be appended exists on both original and sandbox. If it does only exist on original the value is
     * read and used as an increment (it fails if it is not an integer).
     * If not, a normal append is executed in the sandbox table and a delete markForDeletion column
     * as well (just like in PUTs).
     * @param sandboxTable the sandboxTable definition
     * @param append the append definition
     * @return the appended result
     * @throws IOException
     */
    public static Result append(SandboxTable sandboxTable, final Append append) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        byte[] rowId = append.getRow();
        AtomicSandboxMutationOp op = new AtomicSandboxMutationOp(sandboxTable, rowId, append) {
            @Override
            protected Result runMutationOnSandbox() throws IOException {
                // increment normally
                return sandboxTable.table.append(append);
            }

            @Override
            protected Result runMutationOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
                // apply transformed append
                final Append adaptedAppend = new Append(rowId);

                for (Map.Entry<byte[], List<Cell>> appendOpFamMapEntry : append.getFamilyCellMap().entrySet()) {
                    final byte[] family = appendOpFamMapEntry.getKey();

                    for (Cell opCell : appendOpFamMapEntry.getValue()) {
                        final byte[] qualififer = CellUtil.cloneQualifier(opCell);
                        final byte[] value = CellUtil.cloneValue(opCell);

                        // if it's in dirty, ignore
                        ByteBuffer dirtyQualifForCell = originalCellToDirtyQualifMap.get(opCell);
                        if (dirtyQualifForCell != null) {
                            // there's a dirty column for this one
                            byte[] dirtyQualififer = dirtyQualifForCell.array();
                            adaptedAppend.add(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualififer, value);
                        } else {
                            adaptedAppend.add(family, qualififer, value);
                        }


                    }
                }

                return this.sandboxTable.table.append(adaptedAppend);
            }
        };

        return op.runMutation();
    }

    /**
     * Increment might be executed distributedly and it is very important for this operation to
     * happen on the server side. For this reason, the sandbox implementation only makes sure if the column
     * to be incremented exists on both original and sandbox. If it does only exist on original the value is
     * read and used as an increment (it fails if it is not an integer).
     * If not, a normal increment is executed in the sandbox table and a delete markForDeletion column
     * as well (just like in PUTs).
     * @param sandboxTable
     * @param increment
     * @return
     * @throws IOException
     */
    public static Result increment(SandboxTable sandboxTable, final Increment increment) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        final byte[] rowId = increment.getRow();

        AtomicSandboxMutationOp op = new AtomicSandboxMutationOp(sandboxTable, rowId, increment) {
            @Override
            protected Result runMutationOnSandbox() throws IOException {
                // increment normally
                return sandboxTable.table.increment(increment);
            }

            @Override
            protected Result runMutationOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
                // apply transformed increment
                final Increment adaptedIncr = new Increment(rowId);

                for (Map.Entry<byte[], List<Cell>> appendOpFamMapEntry : increment.getFamilyCellMap().entrySet()) {
                    final byte[] family = appendOpFamMapEntry.getKey();

                    for (Cell opCell : appendOpFamMapEntry.getValue()) {
                        final byte[] qualifier = CellUtil.cloneQualifier(opCell);
                        final byte[] value = CellUtil.cloneValue(opCell);

                        // if it's in dirty, ignore
                        ByteBuffer dirtyQualifForCell = originalCellToDirtyQualifMap.get(opCell);
                        if (dirtyQualifForCell != null) {
                            // there's a dirty column for this one
                            byte[] dirtyQualififer = dirtyQualifForCell.array();
                            adaptedIncr.addColumn(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualififer, Bytes.toLong(value));
                        } else {
                            adaptedIncr.addColumn(family, qualifier, Bytes.toLong(value));
                        }
                    }
                }

                return this.sandboxTable.table.increment(adaptedIncr);
            }
        };

        return op.runMutation();
    }

    public static long incrementColumnValue(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        Increment increment = new Increment(row);
        increment.addColumn(family, qualifier, amount);
        increment.setDurability(durability);

        Result result = increment(sandboxTable, increment);
        byte[] value = CellUtil.cloneValue(result.getColumnLatestCell(family, qualifier));
        return Bytes.toLong(value);
    }

    public static boolean checkAndPut(SandboxTable sandboxTable, final byte[] rowId, final byte[] family, final byte[] qualifier, final byte[] value, final Put put) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        AtomicSandboxCheckAndXOp op = new AtomicSandboxCheckAndXOp(sandboxTable, rowId, family, qualifier) {

            @Override
            protected boolean runCheckAndXOpOnSandbox() throws IOException {
                RowMutations rm = _rowMutationsForPut(new RowMutations(put.getRow()), put);

                // TODO retry?
                return sandboxTable.table
                        .checkAndMutate(rowId, family, qualifier,  CompareFilter.CompareOp.EQUAL, value, rm);
            }

            @Override
            protected boolean runCheckAndXOpOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
                RowMutations rm = _rowMutationsForPut(new RowMutations(put.getRow()), put);

                Iterator<ByteBuffer> dirtyColIt = originalCellToDirtyQualifMap.values().iterator();
                byte[] dirtyQualifier = dirtyColIt.next().array();

                return sandboxTable.table
                        .checkAndMutate(rowId, SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier, CompareFilter.CompareOp.EQUAL, value, rm);
            }
        };

        return op.runOp();
    }

    public static boolean checkAndDelete(SandboxTable sandboxTable, byte[] rowId, final byte[] family, final byte[] qualifier, final byte[] value, final Delete delete) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        AtomicSandboxCheckAndXOp op = new AtomicSandboxCheckAndXOp(sandboxTable, rowId, family, qualifier) {

            @Override
            protected boolean runCheckAndXOpOnSandbox() throws IOException {
                RowMutations rm = _rowMutationsForDelete(new RowMutations(delete.getRow()), sandboxTable, delete);

                // TODO retry?
                return sandboxTable.table
                        .checkAndMutate(rowId, family, qualifier,  CompareFilter.CompareOp.EQUAL, value, rm);
            }

            @Override
            protected boolean runCheckAndXOpOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
                RowMutations rm = _rowMutationsForDelete(new RowMutations(delete.getRow()), sandboxTable, delete);

                Iterator<ByteBuffer> dirtyColIt = originalCellToDirtyQualifMap.values().iterator();
                byte[] dirtyQualifier = dirtyColIt.next().array();

                return sandboxTable.table
                        .checkAndMutate(rowId, SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier, CompareFilter.CompareOp.EQUAL, value, rm);
            }
        };

        return op.runOp();
    }

    public static boolean checkAndMutate(SandboxTable sandboxTable, byte[] rowId, final byte[] family, final byte[] qualifier,
                                         CompareFilter.CompareOp compareOp, final byte[] value, final RowMutations rowMutations) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            throw new UnsupportedOperationException(CANNOT_MUTATE_WHEN_DISABLED_MSG);
        }

        AtomicSandboxCheckAndXOp op = new AtomicSandboxCheckAndXOp(sandboxTable, rowId, family, qualifier) {

            @Override
            protected boolean runCheckAndXOpOnSandbox() throws IOException {
                RowMutations adaptedRM = adaptRowMutations(sandboxTable, rowMutations);

                // TODO retry?
                return sandboxTable.table
                        .checkAndMutate(rowId, family, qualifier,  CompareFilter.CompareOp.EQUAL, value, adaptedRM);
            }

            @Override
            protected boolean runCheckAndXOpOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
                RowMutations adaptedRM = adaptRowMutations(sandboxTable, rowMutations);

                Iterator<ByteBuffer> dirtyColIt = originalCellToDirtyQualifMap.values().iterator();
                byte[] dirtyQualifier = dirtyColIt.next().array();

                return sandboxTable.table
                        .checkAndMutate(rowId, SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier, CompareFilter.CompareOp.EQUAL, value, adaptedRM);
            }
        };

        return op.runOp();
    }

    public static boolean exists(SandboxTable sandboxTable, Get get) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            return sandboxTable.table.exists(get);
        }

        Result result = get(sandboxTable, get);
        return result != null && !result.isEmpty();
    }

    public static Boolean[] exists(SandboxTable sandboxTable, final List<Get> gets) throws IOException {
        if (!sandboxTable.sandboxFeatureEnabled) {
            return sandboxTable.table.exists(gets);
        }

        return FluentIterable.from(Lists.newArrayList(get(sandboxTable, gets)))
                .transform(new Function<Result, Boolean>() {
                    @Nullable
                    @Override
                    public Boolean apply(@Nullable Result result) {
                        return result != null && !result.isEmpty();
                    }
                }).toArray(Boolean.class);
    }
}
