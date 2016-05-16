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
        final byte[] rowId = rm.getRow();
        RowMutations finalRm = new RowMutations(rowId);

        for (Mutation mutation : rm.getMutations()) {
            Class clz = mutation.getClass();
            // if it is a delete, add the put to metadata table
            if (clz.equals(Delete.class)) {
                Delete delete = (Delete) mutation;
//                Put markForDeletionPut = null;SandboxTableUtils.markForDeletionPut(sandboxTable, delete);
                finalRm.add(delete);
                // TODO change!
//                finalRm.add(markForDeletionPut);
            }  else if (clz.equals(Put.class)) {
                // if it is a PUT, make sure there is no delete there
                Put put = (Put) mutation;
                Delete deletionMarkDelete = removeDeletionMarkForPut(put);
                finalRm.add(put);
                finalRm.add(deletionMarkDelete);
            }
        }

		try {
			// TODO retry?
			sandboxTable.table.checkAndMutate(rowId,
					SandboxTable.DEFAULT_DIRTY_CF,
					SandboxTable.DEFAULT_TID_COL,
					CompareFilter.CompareOp.EQUAL, null, rm);
		} catch (IOException e) {
			throw new InterruptedIOException(e.toString());
		}
	}
    
	/**
	 * Append might be executed distributedly and it is very important for this
	 * operation to happen on the server side. For this reason, the sandbox
	 * implementation only makes sure if the column to be appended exists on
	 * both original and sandbox. If it does only exist on original the value is
	 * read and used as an increment (it fails if it is not an integer). If not,
	 * a normal append is executed in the sandbox table and a delete
	 * markForDeletion column as well (just like in PUTs).
	 * 
	 * @param sandboxTable
	 *            the sandboxTable definition
	 * @param append
	 *            the append definition
	 * @return the appended result
	 * @throws IOException
	 */

	public static Result append(SandboxTable sandboxTable, Append append)
			throws IOException {
		byte[] rowId = append.getRow();
		// fetch which columns are going to be appended
		// TODO use CellSet
		NavigableMap<byte[], List<Cell>> familyCellMap = append
				.getFamilyCellMap();

		RowMutations rowMutations = new RowMutations(rowId);

		// fetch the column versions from both sides
		Get get = new Get(rowId);
		for (byte[] family : familyCellMap.keySet()) {
			List<Cell> cellToAppend = familyCellMap.get(family);

			for (Cell cell : cellToAppend) {
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				get.addColumn(family, qualifier);
			}
		}

		// fetch merged result
		Result result = get(sandboxTable, get);

		for (byte[] family : familyCellMap.keySet()) {
			List<Cell> cellToAppend = familyCellMap.get(family);

			for (Cell cell : cellToAppend) {
				byte[] qualifier = CellUtil.cloneQualifier(cell);

				// get cell from merged result
				Cell lastVersionCell = result.getColumnLatestCell(family,
						qualifier);

				byte[] existingValue = new byte[0];
				if (lastVersionCell != null) {
					existingValue = CellUtil.cloneValue(lastVersionCell);
				}
				byte[] appendValue = CellUtil.cloneValue(cell);

				byte[] resultValue = new byte[existingValue.length
						+ appendValue.length];
				System.arraycopy(existingValue, 0, resultValue, 0,
						existingValue.length);
				System.arraycopy(appendValue, 0, resultValue,
						existingValue.length, appendValue.length);

				Put put = new Put(rowId);
				put.add(family, qualifier, resultValue);
				rowMutations.add(put);
			}
		}

		mutateRow(sandboxTable, rowMutations);
		return get(sandboxTable, get);
	}

	static final int MAX_TRIES = 300;

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
    public static Result increment(SandboxTable sandboxTable, Increment increment) throws IOException {
    	final byte[] rowId = increment.getRow();
        final byte[] transactionId = generateTransactionId(increment);
        Result result = null;

        // attempt to acquire lock
        int tries = MAX_TRIES + 1;
        try {
            while (!acquireLock(sandboxTable, rowId, transactionId) && --tries > 0) {
                Thread.sleep(1L);


            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (tries == 0) {

            throw new IOException("Sandbox Row Lock – could not acquire lock");
        }

        // empty unless columns are copied to dirty CF. format   dirtyColumnQualif => Cell(family,qualifier) to be incremented
        Map<ByteBuffer, Cell> cellToDirtyColumnQualifMap = Maps.newHashMap();

        // lock acquired
        try {
            CellSet cellSet = new CellSet(increment.getFamilyCellMap());

            // TODO extract method
            final Get get = new Get(rowId);
            for (Cell cell : cellSet) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                get.addColumn(family, qualifier);
            }
            // get sandbox
            Result sandboxResult = sandboxTable.table.get(enrichGet(get));
            Result originalResult = null;

            CellSet sandboxNonExistentCells = new CellSet();
            CellSet sandboxMarkedAsDeletedCells = new CellSet();

            for (Cell cell : cellSet) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                // sandbox cell value = empty and not marked for deletion
                boolean hasDeletionMark = SandboxTableUtils.hasDeletionMarkForColumn(sandboxResult, family, qualifier);

                if (!hasDeletionMark &&
                        !SandboxTableUtils.hasValueForColumn(sandboxResult, family, qualifier)) {
                    sandboxNonExistentCells.add(cell);
                }

                if (hasDeletionMark) {
                    sandboxMarkedAsDeletedCells.add(cell);
                }
            }


            // fetch cells existing in original and not existing (or marked as deleted) in the sandbox
            CellSet cellsExistingOnlyInOriginal = new CellSet();
            if (sandboxNonExistentCells.size() > 0) {
                // TODO narrow Get
                // fetch non existent cells values from original table
                originalResult = sandboxTable.originalTable.get(get);

                // if they exist in the original, keep track of them to copy them later to dirty CF for manipulation
                if (originalResult != null) {
                    for (Cell cell : sandboxNonExistentCells) {
                        byte[] family = CellUtil.cloneFamily(cell);
                        byte[] qualifier = CellUtil.cloneQualifier(cell);

                        if (originalResult.containsColumn(family, qualifier)) {
                            cellsExistingOnlyInOriginal.add(cell);
                        }
                    }
                }
            }

            if (cellsExistingOnlyInOriginal.size() == 0) {
                // increment normally
                result = sandboxTable.table.increment(increment);
            } else {
                // initialize adapted increment definition
                final Increment adaptedIncr = new Increment(rowId);

                // copy cells from original to dirty columns
                final Put putOnDirty = new Put(rowId);

                for (Cell cell : cellsExistingOnlyInOriginal) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtil.cloneQualifier(cell);

                    byte[] dirtyQualififer = new byte[transactionId.length + family.length + qualifier.length];
                    System.arraycopy(transactionId, 0, dirtyQualififer, 0, transactionId.length);
                    System.arraycopy(family, 0, dirtyQualififer, transactionId.length, family.length);
                    System.arraycopy(qualifier, 0, dirtyQualififer, transactionId.length + family.length, qualifier.length);

                    Cell originalResultCell = originalResult.getColumnLatestCell(family, qualifier);

                    putOnDirty.add(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualififer
                            , originalResultCell.getTimestamp(), CellUtil.cloneValue(originalResultCell));

                    // add the dirty column to the adapted increment
                    long amount = increment.getFamilyMapOfLongs().get(family).get(qualifier).longValue();
                    adaptedIncr.addColumn(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualififer, amount);

                    cellToDirtyColumnQualifMap.put(ByteBuffer.wrap(dirtyQualififer), cell);
                }
                sandboxTable.table.put(putOnDirty);
                sandboxTable.table.flushCommits();

                // apply transformed increment
                result = sandboxTable.table.increment(adaptedIncr);

                // copy results in dirty columns to sandboxes ones
                Put putResults = new Put(rowId);
                for (Cell cell : result.rawCells()) {
                    ByteBuffer dirtyQualifierKey = ByteBuffer.wrap(CellUtil.cloneQualifier(cell));
                    Cell originalCell = cellToDirtyColumnQualifMap.get(dirtyQualifierKey);

                    if (originalCell != null) {
                        byte[] family = CellUtil.cloneFamily(originalCell);
                        byte[] qualifier = CellUtil.cloneQualifier(originalCell);
                        byte[] value = CellUtil.cloneValue(originalCell);

                        putResults.add(family, qualifier, value);
                    }
                }
                sandboxTable.table.put(putResults);
                sandboxTable.table.flushCommits();

                // TODO returned result assertion
            }

            // delete deletion marks
            if (sandboxMarkedAsDeletedCells.size() > 0) {
                Delete delete = removeDeletionMark(rowId, sandboxMarkedAsDeletedCells);
                sandboxTable.table.delete(delete);
            }
        } finally {
            // delete any dirty column
            if (cellToDirtyColumnQualifMap.size() > 0) {
                Delete deleteDirty = new Delete(rowId);

                for (ByteBuffer dirtyQualifKey : cellToDirtyColumnQualifMap.keySet()) {
                    deleteDirty.deleteColumns(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifKey.array());
                }
                sandboxTable.table.delete(deleteDirty);
            }

            // release lock
            releaseLock(sandboxTable, rowId, transactionId);
        }

        return result;
    }

    private static boolean acquireLock(SandboxTable sandboxTable, byte[] rowId, byte[] transactionId) throws IOException {
        Put put = new Put(rowId);
        put.add(SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL, transactionId);
        try {
            return sandboxTable .table.checkAndPut(rowId,
                    SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL, null, put);
        } catch (IOException e) {
            throw new IOException(String.format("Could not acquire sandbox row lock for row = %s", Bytes.toString(rowId)), e);
        }
    }

    private static boolean releaseLock(SandboxTable sandboxTable, byte[] rowId, byte[] transactionId) throws IOException {
        Delete deleteLock = new Delete(rowId);
        deleteLock.deleteColumns(SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL);
        try {
            return sandboxTable .table.checkAndDelete(rowId,
                    SandboxTable.DEFAULT_DIRTY_CF, SandboxTable.DEFAULT_TID_COL, transactionId, deleteLock);
        } catch (IOException e) {
            throw new IOException(String.format("Could not acquire sandbox row lock for row = %s", Bytes.toString(rowId)), e);
        }

    }

    //    public static long incrementColumnValue(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
    //        final Get get = new Get(row);
    //        get.addColumn(family, qualifier);
    //        Result shadowResult = sandboxTable.table.get(enrichGet(get));
    //        Result originalResult = null;
    //
    //        // iter family and qualifier
    //        if (!SandboxTableUtils.hasDeletionMarkForColumn(shadowResult, family, qualifier) &&
    //                !SandboxTableUtils.hasValueForColumn(shadowResult, family, qualifier)) {
    //
    //            // fetch only if needed
    //            if (originalResult == null) {
    //                originalResult = sandboxTable.originalTable.get(get);
    //            }
    //
    //            // fill the sandbox with original's value first
    //            if (originalResult.containsColumn(family, qualifier)) {
    //                // read as long
    //                long currentValue = Bytes.toLong(originalResult.getValue(family, qualifier));
    //                long finalValue = currentValue + amount;
    //
    //                try {
    //                    sandboxTable.checkAndPut(r, f, q, null, put));
    //                } catch (DoNotRetryIOException ex) {
    //                    exceptThrown = true;
    //                }
    //
    //                Put fillPut = new Put(row);
    //                fillPut.add(family, qualifier, originalResult.getValue(family, qualifier));
    //                sandboxTable.table.put(fillPut);
    //                sandboxTable.table.flushCommits();
    //            }
    //        }
    //    }
    
    public static boolean checkAndPut(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);
        Result shadowResult = sandboxTable.table.get(enrichGet(get));

        // case the value exists in original but not on sandbox (nor was deleted in sandbox)
        if (!SandboxTableUtils.hasDeletionMarkForColumn(shadowResult, family, qualifier) &&
                !SandboxTableUtils.hasValueForColumn(shadowResult, family, qualifier)) {
            Result originalResult = sandboxTable.originalTable.get(get);

            // fill the sandbox with original's value first
            if (originalResult.containsColumn(family, qualifier)) {
                // TODO this put might polute the sandbox state and break stuff, so please adjust timestamp
                Put fillPut = new Put(row);
                fillPut.add(family, qualifier, originalResult.getValue(family, qualifier));
                sandboxTable.table.put(fillPut);
                sandboxTable.table.flushCommits();
            }
        }

        boolean opSuccess = sandboxTable.table.checkAndPut(row, family, qualifier, value, put);

        if (opSuccess) {
            Delete delete = removeDeletionMarkForPut(put);
            sandboxTable.table.delete(delete);
        }

        return opSuccess;
    }

    public static boolean checkAndDelete(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);
        Result shadowResult = sandboxTable.table.get(enrichGet(get));

        // case the value exists in original but not on sandbox (nor was deleted in sandbox)
        if (!SandboxTableUtils.hasDeletionMarkForColumn(shadowResult, family, qualifier) &&
                !SandboxTableUtils.hasValueForColumn(shadowResult, family, qualifier)) {
            Result originalResult = sandboxTable.originalTable.get(get);

            // fill the sandbox with original's value first
            if (originalResult.containsColumn(family, qualifier)) {
                // TODO this put might polute the sandbox state and break stuff, so please adjust timestamp
                Put fillPut = new Put(row);
                fillPut.add(family, qualifier, originalResult.getValue(family, qualifier));
                sandboxTable.table.put(fillPut);
                sandboxTable.table.flushCommits();
            }
        }

        boolean opSuccess = sandboxTable.table.checkAndDelete(row, family, qualifier, value, delete);

        if (opSuccess) {
            // TODO do smth or delete for other impl
            //markAsDeleted(sandboxTable, delete);
        }

        return opSuccess;
    }

    public static boolean checkAndMutate(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations rm) {
        // TODO
        return false;
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
