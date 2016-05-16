package com.mapr.db.sandbox;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTableUtils.buildAnnotatedColumn;

public class SandboxHTable {
    private static final Log LOG = LogFactory.getLog(SandboxHTable.class);

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
        Result originalResult = sandboxTable.originalTable.get(get);
        Result shadowResult = sandboxTable.table.get(enrichGet(get));

        // if result exists on sandbox...
        if (shadowResult != null && !shadowResult.isEmpty()) {
            return mergeResult(shadowResult, originalResult);
        }

        return originalResult;
    }


    public static Result[] get(SandboxTable sandboxTable, List<Get> gets) throws IOException {
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

                final byte[] annotatedColumn = CellUtil.cloneQualifier(cell);
                String annotatedColumnStr = Bytes.toStringBinary(annotatedColumn);
                final String separator = Bytes.toStringBinary(SandboxTableUtils.FAMILY_QUALIFIER_SEPARATOR);

                int sepIndex = annotatedColumnStr.indexOf(separator);

                if (sepIndex == -1) {
                    continue; // bad representation? TODO log
                }

                try {
                    byte[] family = Arrays.copyOfRange(annotatedColumn, 0, sepIndex);
                    byte[] qualif = Arrays.copyOfRange(annotatedColumn, sepIndex + separator.length(), annotatedColumn.length);

                    Cell deletedCell = new KeyValue(row, family, qualif);
                    markedForDeletion.add(deletedCell);
                } catch (ArrayIndexOutOfBoundsException ex) {
                    // doesn't matter... it's a parsing error on the metadata notation
                    // TODO log
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
        List<Put> puts = FluentIterable.from(deletes).transform(new Function<Delete, Put>() {
            @Nullable
            @Override
            public Put apply(@Nullable Delete delete) {
                try {
                    return SandboxTableUtils.markForDeletionPut(sandboxTable, delete);
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                    return null;
                }
            }
        }).toImmutableList();

        sandboxTable.table.delete(deletes);
        sandboxTable.table.put(puts);
        sandboxTable.table.flushCommits();
    }

    public static void delete(SandboxTable sandboxTable, Delete delete) throws IOException {
        sandboxTable.table.delete(delete);
        markAsDeleted(sandboxTable, delete);
    }

    private static void markAsDeleted(SandboxTable sandboxTable, Delete delete) throws IOException {
        Put markDeletionPut = SandboxTableUtils.markForDeletionPut(sandboxTable, delete);
        sandboxTable.table.put(markDeletionPut);
        sandboxTable.table.flushCommits();
    }

    /**
     * When performing a PUT in the shadow table, it makes sure any delete flag for the column
     * is removed.
     *
     * @param sandboxTable
     * @param put
     */
    public static void put(SandboxTable sandboxTable, Put put) throws InterruptedIOException {
        Delete delete = removeDeletionMarkForPut(put);

        // is there a better way than this?
        try {
            sandboxTable.table.delete(delete);
            sandboxTable.table.put(put);
        } catch (IOException e) {
            throw new InterruptedIOException(e.toString());
        }
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
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualif = CellUtil.cloneQualifier(cell);

                byte[] annotatedColumn = buildAnnotatedColumn(family, qualif);
                delete.deleteColumn(DEFAULT_META_CF, annotatedColumn);
            }
        }

        return delete;
    }

    public static void put(SandboxTable sandboxTable, final List<Put> puts) throws InterruptedIOException {
        List<Delete> deletes = FluentIterable.from(puts)
                .transform(new Function<Put, Delete>() {
                    @Nullable
                    @Override
                    public Delete apply(@Nullable Put put) {
                        return removeDeletionMarkForPut(put);
                    }
                }).toImmutableList();

        try {
            sandboxTable.table.delete(deletes);
            sandboxTable.table.put(puts);
        } catch (IOException ex) {
            LOG.error("Error on batch PUT", ex);
            throw new InterruptedIOException(ex.getMessage());
        }
    }

    public static Result getRowOrBefore(SandboxTable sandboxTable, byte[] row, byte[] family) throws IOException {
        throw new UnsupportedOperationException("This is not supported for MapR table");
    }

    public static void mutateRow(SandboxTable sandboxTable, RowMutations rm) throws IOException {
        RowMutations finalRm = new RowMutations(rm.getRow());

        for (Mutation mutation : rm.getMutations()) {
            Class clz = mutation.getClass();
            // if it is a delete, add the put to metadata table
            if (clz.equals(Delete.class)) {
                Delete delete = (Delete) mutation;
                Put markForDeletionPut = SandboxTableUtils.markForDeletionPut(sandboxTable, delete);
                finalRm.add(delete);
                finalRm.add(markForDeletionPut);
            }  else if (clz.equals(Put.class)) {
                // if it is a PUT, make sure there is no delete there
                Put put = (Put) mutation;
                Delete deletionMarkDelete = removeDeletionMarkForPut(put);
                finalRm.add(put);
                finalRm.add(deletionMarkDelete);
            }
        }

        sandboxTable.table.mutateRow(finalRm);
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
        byte[] rowId = increment.getRow();
        // fetch which columns are going to be appended
        NavigableMap<byte[], List<Cell>> familyCellMap = increment.getFamilyCellMap();

        RowMutations rowMutations = new RowMutations(rowId);

        // fetch the column versions from both sides
        Get get = new Get(rowId);
        for (byte[] family : familyCellMap.keySet()) {
            List<Cell> cellToIncr = familyCellMap.get(family);

            for (Cell cell : cellToIncr) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                get.addColumn(family, qualifier);
            }
        }

        // fetch merged result
        Result result = get(sandboxTable, get);

        for (byte[] family : familyCellMap.keySet()) {
            List<Cell> cellToIncr = familyCellMap.get(family);

            for (Cell cell : cellToIncr) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                // get cell from merged result
                Cell lastVersionCell = result.getColumnLatestCell(family, qualifier);

                byte[] existingValue = new byte[0];
                if (lastVersionCell != null) {
                    existingValue = CellUtil.cloneValue(lastVersionCell);
                }
                byte[] appendValue = CellUtil.cloneValue(cell);

                byte[] resultValue = new byte[existingValue.length + appendValue.length];
                System.arraycopy(existingValue, 0, resultValue, 0, existingValue.length);
                System.arraycopy(appendValue, 0, resultValue, existingValue.length, appendValue.length);

                Put put = new Put(rowId);
                put.add(family, qualifier, resultValue);
                rowMutations.add(put);
            }
        }

        mutateRow(sandboxTable, rowMutations);
        return get(sandboxTable, get);
    }

	public static long incrementColumnValue(SandboxTable sandboxTable,
			byte[] row, byte[] family, byte[] qualifier, long amount,
			Durability durability) throws IOException {
		Increment increment = new Increment(row);
		increment.addColumn(family, qualifier, amount);
		increment.setDurability(durability);

		Result result = increment(sandboxTable, increment);
		byte[] value = CellUtil.cloneValue(result.getColumnLatestCell(family,
				qualifier));
		return Bytes.toLong(value);

	}

    public static boolean checkAndPut(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);
        Result shadowResult = sandboxTable.table.get(enrichGet(get));

        if (!SandboxTableUtils.hasValueForColumn(shadowResult, family, qualifier)) {
            Result originalResult = sandboxTable.originalTable.get(get);

            // fill the sandbox with original's value first
            if (originalResult.containsColumn(family, qualifier)) {
                Put fillPut = new Put(row);
                fillPut.add(family, qualifier, originalResult.getValue(family, qualifier));
                put(sandboxTable, fillPut);
                sandboxTable.table.flushCommits();
            }
        }

        return sandboxTable.table.checkAndPut(row, family, qualifier, value, put);
    }

    public static boolean checkAndDelete(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) {
        // TODO
        return false;
    }

    public static boolean checkAndMutate(SandboxTable sandboxTable, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations rm) {
        // TODO
        return false;
    }

    public static boolean exists(SandboxTable sandboxTable, Get get) throws IOException {
        Result result = get(sandboxTable, get);
        return result != null && !result.isEmpty();
    }

    public static Boolean[] exists(SandboxTable sandboxTable, final List<Get> gets) throws IOException {
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
