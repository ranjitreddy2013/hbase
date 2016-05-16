package com.mapr.db.sandbox;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTableUtils.*;

public abstract class AtomicSandboxOperation {
    static final int MAX_TRIES = 300;
    final byte[] rowId;
    final SandboxTable sandboxTable;
    final Mutation mutation;

    public AtomicSandboxOperation(SandboxTable sandboxTable, byte[] rowId, Mutation mutation) {
        this.rowId = rowId;
        this.sandboxTable = sandboxTable;
        this.mutation = mutation;
    }


    protected abstract Result runOpOnSandbox() throws IOException;

    protected abstract Result runOpWithOrigValues(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException;

    public Result run() throws IOException {
        final byte[] transactionId = generateTransactionId(this.mutation);
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

        // empty unless columns are copied to dirty CF. format
        // Cell(family,qualifier) => dirtyColumnQualif to be used (for mutation or comparison on checkAndX op
        Map<Cell, ByteBuffer> originalCellToDirtyQualifMap = Maps.newTreeMap(CellSet.CELL_FAM_COL_COMPARATOR);

        // lock acquired
        try {
            CellSet cellSet = new CellSet(mutation.getFamilyCellMap());

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
                result = runOpOnSandbox();
            } else {
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

                    originalCellToDirtyQualifMap.put(cell, ByteBuffer.wrap(dirtyQualififer));
                }
                sandboxTable.table.put(putOnDirty);
                sandboxTable.table.flushCommits();

                // TODO give also access to other cells (from sandbox)
                // Run mutation specific logic on dirty columns
                result = runOpWithOrigValues(originalCellToDirtyQualifMap);

                // if the operation returns a result (increment or append)
                if (result != null) {
                    // copy results from dirty columns to sandbox
                    Put putResults = new Put(rowId);

                    for (Map.Entry<Cell, ByteBuffer> cellDirtyQualifEntry : originalCellToDirtyQualifMap.entrySet()) {
                        byte[] dirtyQualifier = cellDirtyQualifEntry.getValue().array();
                        byte[] value = result.getValue(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier);

                        // retrieve original family and qualififer
                        Cell cell = cellDirtyQualifEntry.getKey();
                        byte[] family = CellUtil.cloneFamily(cell);
                        byte[] qualifier = CellUtil.cloneQualifier(cell);

                        putResults.add(family, qualifier, value);
                    }
                    sandboxTable.table.put(putResults);
                    sandboxTable.table.flushCommits();

                    // TODO returned result assertion
                }
            }

            // delete deletion marks
            if (sandboxMarkedAsDeletedCells.size() > 0) {
                Delete delete = removeDeletionMark(rowId, sandboxMarkedAsDeletedCells);
                sandboxTable.table.delete(delete);
            }
        } finally {
            // delete any dirty column
            if (originalCellToDirtyQualifMap.size() > 0) {
                Delete deleteDirty = new Delete(rowId);

                for (ByteBuffer dirtyQualifKey : originalCellToDirtyQualifMap.values()) {
                    deleteDirty.deleteColumns(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifKey.array());
                }
                sandboxTable.table.delete(deleteDirty);
            }

            // release lock
            releaseLock(sandboxTable, rowId, transactionId);
        }

        return result;
    }

    // TODO consider pass these to utils
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
}
