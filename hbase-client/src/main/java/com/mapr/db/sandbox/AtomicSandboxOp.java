package com.mapr.db.sandbox;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;
import static com.mapr.db.sandbox.SandboxTableUtils.buildAnnotatedColumn;
import static com.mapr.db.sandbox.SandboxTableUtils.enrichGet;

public abstract class AtomicSandboxOp {
    static final int MAX_TRIES = 300;
    final byte[] rowId;
    final SandboxTable sandboxTable;
    CellSet sandboxMarkedAsDeletedCells = new CellSet();

    public AtomicSandboxOp(SandboxTable sandboxTable, byte[] rowId) {
        this.rowId = rowId;
        this.sandboxTable = sandboxTable;
    }

    protected abstract byte[] getTransactionId();

    protected abstract CellSet getOpCells();

    protected abstract void runOpOnSandbox() throws IOException;

    protected abstract void runOnDirtyColumns(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException;

    public void run() throws IOException {
        final byte[] transactionId = getTransactionId();

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
            CellSet cellSet = getOpCells();

            final Get get = tableGetForCells(rowId, cellSet);

            // get sandbox version
            Result sandboxResult = sandboxTable.table.get(enrichGet(get));

            CellSet sandboxNonExistentCells = new CellSet();

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
            Result originalResult = null;
            CellSet cellsExistingOnlyInOriginal = new CellSet();
            if (sandboxNonExistentCells.size() > 0) {
                // fetch non existent cells values from original table
                Get nonExistCellGet = tableGetForCells(rowId, sandboxNonExistentCells);
                originalResult = sandboxTable.originalTable.get(nonExistCellGet);

                // if they exist in the original, keep track of them to copy them later to dirty CF for manipulation
                if (originalResult != null && !originalResult.isEmpty()) {
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
                runOpOnSandbox();
            } else if (originalResult != null) {
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

                    if (originalResultCell != null) {
                        putOnDirty.add(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualififer
                                , originalResultCell.getTimestamp(), CellUtil.cloneValue(originalResultCell));

                        originalCellToDirtyQualifMap.put(cell, ByteBuffer.wrap(dirtyQualififer));
                    }
                }
                sandboxTable.table.put(putOnDirty);
                sandboxTable.table.flushCommits();

                runOnDirtyColumns(originalCellToDirtyQualifMap);
            }

            // TODO this deletion loop won't work on checkAndX
            // delete deletion marks
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
    }

    private static Get tableGetForCells(byte[] rowId, CellSet cellSet) {
        final Get get = new Get(rowId);
        for (Cell cell : cellSet) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            get.addColumn(family, qualifier);
        }
        return get;
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
