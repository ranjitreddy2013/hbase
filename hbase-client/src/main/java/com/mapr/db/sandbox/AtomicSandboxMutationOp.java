package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AtomicSandboxMutationOp extends AtomicSandboxOp {

    private final Mutation mutation;
    private final byte[] transactionId;

    private Result result;

    public AtomicSandboxMutationOp(SandboxTable sandboxTable, byte[] rowId, Mutation mutation) {
        super(sandboxTable, rowId);
        this.mutation = mutation;
        this.transactionId = SandboxTableUtils.generateTransactionId(this.mutation);
    }

    protected abstract Result runMutationOnSandbox() throws IOException;
    protected abstract Result runMutationOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException;

    @Override
    protected byte[] getTransactionId() {
        return transactionId;
    }

    @Override
    protected CellSet getOpCells() {
        return new CellSet(mutation.getFamilyCellMap());
    }

    @Override
    protected void runOpOnSandbox() throws IOException {
        result = runMutationOnSandbox();
        removeDeletionMarksIfNeeded();
    }

    @Override
    protected void runOnDirtyColumns(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
        // Run mutation specific logic on dirty columns
        result = runMutationOnDirty(originalCellToDirtyQualifMap);

        // if the operation returns a result (increment or append)
        if (result != null) {
            // copy results from dirty columns to sandbox
            Put putResults = new Put(rowId);

            for (Map.Entry<Cell, ByteBuffer> cellDirtyQualifEntry : originalCellToDirtyQualifMap.entrySet()) {
                byte[] dirtyQualifier = cellDirtyQualifEntry.getValue().array();
                byte[] value = result.getValue(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier);

                // retrieve original family and qualifier
                Cell cell = cellDirtyQualifEntry.getKey();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                putResults.add(family, qualifier, value);
            }
            sandboxTable.table.put(putResults);
            sandboxTable.table.flushCommits();

            removeDeletionMarksIfNeeded();


            // TODO returned result assertion


        }
    }

    private void removeDeletionMarksIfNeeded() throws IOException {
        // remove deletion marks if they exist (for increment and appends mutations)
        if (sandboxMarkedAsDeletedCells.size() > 0) {
            Delete delete = removeDeletionMark(rowId, sandboxMarkedAsDeletedCells);
            sandboxTable.table.delete(delete);
        }
    }

    public Result runMutation() throws IOException {
        super.run();
        return result;
    }
}
