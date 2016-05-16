package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
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

            List<Cell> finalResultCells = Lists.newLinkedList();

            for (Map.Entry<Cell, ByteBuffer> cellDirtyQualifEntry : originalCellToDirtyQualifMap.entrySet()) {
                // retrieve original family and qualifier
                Cell cell = cellDirtyQualifEntry.getKey();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                // get corresponding dirty qualifier
                byte[] dirtyQualifier = cellDirtyQualifEntry.getValue().array();

                // get result
                Cell resultCell = result.getColumnLatestCell(SandboxTable.DEFAULT_DIRTY_CF, dirtyQualifier);

                byte[] value = CellUtil.cloneValue(resultCell);

                // create result cell with original family and qualifier
                KeyValue kv = new KeyValue(result.getRow(), family, qualifier,
                        resultCell.getTimestamp(), KeyValue.Type.codeToType(resultCell.getTypeByte()),
                        value);
                finalResultCells.add(0, kv);

                putResults.add(family, qualifier, value);
            }
            sandboxTable.table.put(putResults);
            sandboxTable.table.flushCommits();

            removeDeletionMarksIfNeeded();


            // copy non dirty cells from result to final list
            for (Cell resultCell : result.rawCells()) {
                byte[] family = CellUtil.cloneFamily(resultCell);

                if (!Arrays.equals(SandboxTable.DEFAULT_DIRTY_CF, family)) {
                    finalResultCells.add(0, resultCell);
                }
            }

            result = Result.create(finalResultCells);

            System.out.println();
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
