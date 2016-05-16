package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public abstract class AtomicSandboxCheckAndXOp extends AtomicSandboxOp {
    private final byte[] transactionId;
    private final CellSet cellSet;
    boolean result;

    public AtomicSandboxCheckAndXOp(SandboxTable sandboxTable, byte[] rowId, final byte[] family, final byte[] qualifier) {
        super(sandboxTable, rowId);

        this.cellSet = new CellSet();
        cellSet.add(rowId, family, qualifier);
        transactionId = SandboxTableUtils.generateTransactionId(rowId, family, qualifier);
    }

    protected abstract boolean runCheckAndXOpOnSandbox() throws IOException;
    protected abstract boolean runCheckAndXOpOnDirty(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException;

    @Override
    protected byte[] getTransactionId() {
        return transactionId;
    }

    @Override
    protected CellSet getOpCells() {
        return cellSet;
    }

    @Override
    protected void runOpOnSandbox() throws IOException {
        result = runCheckAndXOpOnSandbox();
    }

    @Override
    protected void runOnDirtyColumns(Map<Cell, ByteBuffer> originalCellToDirtyQualifMap) throws IOException {
        // Run op specific logic on dirty columns
        result = runCheckAndXOpOnDirty(originalCellToDirtyQualifMap);
    }

    public boolean runOp() throws IOException {
        super.run();
        return result;
    }
}
