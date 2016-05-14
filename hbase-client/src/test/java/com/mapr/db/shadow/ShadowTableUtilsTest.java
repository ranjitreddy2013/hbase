package com.mapr.db.shadow;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ShadowTableUtilsTest {
    AbstractHTable shadowTableTable;
    AbstractHTable originalTable;
    ShadowTable shadowTable;

    @Before
    public void setup() {
        shadowTableTable = mock(AbstractHTable.class);
        originalTable = mock(AbstractHTable.class);
        shadowTable = new ShadowTable(shadowTableTable, originalTable);
    }

//    @Test
//    public void testOriginalTablePathFor() throws Exception {
//        AbstractHTable shadowTable = mock(AbstractHTable.class);
//
//        final String tablePath = "/this/is/a/table";
//        when(shadowTable.getTableName()).thenReturn(tablePath.getBytes());
//        assertEquals(ShadowTableUtils.originalTablePathFor(shadowTable), tablePath);
//    }


    @Test
    public void testGet() throws Exception {
        Get get = mock(Get.class);

        Result originalResult = mock(Result.class);
        when(originalTable.get(any(Get.class))).thenReturn(originalResult);
        when(shadowTableTable.get(any(Get.class))).thenReturn(Result.EMPTY_RESULT);

        Result result = ShadowTableUtils.get(shadowTable, get);

        // both tables are called
        verify(shadowTableTable).get(get);
        verify(originalTable).get(get);

        // result is equal to the original result if shadowTable is non existent
        assertEquals(originalResult, result);
    }

    @Test
    public void testEnrichedGet() throws Exception {
        AbstractHTable shadowTableTable = mock(AbstractHTable.class);
        AbstractHTable originalTable = mock(AbstractHTable.class);
        ShadowTable shadowTable = new ShadowTable(shadowTableTable, originalTable);

        Get get = mock(Get.class);
        when(get.hasFamilies()).thenReturn(false);
        Result result = ShadowTableUtils.get(shadowTable, get);
        verify(get).hasFamilies();
        verify(get, never()).addFamily(any(byte[].class));

        Get get2 = mock(Get.class);
        when(get2.hasFamilies()).thenReturn(true);
        result = ShadowTableUtils.get(shadowTable, get2);
        verify(get2).hasFamilies();
        verify(get2).addFamily(eq(ShadowTableUtils.DEFAULT_META_CF));
    }

    @Test
    public void testCellComparator() throws Exception {
        final byte[] rowKey = "someKey".getBytes();
        final byte[] qual1 = "qual1".getBytes();
        final byte[] qual2 = "qual2".getBytes();
        final byte[] col1 = "col1".getBytes();
        final byte[] col2 = "col2".getBytes();

        Cell cellA, cellB;

        // equals
        cellA = new KeyValue(rowKey, qual1, col1);
        cellB = new KeyValue(rowKey, qual1, col1);
        assertEquals(ShadowTableUtils.SAME_ROW_CELL_COMPARATOR.compare(cellA, cellB), 0);

        // equals
        cellA = new KeyValue(rowKey, qual1, col1);
        cellB = new KeyValue(rowKey, qual1, col2);
        assertEquals(ShadowTableUtils.SAME_ROW_CELL_COMPARATOR.compare(cellA, cellB), -1);
        assertEquals(ShadowTableUtils.SAME_ROW_CELL_COMPARATOR.compare(cellB, cellA), 1);

        // equals
        cellA = new KeyValue(rowKey, qual1, col1);
        cellB = new KeyValue(rowKey, qual2, col1);
        assertEquals(ShadowTableUtils.SAME_ROW_CELL_COMPARATOR.compare(cellA, cellB), -1);
        assertEquals(ShadowTableUtils.SAME_ROW_CELL_COMPARATOR.compare(cellB, cellA), 1);
    }

    @Test
    public void testMergeEmptyResults() throws Exception {
        Result shadowResult = Result.create(new Cell[] {});
        Result originalResult = Result.create(new Cell[] {});

        Result result = ShadowTableUtils.mergeResult(shadowResult, originalResult);
        assertEquals(Result.EMPTY_RESULT.listCells(), result.listCells());
    }

    @Test
    public void testMergeOneEmptyResult() throws Exception {
        byte[] rowKey = "rowKey".getBytes();
        Cell cellA = new KeyValue(rowKey, "f".getBytes(), "q".getBytes(), "v".getBytes());

        Result shadowResult = Result.create(new Cell[] { cellA });
        Result originalResult = Result.create(new Cell[] {});

        Result result = ShadowTableUtils.mergeResult(shadowResult, originalResult);
        assertEquals(Lists.newArrayList(cellA), result.listCells());

        result = ShadowTableUtils.mergeResult(originalResult, shadowResult);
        assertEquals(Lists.newArrayList(cellA), result.listCells());

        result = ShadowTableUtils.mergeResult(shadowResult, null);
        assertEquals(Lists.newArrayList(cellA), result.listCells());
    }

    @Test
    public void testMergeSameResult() throws Exception {
        byte[] rowKey = "rowKey".getBytes();
        Cell cellA = new KeyValue(rowKey, "f".getBytes(), "q".getBytes(), "v".getBytes());
        Cell cellB = new KeyValue(rowKey, "f".getBytes(), "q".getBytes(), "v2".getBytes());

        Result shadowResult = Result.create(new Cell[] { cellA });
        Result originalResult = Result.create(new Cell[] { cellB });

        Result result = ShadowTableUtils.mergeResult(shadowResult, originalResult);
        assertEquals(Lists.newArrayList(cellA), result.listCells());
    }

    @Test
    public void testMergeDeletedColResult() throws Exception {
        byte[] rowKey = "rowKey".getBytes();
        Cell cellA = new KeyValue(rowKey, "f".getBytes(), "q".getBytes(), "v2".getBytes());
        Cell cellB = new KeyValue(rowKey, "f".getBytes(), "q".getBytes(), "v0".getBytes());
        Cell cellAMarkedAsDeleted = new KeyValue(rowKey, ShadowTableUtils.DEFAULT_META_CF, "f:q".getBytes(), "1".getBytes());

        Result shadowResult = Result.create(new Cell[] { cellA, cellAMarkedAsDeleted });
        Result originalResult = Result.create(new Cell[] { cellB });

        Result result = ShadowTableUtils.mergeResult(shadowResult, originalResult);
        assertEquals(Result.EMPTY_RESULT.listCells(), result.listCells());

        shadowResult = Result.create(new Cell[] { cellA, cellAMarkedAsDeleted });
        originalResult = Result.create(new Cell[] { });

        result = ShadowTableUtils.mergeResult(shadowResult, originalResult);
        assertEquals(Result.EMPTY_RESULT.listCells(), result.listCells());
    }

    @Test
    public void testScanWithFamilies() throws Exception {
        Scan scan = new Scan("startRow".getBytes());
        scan.addFamily("family".getBytes());

        ResultScanner origResultScanner = mockResultScanner();
        ResultScanner shadowResultScanner = mockResultScanner();
        when(originalTable.getScanner(eq(scan))).thenReturn(origResultScanner);
        when(shadowTableTable.getScanner(any(Scan.class))).thenReturn(shadowResultScanner);

        ResultScanner result = ShadowTableUtils.getScanner(shadowTable, scan);

        // verify scanners are retrieved from both
        verify(originalTable).getScanner(eq(scan));
        verify(shadowTableTable).getScanner(any(Scan.class));
        assertEquals(result.getClass(), MergedResultScanner.class);
    }

    @Test
    public void testScanAllCols() throws Exception {
        Scan scan = new Scan("startRow".getBytes());
        ResultScanner origResultScanner = mockResultScanner();
        ResultScanner shadowResultScanner = mockResultScanner();
        when(originalTable.getScanner(eq(scan))).thenReturn(origResultScanner);
        when(shadowTableTable.getScanner(any(Scan.class))).thenReturn(shadowResultScanner);

        ResultScanner result = ShadowTableUtils.getScanner(shadowTable, scan);

        // verify scanners are retrieved from both
        verify(originalTable).getScanner(eq(scan));
        verify(shadowTableTable).getScanner(any(Scan.class));
        assertEquals(result.getClass(), MergedResultScanner.class);
    }

    private ResultScanner mockResultScanner() {
        ResultScanner rs = mock(ResultScanner.class);
        when(rs.iterator()).thenReturn(Iterators.<Result>emptyIterator());
        return rs;
    }

    @Test
    public void testDeleteWithColumn() throws Exception {
        final byte[] rowKey = "rowKey".getBytes();
        final byte[] familyQualifier = "familyQualifier".getBytes();
        final byte[] column = "column".getBytes();

        // delete family:column
        Delete delete = new Delete(rowKey);
        delete.deleteColumn(familyQualifier, column);

        ShadowTableUtils.delete(shadowTable, delete);

        // verify the delete is called on the shadow table
        verify(shadowTableTable).delete(eq(delete));

        // verify column is marked for deletion
        verify(shadowTableTable).put(any(Put.class));
    }

    @Test
    public void testDeleteWholeFamily() throws Exception {
        final byte[] rowKey = "rowKey".getBytes();
        final byte[] familyQualifier = "familyQualifier".getBytes();

        Cell cell1 = new KeyValue(rowKey, familyQualifier, "col1".getBytes(), "val".getBytes());
        Cell cell2 = new KeyValue(rowKey, familyQualifier, "col2".getBytes(), "val".getBytes());

        Result mockedResult = mock(Result.class);
        when(originalTable.get(any(Get.class))).thenReturn(mockedResult);
        when(mockedResult.listCells()).thenReturn(Lists.newArrayList(cell1,cell2));

        // delete family:column
        Delete delete = new Delete(rowKey);
        delete.deleteFamily(familyQualifier);


        ShadowTableUtils.delete(shadowTable, delete);

        // verify the delete is called on the shadow table
        verify(shadowTableTable).delete(eq(delete));

        // verify metadata is fetched from original table
        Get get = new Get(rowKey);
        get.addFamily(familyQualifier);
        verify(originalTable).get(eq(get));

        // verify column is marked for deletion
        verify(shadowTableTable).put(any(Put.class));
    }

    // TODO whole row


    @Test
    public void testPut() throws Exception {
        final byte[] rowKey = "rowKey".getBytes();
        final byte[] familyQualifier = "f".getBytes();
        final byte[] column = "c".getBytes();

        Put put = new Put(rowKey);
        put.add(familyQualifier, column, "someValue".getBytes());

        ShadowTableUtils.put(shadowTable, put);

        // verify the markedAsDeleted is always removed
        verify(shadowTableTable).delete(any(Delete.class));
        verify(shadowTableTable).put(eq(put));
    }
}