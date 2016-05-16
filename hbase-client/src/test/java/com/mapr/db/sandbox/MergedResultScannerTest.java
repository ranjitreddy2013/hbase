package com.mapr.db.sandbox;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.mapr.db.sandbox.MergedResultScanner.AnnotatedResult;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergedResultScannerTest {
    ResultScanner mShadow;
    ResultScanner mOriginal;
    Scan scan;

    @Before
    public void setup() {
        mShadow = mock(ResultScanner.class);
        mOriginal = mock(ResultScanner.class);
        scan = mock(Scan.class);
    }

    @Test
    public void testEmptyMerge() throws IOException {
        when(mShadow.iterator()).thenReturn(Iterators.<Result>emptyIterator());
        when(mOriginal.iterator()).thenReturn(Iterators.<Result>emptyIterator());

        MergedResultScanner victim = new MergedResultScanner(mShadow, mOriginal, scan);
        assertFalse(victim.iterator().hasNext());
        assertNull(victim.next());
    }

    @Test
    public void testAnnotatedResultComparator() {
        final byte[] rowKey1 = "aaaa".getBytes();
        final byte[] rowKey2 = "aaab".getBytes();
        final byte[] family = "family".getBytes();
        final byte[] col = "col".getBytes();
        final byte[] value = "value".getBytes();

        Result resultA, resultB;

        resultA = Result.create(Lists.<Cell>newArrayList(new KeyValue(rowKey1, family, col, value)));
        resultB = Result.create(Lists.<Cell>newArrayList(new KeyValue(rowKey2, family, col, value)));

        // diff rows
        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultA, MergedResultScanner.ORIGINAL_RESULT_LABEL),
                new AnnotatedResult(resultB, MergedResultScanner.ORIGINAL_RESULT_LABEL)), -1);

        // label doesn't matter at all
        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultA, MergedResultScanner.ORIGINAL_RESULT_LABEL),
                new AnnotatedResult(resultB, MergedResultScanner.SHADOW_RESULT_LABEL)), -1);
        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultA, MergedResultScanner.SHADOW_RESULT_LABEL),
                new AnnotatedResult(resultB, MergedResultScanner.ORIGINAL_RESULT_LABEL)), -1);

        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultB, MergedResultScanner.ORIGINAL_RESULT_LABEL),
                new AnnotatedResult(resultA, MergedResultScanner.ORIGINAL_RESULT_LABEL)), 1);

        resultA = Result.create(Lists.<Cell>newArrayList(new KeyValue(rowKey1, family, col, value)));
        resultB = Result.create(Lists.<Cell>newArrayList(new KeyValue(rowKey1, family, col, value)));

        // same row, shadow comes always first
        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultA, MergedResultScanner.ORIGINAL_RESULT_LABEL),
                new AnnotatedResult(resultB, MergedResultScanner.SHADOW_RESULT_LABEL)), 1);

        assertEquals(MergedResultScanner.resultComparator.compare(
                new AnnotatedResult(resultA, MergedResultScanner.SHADOW_RESULT_LABEL),
                new AnnotatedResult(resultB, MergedResultScanner.ORIGINAL_RESULT_LABEL)), -1);
    }
}
