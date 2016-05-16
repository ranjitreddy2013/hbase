package com.mapr.db.sandbox;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Merges two ResultScanners, giving one preference over the other
 */
public class MergedResultScanner extends AbstractClientScanner implements ResultScanner {
    private static final Log LOG = LogFactory.getLog(MergedResultScanner.class);

    static final Integer ORIGINAL_RESULT_LABEL = 1;
    static final Integer SHADOW_RESULT_LABEL = 2;

    final ResultScanner shadow;
    final ResultScanner original;

    private Result last = null;
    private Result lastLast = null;

    static final Comparator<byte[]> rowComparator = new org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator();

    // compares scanner results by rowId
    static final Comparator<AnnotatedResult> resultComparator = new Comparator<AnnotatedResult>() {

        @Override
        public int compare(AnnotatedResult o1, AnnotatedResult o2) {
            int rowComparison = rowComparator.compare(o1.result.getRow(), o2.result.getRow());

            if (rowComparison == 0) {
                // shadow table takes precedence
                return (o1.label == SHADOW_RESULT_LABEL)? -1 : 1;
            }

            return rowComparison;
        }
    };

    private final Iterator<AnnotatedResult> finalIt;
    private final Scan scan;

    public MergedResultScanner(ResultScanner shadow, ResultScanner original, Scan scan) {
        this.shadow = shadow;
        this.original = original;

        List<AnnotatedResultIterator> iterators = Lists.newArrayList(
                new AnnotatedResultIterator(shadow.iterator(), SHADOW_RESULT_LABEL),
                new AnnotatedResultIterator(original.iterator(), ORIGINAL_RESULT_LABEL));

        this.finalIt = Iterators.mergeSorted(iterators, resultComparator);
        this.last = nextElement(finalIt);
        this.scan = scan;
    }

    @Override
    public Result next() throws IOException {
        Result result = null;

        try {
        // TODO honour max results – put some counter
        // TODO honour sort order

        this.lastLast = this.last;

        if (lastLast == null) {
            return null;
        }

        last = nextElement(finalIt);

        // they are the same row
        boolean merged = false;
        while (last != null && rowComparator.compare(lastLast.getRow(), last.getRow()) == 0) {
            result = SandboxHTable.mergeResult(lastLast, last);
            last = nextElement(finalIt);

            if (!result.isEmpty()) {
                merged = true;
                break;
            }

            // if result is empty after merge
            if (last == null) {
                return null;
            }

            this.lastLast = this.last;
            last = nextElement(finalIt);
        }

        if (!merged) {
            // makes sure it removes metadata cols as well
            result = SandboxHTable.mergeResult(lastLast, new Result());
        }

        } catch (Exception ex) {
            LOG.error("SCAN",ex);
        }
        return result;
    }

    private static Result nextElement(Iterator<AnnotatedResult> it) {
        try {
            return it.next().result;
        } catch (NoSuchElementException ex) {
            // handled, returning null
            return null;
        }
    }

    @Override
    public void close() {

    }

    static class AnnotatedResult {
        final Result result;
        final Integer label;

        public AnnotatedResult(Result result, Integer label) {
            this.result = result;
            this.label = label;
        }
    }


    private class AnnotatedResultIterator implements Iterator<AnnotatedResult> {
        final Iterator<Result> it;
        final Integer label;

        public AnnotatedResultIterator(Iterator<Result> iterator, Integer label) {
            this.it = iterator;
            this.label = label;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public AnnotatedResult next() {
            return new AnnotatedResult(it.next(), label);
        }

        @Override
        public void remove() {
            it.remove();
        }
    }
}
