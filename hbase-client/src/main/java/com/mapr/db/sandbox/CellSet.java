package com.mapr.db.sandbox;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Stores a bunch of cells
 */
public class CellSet implements Iterable<Cell> {
    static final Comparator<Cell> CELL_FAM_COL_COMPARATOR = new Comparator<Cell>() {
        @Override
        public int compare(Cell a, Cell b) {
            //row
            int c = Bytes.compareTo(
                    a.getRowArray(), a.getRowOffset(), a.getRowLength(),
                    b.getRowArray(), b.getRowOffset(), b.getRowLength());
            if (c != 0) return c;

            // If the column is not specified, the "minimum" key type appears the
            // latest in the sorted order, regardless of the timestamp. This is used
            // for specifying the last key/value in a given row, because there is no
            // "lexicographically last column" (it would be infinitely long). The
            // "maximum" key type does not need this behavior.
            if (a.getFamilyLength() == 0 && a.getTypeByte() == KeyValue.Type.Minimum.getCode()) {
                // a is "bigger", i.e. it appears later in the sorted order
                return 1;
            }
            if (b.getFamilyLength() == 0 && b.getTypeByte() == KeyValue.Type.Minimum.getCode()) {
                return -1;
            }

            //family
            c = Bytes.compareTo(
                    a.getFamilyArray(), a.getFamilyOffset(), a.getFamilyLength(),
                    b.getFamilyArray(), b.getFamilyOffset(), b.getFamilyLength());
            if (c != 0) return c;

            //qualifier
            c = Bytes.compareTo(
                    a.getQualifierArray(), a.getQualifierOffset(), a.getQualifierLength(),
                    b.getQualifierArray(), b.getQualifierOffset(), b.getQualifierLength());

            return c;
        }
    };

    TreeSet<Cell> cells = Sets.newTreeSet(CELL_FAM_COL_COMPARATOR);

    public void add(Cell cell) {
        cells.add(cell);
    }

    public void add(byte[] row, byte[] family, byte[] qualififer) {
        // create the bare minimum cell for comparison
        Cell cell = CellUtil.createCell(row, family, qualififer, Long.MAX_VALUE, KeyValue.Type.Minimum, new byte[0], new byte[0]);
        add(cell);
    }


    @Override
    public Iterator<Cell> iterator() {
        return cells.iterator();
    }

    public void addAll(CellSet cellSet) {
        for (Cell cell : cellSet) {
            add(cell);
        }
    }

    public void addAll(Iterable<Cell> cells) {
        for (Cell cell : cells) {
            add(cell);
        }
    }
}
