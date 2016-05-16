package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static com.mapr.db.sandbox.SandboxTestUtils.countCells;
import static com.mapr.db.sandbox.SandboxTestUtils.countRows;
import static org.junit.Assert.*;

public class EmptyOriginalTableIntegrationTest extends
		BaseSandboxIntegrationTest {
	@Test
	public void testSandboxCountRows() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);

		ResultScanner scanner = hTable.getScanner(new Scan());
		assertEquals("original and sandbox tables should be empty", 0L,
				countRows(scanner));

		for (int i = 0; i < 25; i++) {
			Put put = new Put(new String("rowId" + i).getBytes());
			put.add(CF1, "col".getBytes(), Integer.toString(i).getBytes());
			hTable.put(put);
		}
		hTable.flushCommits();

		ResultScanner scanner2 = hTable.getScanner(new Scan());
		assertEquals("sandbox should have the new rows", 25L,
				countRows(scanner2));

		HTable originalHTable = new HTable(conf, originalTablePath);
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should remain empty", 0L,
				countRows(scanner));
	}

	@Test
	public void testExists() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		String testRow1 = "rowId3";
		String testRow2 = "rowId5";

		assertFalse("sandbox should not contain any row at all",
				hTable.exists(new Get(testRow1.getBytes())));
		assertFalse("sandbox should not contain any row at all",
				hTable.exists(new Get(testRow2.getBytes())));

		// add the rows
		Put put1 = new Put(testRow1.getBytes());
		put1.add(CF1, "col".getBytes(), Integer.toString(1).getBytes());
		hTable.put(put1);

		Put put2 = new Put(testRow2.getBytes());
		put2.add(CF1, "col1".getBytes(), Integer.toString(1).getBytes());
		put2.add(CF1, "col2".getBytes(), Integer.toString(1).getBytes());
		hTable.put(put2);
		hTable.flushCommits();

		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow1.getBytes())));
		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow2.getBytes())));
		assertTrue("sandbox should contain the newly added row",
				hTable.exists(new Get(testRow1.getBytes())));
		assertTrue("sandbox should contain the newly added row",
				hTable.exists(new Get(testRow2.getBytes())));

		// delete the row
		hTable.delete(new Delete(testRow1.getBytes()));
		hTable.flushCommits();

		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow1.getBytes())));
		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow2.getBytes())));
		assertFalse("sandbox should not contain the deleted row",
				hTable.exists(new Get(testRow1.getBytes())));
		assertTrue("sandbox should still contain the other row",
				hTable.exists(new Get(testRow2.getBytes())));

		// delete a col from 2nd row and check existance
		Delete delete = new Delete(testRow1.getBytes());
		delete.deleteColumn(CF1, "col2".getBytes());
		hTable.delete(delete);
		hTable.flushCommits();

		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow1.getBytes())));
		assertFalse("original table should not contain added entry to sandbox",
				originalHTable.exists(new Get(testRow2.getBytes())));
		assertFalse("sandbox should not contain the deleted row",
				hTable.exists(new Get(testRow1.getBytes())));
		assertTrue("sandbox should still contain the 2nd row",
				hTable.exists(new Get(testRow2.getBytes())));
	}

	@Test
	public void testBatchExists() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		String testRow1 = "rowId3";
		String testRow2 = "rowId5";

		Boolean[] results;

		// check original table
		results = originalHTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertFalse("original should not contain any row at all", result);
		}

		// check sandbox
		results = hTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertFalse("sandbox should not contain any row at all", result);
		}

		// add the rows
		Put put1 = new Put(testRow1.getBytes());
		put1.add(CF1, "col".getBytes(), Integer.toString(1).getBytes());
		hTable.put(put1);

		Put put2 = new Put(testRow2.getBytes());
		put2.add(CF1, "col1".getBytes(), Integer.toString(1).getBytes());
		put2.add(CF1, "col2".getBytes(), Integer.toString(1).getBytes());
		hTable.put(put2);
		hTable.flushCommits();

		// check original table
		results = originalHTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertFalse("original should not contain any row at all", result);
		}

		// check sandbox
		results = hTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertTrue("sandbox should contain the newly added row", result);
		}

		// delete the row
		hTable.delete(new Delete(testRow1.getBytes()));
		hTable.flushCommits();

		// check original table
		results = originalHTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertFalse("original should not contain any row at all", result);
		}

		// check sandbox table
		results = hTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		assertFalse("sandbox should not contain the deleted row", results[0]);
		assertTrue("sandbox should still contain the other row", results[1]);

		// delete a col from 2nd row and check existance
		Delete delete = new Delete(testRow1.getBytes());
		delete.deleteColumn(CF1, "col2".getBytes());
		hTable.delete(delete);
		hTable.flushCommits();

		// check original table
		results = originalHTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		for (Boolean result : results) {
			assertFalse("original should not contain any row at all", result);
		}

		// check sandbox table
		results = hTable.exists(Lists.newArrayList(
				new Get(testRow1.getBytes()), new Get(testRow2.getBytes())));

		assertFalse("sandbox should not contain the deleted row", results[0]);
		assertTrue("sandbox should still contain the 2nd row", results[1]);
	}

	@Test
	public void testBatchDelete() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		ResultScanner scanner;

		// check initial state
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original and sandbox tables should be empty", 0L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("original and sandbox tables should be empty", 0L,
				countCells(scanner));

		// insert some data into sandbox
		for (int i = 0; i < 25; i++) {
			Put put = new Put(new String("rowId" + i).getBytes());
			put.add(CF1, "col1".getBytes(), Integer.toString(i).getBytes());
			put.add(CF1, "col2".getBytes(), Integer.toString(i * 5).getBytes());
			hTable.put(put);
		}
		hTable.flushCommits();

		// count cells
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should be empty", 0L, countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted cells", 50L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted rows", 25L,
				countRows(scanner));

		// delete 2nd col from 5 rows
		List<Delete> deletes = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			Delete delete = new Delete(new String("rowId" + i).getBytes());
			delete.deleteColumns(CF1, "col2".getBytes());
			deletes.add(delete);
		}
		hTable.delete(deletes);

		// recheck
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should be empty", 0L, countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted cells", 45L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted rows", 25L,
				countRows(scanner));
	}

	@Test
	public void testBatchPut() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		ResultScanner scanner;

		// check initial state
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original and sandbox tables should be empty", 0L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("original and sandbox tables should be empty", 0L,
				countCells(scanner));

		// insert some data into sandbox
		for (int i = 0; i < 25; i++) {
			Put put = new Put(new String("rowId" + i).getBytes());
			put.add(CF1, "col1".getBytes(), Integer.toString(i).getBytes());
			put.add(CF1, "col2".getBytes(), Integer.toString(i * 5).getBytes());
			hTable.put(put);
		}
		hTable.flushCommits();

		// count cells
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should be empty", 0L, countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted cells", 50L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted rows", 25L,
				countRows(scanner));

		// delete some rows
		for (int i = 0; i < 10; i++) {
			Delete delete = new Delete(new String("rowId" + i).getBytes());
			hTable.delete(delete);
		}

		// count cells
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should be empty", 0L, countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted cells", 30L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted rows", 15L,
				countRows(scanner));

		// update columns
		for (int i = 5; i < 10; i++) {
			Put put = new Put(new String("rowId" + i).getBytes());
			put.add(CF1, "col3".getBytes(), "other value".getBytes());
			hTable.put(put);
		}
		hTable.flushCommits();

		// count cells
		scanner = originalHTable.getScanner(new Scan());
		assertEquals("original table should be empty", 0L, countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted cells", 35L,
				countCells(scanner));
		scanner = hTable.getScanner(new Scan());
		assertEquals("sandbox should contain all inserted rows", 20L,
				countRows(scanner));
	}

	@Test
	public void testMutateRow() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		final String testRow = "rowId1";

		Result result;

		// check there's nothing in the sandbox before the test
		Get get = new Get(testRow.getBytes());

		result = originalHTable.get(get);
		assertTrue("original should not return any row", result.isEmpty());
		result = hTable.get(get);
		assertTrue("sandbox should not return any row", result.isEmpty());

		// load sandbox table with a row
		Put put = new Put(testRow.getBytes());
		put.add(CF1, "col1".getBytes(), "1".getBytes());
		put.add(CF1, "col2".getBytes(), "2".getBytes());
		hTable.put(put);
		hTable.flushCommits();

		// perform mutation
		put = new Put(testRow.getBytes());
		put.add(CF1, "col2".getBytes(), "20".getBytes());

		Delete delete = new Delete(testRow.getBytes());
		delete.deleteColumn(CF1, "col1".getBytes());

		RowMutations rm = new RowMutations(testRow.getBytes());
		rm.add(put);
		rm.add(delete);
		hTable.mutateRow(rm);
		hTable.flushCommits();

		// verify things got changed
		result = originalHTable.get(get);
		assertTrue("original should not return any row", result.isEmpty());
		result = hTable.get(get);

		assertFalse("sandbox should return the mutations", result.isEmpty());
		assertTrue("should contain the changed column", result.containsColumn(CF1, "col2".getBytes()));
		assertFalse("shouldn't contain the deleted column", result.containsColumn(CF1, "col1".getBytes()));

		assertTrue(
				"should contain the updated value",
				CellUtil.matchingValue(result.getColumnLatestCell(CF1, "col2".getBytes()),"20".getBytes()));

	}

	@Test
	public void testAppend() throws IOException {
		HTable hTable = new HTable(conf, sandboxTablePath);
		HTable originalHTable = new HTable(conf, originalTablePath);

		final String testRow = "rowId1";

		Result result;

		// check there's nothing in the sandbox before the test
		Get get = new Get(testRow.getBytes());

		result = originalHTable.get(get);
		assertTrue("original should not return any row", result.isEmpty());
		result = hTable.get(get);
		assertTrue("sandbox should not return any row", result.isEmpty());

		// load sandbox table with a row
		Put put = new Put(testRow.getBytes());
		put.add(CF1, "col".getBytes(), "1".getBytes());
		hTable.put(put);
		hTable.flushCommits();

		// perform mutation
		Append append = new Append(testRow.getBytes());
		append.add(CF1, "col".getBytes(), "20".getBytes());
		append.add(CF1, "col2".getBytes(), "456".getBytes());
		hTable.append(append);

		// verify things got changed
		result = originalHTable.get(get);
		assertTrue("original should not return any row", result.isEmpty());
		result = hTable.get(get);
		assertFalse("original should not return any row", result.isEmpty());

		assertFalse("sandbox should return the mutations", result.isEmpty());
		assertTrue(
				"should contain the updated value",
				CellUtil.matchingValue(result.getColumnLatestCell(CF1, "col".getBytes()), "120".getBytes()));
	}
	
    @Test
    public void testIncrement() throws IOException {
        HTable hTable = new HTable(conf, sandboxTablePath);
        HTable originalHTable = new HTable(conf, originalTablePath);

        final String testRow = "rowId1";

        Result result;

        // check there's nothing in the sandbox before the test
        Get get = new Get(testRow.getBytes());

        result = originalHTable.get(get);
        assertTrue("original should not return any row", result.isEmpty());
        result = hTable.get(get);
        assertTrue("sandbox should not return any row", result.isEmpty());

        // load sandbox table with a row
        Put put = new Put(testRow.getBytes());
        put.add(CF1, "col".getBytes(), Bytes.toBytes(2L));
        originalHTable.put(put);
        originalHTable.flushCommits();

        // perform mutation
        Increment increment = new Increment(testRow.getBytes());
        increment.addColumn(CF1, "col".getBytes(), 20L);
        increment.addColumn(CF1, "col2".getBytes(), 4L);
        hTable.increment(increment);

        // verify things got changed
        result = originalHTable.get(get);
        assertFalse("original should not return any row", result.isEmpty());
        result = hTable.get(get);
        assertTrue("sandbox should not return any row", result.isEmpty()); // TODO change

        assertFalse("sandbox should return the mutations", result.isEmpty());
        assertTrue("should contain the updated value",
                CellUtil.matchingValue(result.getColumnLatestCell(CF1, "col".getBytes()),
                        Bytes.toBytes(22L)));

        assertTrue("should contain the updated value",
                CellUtil.matchingValue(result.getColumnLatestCell(CF1, "col2".getBytes()),
                        Bytes.toBytes(4L)));
    }


}