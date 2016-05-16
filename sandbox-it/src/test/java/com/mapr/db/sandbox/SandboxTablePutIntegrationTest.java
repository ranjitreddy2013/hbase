package com.mapr.db.sandbox;

import org.apache.hadoop.hbase.client.HTable;
import org.junit.Test;

import java.io.IOException;

import static com.mapr.db.sandbox.SandboxTestUtils.getCellValue;
import static com.mapr.db.sandbox.SandboxTestUtils.setCellValue;
import static org.junit.Assert.assertEquals;

public class SandboxTablePutIntegrationTest extends BaseSandboxIntegrationTest {
  @Test
  public void testPutOnFilledOriginal() throws IOException, SandboxException {
    // CASE original filled, sandbox empty
    fillOriginalTable();

    assertEquals("original should remain intact", "someString",
            getCellValue(hTableOriginal, existingRowId, CF2, COL2));
    assertEquals("sandbox should return original version", "someString",
            getCellValue(hTableSandbox, existingRowId, CF2, COL2));

    // put to non existing column in orig
    setCellValue(hTableSandbox, existingRowId, CF1, COL2, "testVal1");
    setCellValue(hTableMimic, existingRowId, CF1, COL2, "testVal1");

    // put to existing column in orig
    setCellValue(hTableSandbox, existingRowId, CF2, COL2, "testVal2");
    setCellValue(hTableMimic, existingRowId, CF2, COL2, "testVal2");

    assertEquals("value should be set in sandbox", "testVal2",
            getCellValue(hTableSandbox, existingRowId, CF2, COL2));
    assertEquals("value should be set in mimic", "testVal2",
            getCellValue(hTableMimic, existingRowId, CF2, COL2));

    assertEquals("value should be set in sandbox", "testVal1",
            getCellValue(hTableSandbox, existingRowId, CF1, COL2));
    assertEquals("value should be set in mimic", "testVal1",
            getCellValue(hTableMimic, existingRowId, CF1, COL2));


    // CASE: original filled, sandbox filled (with previously inserted values)
    setCellValue(hTableOriginal, existingRowId, CF2, COL2, "changedInOriginal");

    // put to non existing column in orig
    setCellValue(hTableSandbox, existingRowId, CF2, COL1, "testVal3");
    setCellValue(hTableMimic, existingRowId, CF2, COL1, "testVal3");

    // put to existing column in orig
    setCellValue(hTableSandbox, existingRowId, CF2, COL2, "testVal4");
    setCellValue(hTableMimic, existingRowId, CF2, COL2, "testVal4");

    verifyFinalState(hTableMimic);
    verifyFinalState(hTableSandbox);

    pushSandbox();
    verifyFinalState(hTableOriginal);
  }

  private void verifyFinalState(HTable hTable) throws IOException {
    assertEquals(String.format("value should be appended to table %s", hTable.getName()),
            "testVal1",
            getCellValue(hTable, existingRowId, CF1, COL2));
    assertEquals(String.format("value should be appended to table %s", hTable.getName()),
            "testVal4",
            getCellValue(hTable, existingRowId, CF2, COL2));
    assertEquals(String.format("value should be appended to table %s", hTable.getName()),
            "testVal3",
            getCellValue(hTable, existingRowId, CF2, COL1));
  }

  @Test
  public void testAppendOnEmptyOriginal() throws IOException, SandboxException {
    // CASE original empty, sandbox empty
    assertEquals("original should remain intact", null,
            getCellValue(hTableOriginal, newRowId, CF2, COL2));
    assertEquals("sandbox should return original version", null,
            getCellValue(hTableSandbox, newRowId, CF2, COL2));

    // put to non existing column in orig
    setCellValue(hTableSandbox, newRowId, CF1, COL2, "testVal1");
    setCellValue(hTableMimic, newRowId, CF1, COL2, "testVal1");

    assertEquals("value should be inserted in sandbox", "testVal1",
            getCellValue(hTableSandbox, newRowId, CF1, COL2));
    assertEquals("value should be inserted in mimic", "testVal1",
            getCellValue(hTableMimic, newRowId, CF1, COL2));


    // CASE: original empty, sandbox filled (with previous put)

    // put to existing column in sandbox, non existant in orig
    setCellValue(hTableSandbox, newRowId, CF1, COL2, "testVal2");
    setCellValue(hTableMimic, newRowId, CF1, COL2, "testVal2");

    // put to non existing column in orig, nor sandbox
    setCellValue(hTableSandbox, newRowId, CF1, COL1, "testVal3");
    setCellValue(hTableMimic, newRowId, CF1, COL1, "testVal3");

    verifyFinalState2(hTableMimic);
    verifyFinalState2(hTableSandbox);

    pushSandbox();
    verifyFinalState2(hTableOriginal);
  }

  private void verifyFinalState2(HTable hTable) throws IOException {
    assertEquals(String.format("value should be appended to table %s", hTable.getName()),
            "testVal2",
            getCellValue(hTable, newRowId, CF1, COL2));
    assertEquals(String.format("value should be inserted to table %s", hTable.getName()),
            "testVal3",
            getCellValue(hTable, newRowId, CF1, COL1));
  }


  // TODO put after delete column/row (with original with contents) and then delete again



  // TODO batch puts

//  @Test
//  public void testBatchPut() throws IOException {
//    HTable hTable = new HTable(conf, sandboxTablePath);
//    HTable originalHTable = new HTable(conf, originalTablePath);
//
//    ResultScanner scanner;
//
//    // check initial state
//    scanner = originalHTable.getScanner(new Scan());
//    assertEquals("original and sandbox tables should be empty", 0L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("original and sandbox tables should be empty", 0L, countCells(scanner));
//
//
//    // insert some data into sandbox
//    for (int i = 0; i < 25; i++) {
//      Put put = new Put(new String("rowId" + i).getBytes());
//      put.add(CF1, "col1".getBytes(), Integer.toString(i).getBytes());
//      put.add(CF1, "col2".getBytes(), Integer.toString(i * 5).getBytes());
//      hTable.put(put);
//    }
//    hTable.flushCommits();
//
//    // count cells
//    scanner = originalHTable.getScanner(new Scan());
//    assertEquals("original table should be empty", 0L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted cells", 50L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted rows", 25L, countRows(scanner));
//
//    // delete some rows
//    for (int i = 0; i < 10; i++) {
//      Delete delete = new Delete(new String("rowId" + i).getBytes());
//      hTable.delete(delete);
//    }
//
//    // count cells
//    scanner = originalHTable.getScanner(new Scan());
//    assertEquals("original table should be empty", 0L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted cells", 30L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted rows", 15L, countRows(scanner));
//
//    // update columns
//    for (int i = 5; i < 10; i++) {
//      Put put = new Put(new String("rowId" + i).getBytes());
//      put.add(CF1, "col3".getBytes(), "other value".getBytes());
//      hTable.put(put);
//    }
//    hTable.flushCommits();
//
//    // count cells
//    scanner = originalHTable.getScanner(new Scan());
//    assertEquals("original table should be empty", 0L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted cells", 35L, countCells(scanner));
//    scanner = hTable.getScanner(new Scan());
//    assertEquals("sandbox should contain all inserted rows", 20L, countRows(scanner));
//  }


}
