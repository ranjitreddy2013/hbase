package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.mapr.db.sandbox.utils.SandboxAdminUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SandboxPushIntegrationTest extends BaseSandboxIntegrationTest {

    @Ignore
    @Test
    public void testPreventSandboxEditing() throws IOException, SandboxException, InterruptedException {
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "initial val");
        setCellValue(hTableSandbox, newRowId, CF2, COL1, "initial val");

        SandboxAdminUtils.lockEditsForTable(restClient, sandboxTablePath, CF1_NAME);

        Thread.sleep(1000L); // sometime it takes time for locks to be applied

        boolean thrown = false;
        try {
            setCellValue(hTableSandbox, newRowId, CF1, COL1, "another val");
        } catch (Exception ex) {
            thrown = true;
        }

        assertTrue("shouldn't be able to change the value of a locked sandbox CF", thrown);
    }

    @Test
    public void testSimulataneousPush() throws IOException, SandboxException {
        final int SIMULATENOUS_TASKS = 6;

        // add a value to the original
        setCellValue(hTableOriginal, newRowId, CF2, COL1, "initial val");

        // create alternative sandboxes
        List<String> sandboxTablePaths = Lists.newArrayList();

        for (int i = 0; i < SIMULATENOUS_TASKS - 1; i++) {
            String newSandboxPath = String.format("%s_sand_%d", originalTablePath, i);
            sandboxTablePaths.add(newSandboxPath);

            // create sandbox
            sandboxAdmin.createSandbox(newSandboxPath, originalTablePath);
        }

        // assert there's only one initial record in the original
        ResultScanner origResults, sandResults;
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        assertEquals("table should have initial cell", 1L, countCells(origResults));
        assertEquals("table should have initial cell", 1L, countCells(sandResults));

        for (String altSandboxTablePath : sandboxTablePaths) {
            // assert there's only one initial record in the original
            HTable hTableNewSandbox = new HTable(conf, altSandboxTablePath);
            ResultScanner newSandResults = hTableNewSandbox.getScanner(scan);
            assertEquals("table should have initial cell", 1L, countCells(newSandResults));
        }


        // add different records to all sandboxes
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "v4");
        for (String altSandboxTablePath : sandboxTablePaths) {
            // add a unique cell to each sandbox
            HTable hTableNewSandbox = new HTable(conf, altSandboxTablePath);
            setCellValue(hTableNewSandbox, newRowId, CF1, altSandboxTablePath.getBytes(), "v4");
        }

        // original remains with one
        origResults = hTableOriginal.getScanner(scan);
        assertEquals("table should have initial cell", 1L, countCells(origResults));

        // assert there's two records = the initial one from original and the added one (to each sandbx)
        sandResults = hTableSandbox.getScanner(scan);
        assertEquals("table should have initial cell + added one", 2L, countCells(sandResults));

        for (String altSandboxTablePath : sandboxTablePaths) {
            // assert there's two records = the initial one from original and the added one (to each sandbx)
            HTable hTableNewSandbox = new HTable(conf, altSandboxTablePath);
            ResultScanner newSandResults = hTableNewSandbox.getScanner(scan);
            assertEquals("table should have initial cell + added one", 2L, countCells(newSandResults));
        }

        // invoke simultaneous push
        List<Runnable> tasksList = new ArrayList<Runnable>(SIMULATENOUS_TASKS);

        final AtomicInteger succeedPushCount = new AtomicInteger(0);
        final AtomicInteger failedPushCount = new AtomicInteger(0);

        // tasks to push sandboxes (will be spawned all at same time)
        tasksList.add(getPushSandboxTableTask(sandboxTablePath, succeedPushCount, failedPushCount));
        for (String altSandboxTablePath : sandboxTablePaths) {
            tasksList.add(getPushSandboxTableTask(altSandboxTablePath, succeedPushCount, failedPushCount));
        }

        int maxTimeoutSeconds = 300;
        try {
            assertConcurrent("push act with a lock", tasksList, maxTimeoutSeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // verify
        assertEquals("only one successful push", 1, succeedPushCount.get());
        assertEquals("all other push attempts should fail", SIMULATENOUS_TASKS-1, failedPushCount.get());

        origResults = hTableOriginal.getScanner(scan);
        assertEquals("table should have initial cell", 2L, countCells(origResults));

        // delete alternative sandboxes
        for (String altSandboxTablePath : sandboxTablePaths) {
            sandboxAdmin.deleteSandbox(altSandboxTablePath);
        }
    }

    private Runnable getPushSandboxTableTask(final String path, final AtomicInteger succeedPushCount, final AtomicInteger failedPushCount) {
        return new Runnable() {
            public void run() {
                try {
                    sandboxAdmin.pushSandbox(path, false, false);
                    succeedPushCount.incrementAndGet();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (SandboxException e) {
                    if (e.getMessage().equals(SandboxAdmin.LOCK_ACQ_FAIL_MSG)) {
                        failedPushCount.incrementAndGet();
                    }
                }
            }
        };
    }

    @Test
    public void testSnapshotPush() throws IOException, SandboxException {
        // add a value to the original
        setCellValue(hTableOriginal, newRowId, CF2, COL1, "initial val");

        // create sandbox
        String sandboxPath2 = String.format("%s_sand2", originalTablePath);
        sandboxAdmin.createSandbox(sandboxPath2, originalTablePath);

        HTable hTableSandbox2 = new HTable(conf, sandboxPath2);

        ResultScanner origResults, sandResults;
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox2.getScanner(scan);
        assertEquals("table should have initial cell", 1L, countCells(origResults));
        assertEquals("table should have initial cell", 1L, countCells(sandResults));


        setCellValue(hTableSandbox2, newRowId, CF1, COL1, "v4");
        sandResults = hTableSandbox2.getScanner(scan);
        assertEquals("table should have initial cell + added one", 2L, countCells(sandResults));

        sandboxAdmin.pushSandbox(sandboxPath2, true, false);

        Pair<String, Path> volumeInfo = SandboxAdminUtils.getVolumeInfoForPath(restClient, new Path(originalTablePath));
        String volumeName = volumeInfo.getFirst();
        String volumeMountPath = volumeInfo.getSecond().toString();
        String origTableRelativeVolumePath = originalTablePath.substring(volumeMountPath.length());

        String sandbox2Fid = SandboxTableUtils.getFidFromPath(fs, sandboxPath2);
        String snapshotName = String.format(SandboxAdmin.SANDBOX_PUSH_SNAPSHOT_FORMAT, sandbox2Fid);

        Path snapshotPath = new Path(String.format("%s/.snapshot/%s/%s",
                volumeMountPath, snapshotName, origTableRelativeVolumePath));

        // verify existence of table
        assertTrue("snapshot should exist", fs.exists(snapshotPath));

        // verify original content
        origResults = hTableOriginal.getScanner(scan);
        assertEquals("table should have initial cell + pushed content", 2L, countCells(origResults));

        // verify snapshot's content
        HTable hTableSnapshot = new HTable(conf, snapshotPath.toString());
        origResults = hTableSnapshot.getScanner(scan);
        assertEquals("table should have initial cell", 1L, countCells(origResults));

        // cleanup the snapshot
        SandboxAdminUtils.removeSnapshot(restClient, volumeName, snapshotName);
    }

    @Ignore
    @Test
    public void testForcedPush() throws IOException, SandboxException {
        // add a value to the original
        setCellValue(hTableOriginal, newRowId, CF2, COL1, "initial val");

        ResultScanner origResults, sandResults;
        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        assertEquals("table should have initial cell", 1L, countCells(origResults));
        assertEquals("table should have initial cell", 1L, countCells(sandResults));

        // add cell to sandbox
        setCellValue(hTableSandbox, newRowId, CF1, COL1, "ts1");
        sandResults = hTableSandbox.getScanner(scan);
        assertEquals("table should have initial cell + added one", 2L, countCells(sandResults));

        // TODO do same test without push until here and then change verify block below:
        // re-write same cell in original (ts will be > than sandbox cell ts)
        setCellValue(hTableOriginal, newRowId, CF1, COL1, "ts2");
        sandboxAdmin.createEmptySandboxTable(sandboxTablePath+"2", originalTablePath);
        sandboxAdmin.pushSandbox(sandboxTablePath, false, true);

        origResults = hTableOriginal.getScanner(scan);
        sandResults = hTableSandbox.getScanner(scan);
        assertEquals("table should have both 2 cells", 2L, countCells(origResults));
        assertEquals("table should have both 2 cells", 2L, countCells(sandResults));
        assertEquals("sandbox version should be the final one", "ts1",
                getCellValue(hTableOriginal, newRowId, CF1, COL1));


    }
}
