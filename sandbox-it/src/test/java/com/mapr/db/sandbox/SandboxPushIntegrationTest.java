package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.sandbox.SandboxTestUtils.*;
import static org.junit.Assert.assertEquals;

public class SandboxPushIntegrationTest extends BaseSandboxIntegrationTest {
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

    // TODO add test for force push
}
