package com.mapr.db.sandbox;


import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.security.UserGroupInformation;

import static com.mapr.db.sandbox.SandboxTable.DEFAULT_META_CF;

/**
 * Goes through all the changes in a sandbox and updates the timestamp
 */
public class TouchSandboxChangesJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(TouchSandboxChangesJob.class);
    final static String NAME = "TouchSandboxChanges";
    final static String PARAM_PUSH_TS = "sandbox.push.ts";
    String sandboxTablePath;
    HTable hTableSandbox;
    boolean isMapReduce = true;
    static boolean isSuccess = false;

    public TouchSandboxChangesJob(String sandboxTablePath) {
        this.sandboxTablePath = sandboxTablePath;
    }


    public int run(String args[]) throws Exception {
        if (isMapReduce) {
            return runMapReduce();
        }

        return -1;
    }

    public static class TouchSandboxTableMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
        long pushTs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            pushTs = Long.parseLong(context.getConfiguration().get(PARAM_PUSH_TS));
        }

        @Override
        public void map(ImmutableBytesWritable row, Result result, Context context)
                throws IOException {
            try {
                final byte[] rowId = result.getRow();
                Delete delete = new Delete(rowId);
                Put put = new Put(rowId);

                for (Cell cell : result.rawCells()) {
                    long ts = cell.getTimestamp();

                    if (ts < pushTs) {
                        byte[] columnQualifier = CellUtil.cloneQualifier(cell);

                        // detect if it is a delete
                        // is cell marked for deletion?
                        if (Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                                DEFAULT_META_CF, 0, DEFAULT_META_CF.length) == 0) {
                            Pair<byte[], byte[]> cellColumn = SandboxTableUtils.getCellFromMarkedForDeletionCell(cell);
                            delete.deleteColumns(cellColumn.getFirst(), cellColumn.getSecond(), pushTs);
                        } else {
                            byte[] family = CellUtil.cloneFamily(cell);
                            byte[] cellValue = CellUtil.cloneValue(cell);
                            put.add(family, columnQualifier, pushTs, cellValue);
                        }
                    }
                }

                if (!delete.isEmpty()) {
                    context.write(row, delete);
                }

                if (!put.isEmpty()) {
                    context.write(row, put);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public int runMapReduce() throws Exception {
        Job job = Job.getInstance(getConf(), NAME + " " + sandboxTablePath);
        Configuration conf = job.getConfiguration();
        final long pushTimestamp = System.currentTimeMillis();
        conf.set(PARAM_PUSH_TS, Long.toString(pushTimestamp));
        conf.set(TableOutputFormat.OUTPUT_TABLE, sandboxTablePath);
        job.setJarByClass(TouchSandboxChangesJob.class);

        // scan definition
        Scan scan = new Scan();
        scan.setTimeRange(0, pushTimestamp-1);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initTableMapperJob(sandboxTablePath,
                scan,
                TouchSandboxTableMapper.class,
                ImmutableBytesWritable.class,
                Result.class,
                job);

        // We don't configure reducer, the mapper will directly write to table
        job.setNumReduceTasks(0);

        String uuid = UUID.randomUUID().toString();
        Path outputPath = new Path(conf.get("hadoop.tmp.dir"), "output_" + uuid);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Result.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.submit();
        LOG.info("Force push mapreduce job_id: "+ job.getJobID().toString());

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        if (ret == 0) {
            FileSystem fs = outputPath.getFileSystem(conf);
            fs.delete(outputPath);
        }

        return ret;
    }

    public static boolean touchSandboxChanges(final String sandboxTablePath, String username, final boolean isMapReduce) throws Exception {
        if (!isMapReduce) {
            throw new SandboxException("Force Push without mapreduce is not supported", null);
        }

        //Run as logged-in user instead of mapr user
        String usingMapReduce = isMapReduce ? "using map-reduce" : "not using map-reduce";
        LOG.info("Running touch sandbox changes job " + usingMapReduce + " as user: " + username);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(username);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                try {
                    final int status = ToolRunner.run(new Configuration(), new TouchSandboxChangesJob(sandboxTablePath), new String[0]);
                    if (status==0) {
                        setJobSuccessful();
                    }
                } catch (Exception e) {
                    LOG.error("Exception while running force push job: " + e);
                    e.printStackTrace();
                    throw new SandboxException("Something went wrong running force push job", e);
                }
                return null;
            }
        });

        try {
            final int status = ToolRunner.run(new Configuration(),
                    new TouchSandboxChangesJob(sandboxTablePath), new String[0]);
            if (status==0) {
                setJobSuccessful();
            }
        } catch (Exception e) {
            LOG.error("Exception while running job: " + e);
            throw new SandboxException(e.getMessage(), e);
        }
        return isSuccess;
    }

    public static void setJobSuccessful() {
        isSuccess = true;
    }
}