package com.mapr.db.sandbox.utils;

import com.google.common.collect.Lists;
import com.mapr.cli.*;
import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.cliframework.base.CommandOutput;
import com.mapr.cliframework.base.ProcessedInput;
import com.mapr.db.sandbox.SandboxTable;
import com.mapr.fs.ErrnoException;
import com.mapr.fs.MapRFileStatus;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.proto.Dbserver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class SandboxAdminUtils {
    public static void printErrors(CommandOutput commandOutput) {
        if (commandOutput != null) {
            for (String msg : commandOutput.getOutput().getMessages()) {
                System.err.println(msg);
            }
        }
    }

    public static void createTable(CLICommandFactory cmdFactory, String tablePath) {
        ProcessedInput tableCreationInput = new ProcessedInput(new String[]{
                "table", "create",
                "-path", tablePath
        });

        // Create table
        CommandOutput commandOutput = null;
        try {
            DbCommands tableCreateCmd = (DbCommands) cmdFactory.getCLI(tableCreationInput);
            commandOutput = tableCreateCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void createTableCF(CLICommandFactory cmdFactory, String tablePath, String cf) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "cf", "create",
                "-path", tablePath,
                "-cfname", cf
        });

        // Create column family
        CommandOutput commandOutput = null;
        try {
            DbCfCommands cmd = (DbCfCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void createSimilarTable(CLICommandFactory cmdFactory, String tablePath, String similarToTablePath) {
        List<String> params = Lists.newArrayList(
                "table", "create",
                "-path", tablePath
        );

        if (similarToTablePath != null) {
            params.addAll(Lists.<String>newArrayList(
                    "-copymetafrom", similarToTablePath,
                    "-copymetatype", "all"
            ));
        }

        ProcessedInput input = new ProcessedInput(params.toArray(new String[params.size()]));

        // Create sandbox table
        CommandOutput commandOutput = null;
        try {
            DbCommands cmd = (DbCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }


    public static void addTableReplica(CLICommandFactory cmdFactory, String fromTablePath, String toTablePath, boolean paused) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "replica", "add",
                "-path", fromTablePath,
                "-replica", toTablePath,
                "-synchronous", "true",
                "-paused", Boolean.toString(paused)
        });


        // Add Replica Table
        CommandOutput commandOutput = null;
        try {
            DbReplicaCommands cmd = (DbReplicaCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void addUpstreamTable(CLICommandFactory cmdFactory, String toTablePath, String fromTablePath) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "upstream", "add",
                "-path", toTablePath,
                "-upstream", fromTablePath
        });

        // Add Upstream Table
        CommandOutput commandOutput = null;
        try {
            DbUpstreamCommands cmd = (DbUpstreamCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void removeUpstreamTable(CLICommandFactory cmdFactory, String toTablePath, String fromTablePath) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "upstream", "remove",
                "-path", toTablePath,
                "-upstream", fromTablePath
        });

        // Add Upstream Table
        CommandOutput commandOutput = null;
        try {
            DbUpstreamCommands cmd = (DbUpstreamCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void deleteTable(CLICommandFactory cmdFactory, String tablePath) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "delete",
                "-path", tablePath
        });

        CommandOutput commandOutput = null;
        try {
            DbCommands cmd = (DbCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void resumeReplication(CLICommandFactory cmdFactory, String fromTablePath, String toTablePath) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "replica", "resume",
                "-path", fromTablePath,
                "-replica", toTablePath
        });

        CommandOutput commandOutput = null;
        try {
            DbReplicaCommands cmd = (DbReplicaCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void pauseReplication(CLICommandFactory cmdFactory, String fromTablePath, String toTablePath) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "replica", "pause",
                "-path", fromTablePath,
                "-replica", toTablePath
        });

        CommandOutput commandOutput = null;
        try {
            DbReplicaCommands cmd = (DbReplicaCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void deleteCF(CLICommandFactory cmdFactory, String tablePath, String cfName) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "table", "cf", "delete",
                "-path", tablePath,
                "-cfname", cfName
        });

        CommandOutput commandOutput = null;
        try {
            DbCfCommands cmd = (DbCfCommands) cmdFactory.getCLI(input);
            commandOutput = cmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void writeToDfsFile(MapRFileSystem fs, Path destinationPath, String content) {
        File temp = null;
        try {
            temp = File.createTempFile("tmp_file", ".tmp");
            BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
            bw.write(content);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (temp != null) {
            try {
                fs.copyFromLocalFile(true, true, new Path(temp.toURI()), destinationPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("Could not create temp file");
            System.exit(-1);
        }

    }

    /**
     * Returns volume Name and volume mount path for the volume where a given file lives
     * @param fs
     * @param path path
     * @return
     * @throws IOException
     */
    public static Pair<String,Path> getVolumeInfoForPath(MapRFileSystem fs, Path path) throws IOException {
        final Path rootDfsPath = new Path("/");

        String volumeName = "mapr.cluster.root";
        Path currentPath =  path;
        while (!currentPath.equals(rootDfsPath)) {
            MapRFileStatus stat = fs.getMapRFileStatus(currentPath);

            if (stat.isVol()) {
                volumeName = stat.getVolumeInfo().name;
            }

            currentPath = currentPath.getParent();
        }

        return Pair.newPair(volumeName, currentPath);
    }

    public static void createSnapshot(CLICommandFactory cmdFactory, String volumeName, String snapshotName) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "snapshot", "create",
                "-snapshotname", snapshotName,
                "-volume", volumeName
        });

        try {
            SnapshotCommands cmd = (SnapshotCommands) cmdFactory.getCLI(input);
            cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void removeSnapshot(CLICommandFactory cmdFactory, String volumeName, String snapshotName) {
        ProcessedInput input = new ProcessedInput(new String[] {
                "snapshot", "remove",
                "-snapshotname", snapshotName,
                "-volume", volumeName
        });

        try {
            SnapshotCommands cmd = (SnapshotCommands) cmdFactory.getCLI(input);
            cmd.executeRealCommand();
        } catch (Exception e) {
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void lockEditsForTable(MapRFileSystem fs, String tablePathStr) throws IOException {
        Path tablePath = new Path(tablePathStr);
        List<Dbserver.ColumnFamilyAttr> cfAttrs = fs.listColumnFamily(tablePath, false);

        for (Dbserver.ColumnFamilyAttr cfAttr : cfAttrs) {
            Dbserver.ColumnFamilyAttr.Builder builder = cfAttr.toBuilder();
            final String cfName = cfAttr.getSchFamily().getName();
            if (!cfName.equals(SandboxTable.DEFAULT_META_CF_NAME) &&
                    !cfName.equals(SandboxTable.DEFAULT_DIRTY_CF_NAME)) {

                Dbserver.AccessControlExpression writePermAce = Dbserver.AccessControlExpression.newBuilder()
                        .setAccessType(Dbserver.DBAccessType.FamilyWriteData)
                        .build();

                Dbserver.ColumnFamilyAttr.newBuilder().clearAces().addAces(writePermAce);

                try {
                    fs.modifyColumnFamily(tablePath, cfName, builder.build());
                } catch (ErrnoException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
