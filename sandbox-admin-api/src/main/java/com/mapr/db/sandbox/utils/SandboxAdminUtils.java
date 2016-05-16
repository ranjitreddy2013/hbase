package com.mapr.db.sandbox.utils;

import com.mapr.cli.DbCfCommands;
import com.mapr.cli.DbCommands;
import com.mapr.cli.DbReplicaCommands;
import com.mapr.cli.DbUpstreamCommands;
import com.mapr.cliframework.base.CLICommandFactory;
import com.mapr.cliframework.base.CommandOutput;
import com.mapr.cliframework.base.ProcessedInput;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
        ProcessedInput colFamilyCreationInput = new ProcessedInput(new String[] {
                "table", "cf", "create",
                "-path", tablePath,
                "-cfname", cf
        });

        // Create column family
        CommandOutput commandOutput = null;
        try {
            DbCfCommands cfCreationCmd = (DbCfCommands) cmdFactory.getCLI(colFamilyCreationInput);
            commandOutput = cfCreationCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void createSimilarTable(CLICommandFactory cmdFactory, String tablePath, String similarToTablePath) {
        ProcessedInput tableCreationInput = new ProcessedInput(new String[] {
                "table", "create",
                "-path", tablePath,
                "-copymetafrom", similarToTablePath,
                "-copymetatype", "all"
        });

        // Create sandbox table
        CommandOutput commandOutput = null;
        try {
            DbCommands tableCreateCmd = (DbCommands) cmdFactory.getCLI(tableCreationInput);
            commandOutput = tableCreateCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }
    }

    public static void setupReplication(CLICommandFactory cmdFactory, String fromTablePath, String toTablePath, boolean paused) {
        ProcessedInput addTableReplicaInput = new ProcessedInput(new String[] {
                "table", "replica", "add",
                "-path", fromTablePath,
                "-replica", toTablePath,
                "-synchronous", "true",
                "-paused", Boolean.toString(paused)
        });

        ProcessedInput addUpstreamTableInput = new ProcessedInput(new String[] {
                "table", "upstream", "add",
                "-path", toTablePath,
                "-upstream", fromTablePath
        });

        // Add Replica Table
        CommandOutput commandOutput = null;
        try {
            DbReplicaCommands addReplicaCmd = (DbReplicaCommands) cmdFactory.getCLI(addTableReplicaInput);
            commandOutput = addReplicaCmd.executeRealCommand();
        } catch (Exception e) {
            SandboxAdminUtils.printErrors(commandOutput);
            e.printStackTrace(); // TODO handle properly
        }

        // Add Upstream Table
        try {
            DbUpstreamCommands addUpstreamCmd = (DbUpstreamCommands) cmdFactory.getCLI(addUpstreamTableInput);
            commandOutput = addUpstreamCmd.executeRealCommand();
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

        try {
            fs.copyFromLocalFile(true, true, new Path(temp.toURI()), destinationPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
