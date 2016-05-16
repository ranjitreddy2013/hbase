package com.mapr.db.sandbox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SandboxTool {
    static final String OP_CREATE = "create";
    static final String OP_PUSH = "push";
    static final String OP_DELETE = "delete";
    static final String OP_LIST = "list";

    static final boolean DEFAULT_SNAPSHOT_BEFORE_PUSH = false;
    static final boolean DEFAULT_FORCE_PUSH = false;

    static Options createOpts, pushOpts, listOpts, deleteOpts;
    static Map<String, Options> cmdOperationOpts = Maps.newHashMap();
    static {
        createOpts = new Options();
        createOpts.addOption(OptionBuilder.withArgName("original table")
                .hasArg()
                .withDescription("original table path")
                .isRequired()
                .create("original"));
        createOpts.addOption(OptionBuilder.withArgName("sandbox table")
                .hasArg()
                .withDescription("sandbox table path")
                .isRequired()
                .create("path"));
        cmdOperationOpts.put(OP_CREATE, createOpts);

        listOpts = new Options();
        listOpts.addOption(OptionBuilder.withArgName("original table")
                .hasArg()
                .withDescription("original table path")
                .create("original"));
        cmdOperationOpts.put(OP_LIST, listOpts);

        pushOpts = new Options();
        pushOpts.addOption(OptionBuilder.withArgName("sandbox table")
                .hasArg()
                .withDescription("sandbox table path to push")
                .isRequired()
                .create("path"));
        pushOpts.addOption(OptionBuilder
                .hasArg() // forces the true or false
                .withDescription(
                        String.format("if true, it takes a snapshot to original table's volume before pushing sandbox (default: %s)", DEFAULT_SNAPSHOT_BEFORE_PUSH)
                )
                .create("snapshot"));
        pushOpts.addOption(OptionBuilder
                .hasArg() // forces the true or false
                // TODO improve the description : >
                .withDescription(
                        String.format("if true, forces all sandbox cell version to overwrite any record that was changed since sandbox creation (default: %s)", DEFAULT_FORCE_PUSH)
                )
                .create("force"));
        cmdOperationOpts.put(OP_PUSH, pushOpts);

        deleteOpts = new Options();
        deleteOpts.addOption(OptionBuilder.withArgName("sandbox table")
                .hasArg()
                .withDescription("sandbox table path to delete")
                .isRequired()
                .create("path"));
        cmdOperationOpts.put(OP_DELETE, deleteOpts);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            printUsage(null);
            System.exit(-1);
        }

        String operation = args[0];

        if (!cmdOperationOpts.containsKey(operation)) {
            printUsage(null);
            System.exit(-1);
        }

        Options opts = cmdOperationOpts.get(operation);

        CommandLine cmd = null;
        CommandLineParser parser = new BasicParser();
        try {
            cmd = parser.parse( opts, args, false);
        } catch (ParseException e) {
            printUsage(operation);
            System.exit(-1);
        }

        try {
            final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            final SandboxAdmin sandboxAdmin = requiresPassword(operation) ?
                    new SandboxAdmin(new Configuration(), currentUser.getUserName(), promptPassword()) :
                    new SandboxAdmin(new Configuration(), null);

            if (operation.equals(OP_CREATE)) {
                sandboxAdmin.createSandbox(cmd.getOptionValue("path"),
                        cmd.getOptionValue("original"));
            } else {
                if (operation.equals(OP_PUSH)) {
                    boolean snapshot = DEFAULT_SNAPSHOT_BEFORE_PUSH;
                    boolean forcePush = DEFAULT_FORCE_PUSH;

                    if (cmd.hasOption("snapshot")) {
                        snapshot = Boolean.valueOf(cmd.getOptionValue("snapshot"));
                    }

                    if (cmd.hasOption("force")) {
                        forcePush = Boolean.valueOf(cmd.getOptionValue("force"));
                    }

                    sandboxAdmin.pushSandbox(cmd.getOptionValue("path"), snapshot, forcePush);
                } else if (operation.equals(OP_LIST)) {
                    String original = cmd.getOptionValue("original");

                    if (original != null) {
                        sandboxAdmin.info(original);
                    } else {
                        List<String> recentSandboxes = sandboxAdmin.listRecent();
                        StringBuffer sb = new StringBuffer();
                        // TODO paged
                        for (String recentSandbox : recentSandboxes) {
                            sb.append(recentSandbox).append("\n");
                        }
                        System.out.println(sb.toString());
                    }
                } else if (operation.equals(OP_DELETE)) {
                    sandboxAdmin.deleteSandbox(cmd.getOptionValue("path"));
                }
            }
        } catch (SandboxException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
        System.err.println("Error: " + e.getMessage());
        System.exit(-1);
    }
        System.exit(0);
    }

    private static boolean requiresPassword(String operation) {
        return Lists.newArrayList(OP_CREATE, OP_DELETE, OP_PUSH).contains(operation);
    }

    private static String promptPassword() {
        System.out.print("Password: ");
        System.out.flush();
        return String.valueOf(System.console().readPassword());
    }

    private static void printUsage(String op) {
        HelpFormatter formatter = new HelpFormatter();

        Set<String> ops;
        if (op == null) {
            ops = cmdOperationOpts.keySet();

            System.out.println("Usage: sandboxcli <operation> <args>");
            System.out.println("Operations:\n");
        } else {
            ops = Sets.newHashSet(op);
        }

        for (String operation : ops) {
            formatter.printHelp(80, String.format("sandboxcli %s", operation), "", cmdOperationOpts.get(operation), "");
        }
    }
}
