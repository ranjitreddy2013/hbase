package com.mapr.db.sandbox;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SandboxTool {
    static final String OP_CREATE = "create";
    static final String OP_PUSH = "push";
    static final String OP_INFO = "info";

    static Options createOpts, pushOpts, infoOpts;
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

        infoOpts = new Options();
        infoOpts.addOption(OptionBuilder.withArgName("original table")
                .hasArg()
                .withDescription("original table path")
                .isRequired()
                .create("original"));
        cmdOperationOpts.put(OP_INFO, infoOpts);

        pushOpts = new Options();
        pushOpts.addOption(OptionBuilder.withArgName("sandbox table")
                .hasArg()
                .withDescription("sandbox table path to push")
                .isRequired()
                .create("path"));
        pushOpts.addOption(OptionBuilder
                .withDescription("hang until all records are pushed to the original table")
                .create("wait"));
        cmdOperationOpts.put(OP_PUSH, pushOpts);
    }

    public static void main(String[] args) throws IOException, SandboxException {
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

        SandboxAdmin sandboxAdmin = new SandboxAdmin(new Configuration());

        if (operation.equals(OP_CREATE)) {
            sandboxAdmin.createSandbox(cmd.getOptionValue("path"),
                    cmd.getOptionValue("original"));
        } else if (operation.equals(OP_PUSH)) {
            sandboxAdmin.pushSandbox(cmd.getOptionValue("path"),
                    cmd.hasOption("wait"));
        } else if (operation.equals(OP_INFO)) {
            sandboxAdmin.info(cmd.getOptionValue("original"));
        }
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
