package com.mapr.db.sandbox;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.sql.*;

public class DrillViewConverter {
    private static final Log LOG = LogFactory.getLog(DrillViewConverter.class);

    final Connection connection;
    private final String originalTablePath;
    private final String sandboxTablePath;

    static final String DRILL_JDBC_DRIVER_CLASS = "org.apache.drill.jdbc.Driver";
    static final String VIEW_CREATION_FORMAT = "CREATE VIEW %s.`%s` AS %s";
    static final String RETRIEVE_VIEWS_SQL_STMT = "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS";

    public DrillViewConverter(String drillConnStr, String user, String password, String originalTablePath, String sandboxTablePath) throws SandboxException {
        try {
            Class.forName(DRILL_JDBC_DRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new SandboxException("Could not find Drill in the classpath.", e);
        }

        try {
            connection = DriverManager.getConnection(drillConnStr, user, password);
        } catch (SQLException e) {
            throw new SandboxException(String.format("Could not connect to Drill (connStr = %s)",
                    drillConnStr), e);
        }

        this.originalTablePath = originalTablePath;
        this.sandboxTablePath = sandboxTablePath;
    }

    public void convertViews() throws SandboxException {
        Statement st = null;
        try {
            st = connection.createStatement();
            ResultSet rs = st.executeQuery(RETRIEVE_VIEWS_SQL_STMT);
            while(rs.next()){
                final String viewDefinition = rs.getString(3);

                if (viewDefinition.contains(originalTablePath)) {
                    final String viewSchema = rs.getString(1);
                    final String adaptedViewDef = adaptViewDefinition(viewDefinition);
                    final String adaptedViewName = adaptViewName(rs.getString(2));
                    attemptCreateAdaptedView(connection.createStatement(), viewSchema, adaptedViewName, adaptedViewDef);
                }
            }
        } catch (SQLException e) {
            throw new SandboxException("Error executing SQL in Drill", e);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
            } catch(SQLException se2) {
            }
        }

    }

    private void attemptCreateAdaptedView(Statement st, String viewSchema, final String adaptedViewName, String adaptedViewDef) throws SandboxException {
        String suffix = "";
        int suffixCount = 1;

        while (true) {
            String newViewName = adaptedViewName + suffix;
            String newViewCreationStmt = String.format(VIEW_CREATION_FORMAT,
                    viewSchema, newViewName, adaptedViewDef);

            try {
                st.execute(newViewCreationStmt);
                LOG.info(String.format("Created view %s.%s pointing to sandbox table", viewSchema, newViewName));
                System.out.println(String.format("%s.%s view created.", viewSchema, newViewName));
                break;
            } catch (SQLException e) {
                String msg = e.getLocalizedMessage();
                if (msg.contains("A view with given name") && msg.contains("already exists in schema")) {
                    suffix = Integer.toString(++suffixCount);
                } else {
                    throw new SandboxException("Error in Drill: " + msg, e);
                }
            }
        }
    }


    private String adaptViewDefinition(String viewDefinition) {
        return viewDefinition.replace(originalTablePath, sandboxTablePath);
    }

    private String adaptViewName(String currentViewName) {
        Path sandboxPath = new Path(sandboxTablePath);
        return String.format("%s_%s", currentViewName, sandboxPath.getName());
    }
}
