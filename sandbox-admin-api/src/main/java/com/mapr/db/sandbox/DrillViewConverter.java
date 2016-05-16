package com.mapr.db.sandbox;

import org.apache.hadoop.conf.Configuration;

import java.sql.*;

public class DrillViewConverter {
    final Connection connection;
    public DrillViewConverter(String drillConnStr) throws SandboxException {
        try {
            Class.forName("org.apache.drill.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SandboxException("Could not find Drill in the classpath.", e);
        }

        try {
            connection = DriverManager.getConnection(drillConnStr);
        } catch (SQLException e) {
            throw new SandboxException(String.format("Could not connect to Drill (connStr = %s)",
                    drillConnStr), e);
        }
    }

    public void X() {
        try {
            Statement st = connection.createStatement();

            ResultSet rs = st.executeQuery("SELECT * from cp.`employee`");
            while(rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println();
    }
}
