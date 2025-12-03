package org.example;

import org.apache.beam.sdk.transforms.DoFn;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSinkFn extends DoFn<String, Void> {

    private transient Connection conn;
    private transient PreparedStatement stmt;

    @Setup
    public void setup() throws Exception {
        conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/pipeline",
                "root",
                "rootpass"
        );

        stmt = conn.prepareStatement(
                "INSERT INTO classified_entries (payload) VALUES (?)"
        );
    }

    @ProcessElement
    public void process(ProcessContext ctx) throws Exception {
        stmt.setString(1, ctx.element());
        stmt.executeUpdate();
    }

    @Teardown
    public void teardown() throws Exception {
        stmt.close();
        conn.close();
    }
}
