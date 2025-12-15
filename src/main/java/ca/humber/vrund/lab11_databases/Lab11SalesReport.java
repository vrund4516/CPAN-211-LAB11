/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ca.humber.vrund.lab11_databases;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author vrund
 */

public class Lab11SalesReport {

    // ----- Only change PASS -----
    private static final String USER = "root";
    private static final String PASS = "9898670643";
    // ----------------------------

    private static final String DB_NAME = "lab11";
    private static final String HOST = "localhost";
    private static final String PORT = "3306";

    // Your lab file (the provided script)
    private static final String SQL_FILE_1 = "SalesScripts.sql";
    private static final String SQL_FILE_2 = "salesScripts.sql"; // lab doc uses this casing sometimes :contentReference[oaicite:3]{index=3}

    private static String serverUrl() {
        return "jdbc:mysql://" + HOST + ":" + PORT +
                "/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    private static String dbUrl() {
        return "jdbc:mysql://" + HOST + ":" + PORT + "/" + DB_NAME +
                "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    public static void main(String[] args) {

        try {
            // Load MySQL driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 1) Connect to server (NO DB yet) and create DB if needed
            try (Connection serverConn = DriverManager.getConnection(serverUrl(), USER, PASS);
                 Statement st = serverConn.createStatement()) {
                st.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DB_NAME);
            }

            // 2) Connect directly to lab11 database
            try (Connection conn = DriverManager.getConnection(dbUrl(), USER, PASS)) {

                // Clean rerun
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate("DROP TABLE IF EXISTS Sales");
                }

                // 3) Run provided SalesScripts.sql (creates + inserts) :contentReference[oaicite:4]{index=4}
                Path scriptPath = locateSqlFile();
                runSqlScript(conn, scriptPath);

                // 4) Select all records from Sales :contentReference[oaicite:5]{index=5}
                Map<String, Integer> totals = new TreeMap<>();

                String query = "SELECT Customer, Product, Price FROM Sales";
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery(query)) {

                    // 5) Process in Java collection (NO SQL SUM) :contentReference[oaicite:6]{index=6}
                    while (rs.next()) {
                        String customer = rs.getString("Customer");
                        int price = rs.getInt("Price");
                        totals.merge(customer, price, Integer::sum);
                    }
                }

                // 6) Output (each customer once) :contentReference[oaicite:7]{index=7}
                System.out.println("Final Report (Weekly Bills)");
                System.out.println("---------------------------");
                for (Map.Entry<String, Integer> e : totals.entrySet()) {
                    System.out.printf("%-12s : $%d%n", e.getKey(), e.getValue());
                }
            }

        } catch (ClassNotFoundException e) {
            System.out.println("ERROR: MySQL JDBC Driver not found. Check your pom.xml dependency.");
        } catch (SQLException e) {
            System.out.println("SQL ERROR: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("FILE ERROR: " + e.getMessage());
        }
    }

    // Finds SalesScripts.sql from common places (project root is best)
    private static Path locateSqlFile() throws IOException {
        Path cwd = Path.of("").toAbsolutePath();

        Path[] candidates = new Path[]{
                cwd.resolve(SQL_FILE_1),
                cwd.resolve(SQL_FILE_2),
                cwd.resolve("src").resolve(SQL_FILE_1),
                cwd.resolve("src").resolve(SQL_FILE_2),
                cwd.resolve("src").resolve("main").resolve("resources").resolve(SQL_FILE_1),
                cwd.resolve("src").resolve("main").resolve("resources").resolve(SQL_FILE_2)
        };

        for (Path p : candidates) {
            if (Files.exists(p)) return p;
        }

        throw new IOException("Cannot find SalesScripts.sql. Put it in project root (same folder as pom.xml). Current folder: " + cwd);
    }

    // Reads .sql, removes "--" comment lines, splits by ';', executes each statement
    private static void runSqlScript(Connection conn, Path scriptPath) throws IOException, SQLException {
        String content = Files.readString(scriptPath);

        StringBuilder cleaned = new StringBuilder();
        for (String line : content.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.startsWith("--") || trimmed.isEmpty()) continue;
            cleaned.append(line).append("\n");
        }

        String[] statements = cleaned.toString().split(";");
        try (Statement st = conn.createStatement()) {
            for (String s : statements) {
                String sql = s.trim();
                if (!sql.isEmpty()) {
                    st.execute(sql);
                }
            }
        }
    }
}
