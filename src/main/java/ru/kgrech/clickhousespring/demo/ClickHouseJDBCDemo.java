package ru.kgrech.clickhousespring.demo;

import java.sql.*;

/**
 * Sample class to demo JDBC queries over clickhouse
 * @author kgrech
 */
public class ClickHouseJDBCDemo implements AutoCloseable {

    private static final String DB_URL = "jdbc:clickhouse://localhost:8123/default";

    private final Connection conn;

    /**
     * Creates new instance
     * @throws SQLException in case of connection issue
     */
    public ClickHouseJDBCDemo() throws SQLException {
        conn = DriverManager.getConnection(DB_URL);
    }

    /**
     * Queries db to get most popular flight route for ths given year
     * @param year year to query
     * @throws SQLException in case of query issue
     */
    public void popularYearRoutes(int year) throws SQLException {
        String query = "SELECT " +
                "    OriginCityName, " +
                "    DestCityName, " +
                "    count(*) AS flights, " +
                "    bar(flights, 0, 20000, 40) AS bar " +
                "FROM ontime WHERE Year = ? GROUP BY OriginCityName, DestCityName ORDER BY flights DESC LIMIT 20";
        long time = System.currentTimeMillis();
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setInt(1, year);
            try (ResultSet rs = statement.executeQuery()) {
                Util.printRs(rs);
            }
        }
        System.out.println("Time: " + (System.currentTimeMillis() - time) +" ms");
    }


    /**
     * Queries db to get top flight cities
     * @throws SQLException in case of query issue
     */
    public void mostFlightCities() throws SQLException {
        String query = "SELECT OriginCityName, " +
                "count(*) AS flights FROM ontime " +
                "GROUP BY OriginCityName ORDER BY flights DESC LIMIT 20";
        long time = System.currentTimeMillis();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(query)) {
                Util.printRs(rs);
            }
        }
        System.out.println("Time: " + (System.currentTimeMillis() - time) +" ms");
    }

    /**
     * Queries db to get cities with maximal number of different routes
     * @throws SQLException in case of query issue
     */
    public void maxDirectionCities() throws SQLException {
        String query = "SELECT OriginCityName, uniq(Dest) AS u " +
                "FROM ontime GROUP BY OriginCityName ORDER BY u DESC LIMIT 20";
        long time = System.currentTimeMillis();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(query)) {
                Util.printRs(rs);
            }
        }
        System.out.println("Time: " + (System.currentTimeMillis() - time) +" ms");
    }

    /**
     * Queries db to get the cities with biggest season change
     * @throws SQLException in case of query issue
     */
    public void seasonFlights() throws SQLException {
        String query = "SELECT " +
                "    DestCityName, " +
                "    any(total), " +
                "    avg(abs(monthly * 12 - total) / total) AS avg_month_diff " +
                "FROM " +
                "( " +
                "    SELECT DestCityName, count() AS total " +
                "    FROM ontime GROUP BY DestCityName HAVING total > 100000 " +
                ") " +
                "ALL INNER JOIN " +
                "( " +
                "    SELECT DestCityName, Month, count() AS monthly " +
                "    FROM ontime GROUP BY DestCityName, Month HAVING monthly > 10000 " +
                ") " +
                "USING DestCityName " +
                "GROUP BY DestCityName " +
                "ORDER BY avg_month_diff DESC " +
                "LIMIT 20";
        long time = System.currentTimeMillis();
        try (Statement statement = conn.createStatement()) {
            try (ResultSet rs = statement.executeQuery(query)) {
                Util.printRs(rs);
            }
        }
        System.out.println("Time: " + (System.currentTimeMillis() - time) +" ms");
    }

    @Override
    public void close() throws Exception {
       if (conn != null) {
           conn.close();
       }
    }

    /**
     * Application entry point
     * @param args cli args
     */
    public static void main(String[] args) throws Exception {
        try (ClickHouseJDBCDemo demo = new ClickHouseJDBCDemo()) {
            demo.popularYearRoutes(2015);
            demo.mostFlightCities();
            demo.maxDirectionCities();
            demo.seasonFlights();
        }
    }
}
