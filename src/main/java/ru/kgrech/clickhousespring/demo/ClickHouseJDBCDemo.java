package ru.kgrech.clickhousespring.demo;

import java.sql.*;

/**
 * Sample class to demo JDBC queries over clickhouse
 * @author kgrech
 */
public class ClickHouseJDBCDemo {

    private static final String DB_URL = "jdbc:clickhouse://165.227.137.24:8123/default";

    public void popular2015routest() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("SELECT\n" +
                "    OriginCityName,\n" +
                "    DestCityName,\n" +
                "    count(*) AS flights,\n" +
                "    bar(flights, 0, 20000, 40)\n" +
                "FROM ontime WHERE Year = 2015 GROUP BY OriginCityName, DestCityName ORDER BY flights DESC LIMIT 2");
        while(rs.next()){
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = rs.getString(i);
                System.out.print(rs.getMetaData().getColumnName(i) + ": " + columnValue);
            }
            System.out.println("");

        }
        rs.close();
        statement.close();
        conn.close();
    }

    public static void main(String[] args) throws Exception {
        ClickHouseJDBCDemo demo = new ClickHouseJDBCDemo();
        demo.popular2015routest();
    }
}
