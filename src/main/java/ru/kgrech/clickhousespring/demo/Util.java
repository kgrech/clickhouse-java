package ru.kgrech.clickhousespring.demo;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Utility methods used in the sample
 * @author kgrech
 */
public class Util {

    /**
     * Prints result set as a table to std.out
     * @param rs result set to be printed
     * @throws SQLException in case of issues with reading the result set
     */
    public static void printRs(ResultSet rs) throws SQLException {
        ResultSetMetaData rsMetadata = rs.getMetaData();
        for (int i = 1; i<= rsMetadata.getColumnCount(); i++) {
            System.out.printf("%-35s|", rs.getMetaData().getColumnName(i));
        }
        System.out.println();
        printSeparator(rsMetadata.getColumnCount());
        while (rs.next()) {
            for (int i = 1; i <= rsMetadata.getColumnCount(); i++) {
                System.out.printf("%-35s|", rs.getString(i));
            }
            System.out.println();
        }
        printSeparator(rsMetadata.getColumnCount());
    }

    /**
     * prints line full of - symbols for the given number of columns
     * @param columns number of columns in result set
     */
    private static void printSeparator(int columns) {
        for (int i = 1; i<= 36*columns; i++) {
            System.out.print("-");
        }
        System.out.println();
    }
}
