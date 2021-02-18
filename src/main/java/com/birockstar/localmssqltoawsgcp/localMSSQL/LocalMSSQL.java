package com.birockstar.localmssqltoawsgcp.localMSSQL;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.birockstar.localmssqltoawsgcp.S3.PutObject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class LocalMSSQL {

    private static final String CONNECTIONURL = "jdbc:sqlserver://localhost:1433;databaseName=WideWorldImporters;user=javasqlaccount;password=specically80";

    public static void main(String[] args) {

        /*
         * String[] colHeaders = getHeaders("Warehouse","StockItems");
         * 
         * for (String col : colHeaders) {
         * 
         * System.out.println(col);
         * 
         * }
         */

        getSourceDataRSHeader();

        // getSourceDataAndWriteDirectlyToS3();

    }

    // don't write local file, just write to S3
    public static void getSourceDataAndWriteDirectlyToS3() {

        List<byte[]> byteList = new ArrayList<>();

        String query = null;

        // String path = "D:\\Downloads\\localSQLtoS3output_testing\\";

        String s = "|";

        char delimiter = s.charAt(0);

        Map<String, String> sourceQueries = sourceQueries();

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            for (Map.Entry<String, String> e : sourceQueries.entrySet()) {

                String queryName = e.getKey();

                // String objectPath = path + queryName + ".csv";

                String objectKey = "raw_data/" + queryName + "/" + queryName + ".csv";

                // FileWriter out = new FileWriter(objectPath);

                // try (CSVPrinter printer = new CSVPrinter(out,
                // CSVFormat.DEFAULT.withDelimiter(delimiter))) {

                query = e.getValue();

                // System.out.println(e.getKey() + " " + e.getValue());

                try (PreparedStatement statement = connection.prepareStatement(query)) {

                    ResultSet rs = statement.executeQuery();

                    ResultSetMetaData rsmd = rs.getMetaData();

                    int colNumber = rsmd.getColumnCount();

                    // iterate row
                    while (rs.next()) {

                        // iterate columns
                        for (int i = 1; i < colNumber; i++) {

                            // add column contents
                            byteList.add(rs.getBytes(i));
                            // add delimiter
                            byteList.add(String.valueOf(delimiter).getBytes(StandardCharsets.UTF_8));

                        }

                    }

                }
                // }

                byte[] allbytes = byteList.stream().collect(() -> new ByteArrayOutputStream(), (b, d) -> {
                    try {
                        b.write(d);
                    } catch (IOException e1) {
                        throw new RuntimeException(e1);
                    }
                }, (a, b) -> {
                }).toByteArray();
                // System.out.println(new String(allbytes));

                // upload to S3
                new PutObject().putS3Object(objectKey, allbytes);

            }
        }

        catch (SQLException ex) {

            ex.printStackTrace();
        }

    }

    public static ResultSet getHeadersRS(String schema, String tableName) {

        ResultSet rs = null;

        String colNameHeadersQuery = " select top 100 COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS \n"
                + "where TABLE_SCHEMA = '" + schema + "' and TABLE_NAME = '" + tableName + "'";

        // execute the query and get the results

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            try (PreparedStatement statement = connection.prepareStatement(colNameHeadersQuery)) {

                rs = statement.executeQuery();

                return rs;

            }

        } catch (SQLException ex) {

            ex.printStackTrace();
        }

        return rs;

    }

    public static String[] getHeaders(String schema, String tableName) {

        List<String> colNames = new ArrayList<>();

        String colNameHeadersQuery = " select top 100 COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS \n"
                + "where TABLE_SCHEMA = '" + schema + "' and TABLE_NAME = '" + tableName + "'";

        // execute the query and get the results

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            try (PreparedStatement statement = connection.prepareStatement(colNameHeadersQuery)) {

                ResultSet rs = statement.executeQuery();

                while (rs.next()) {

                    colNames.add(rs.getString(1));

                }

            }

        } catch (SQLException ex) {

            ex.printStackTrace();
        }

        String[] headerArray = colNames.toArray(new String[0]);

        return headerArray;

    }

    // write local file then copy to S3
    public static void getSourceData() {

        String schema = "Warehouse";

        String query = null;

        String path = "D:\\Downloads\\localSQLtoS3output_testing\\";

        String s = "|";

        char delimiter = s.charAt(0);

        Map<String, String> sourceQueries = sourceQueries();

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            for (Map.Entry<String, String> e : sourceQueries.entrySet()) {

                String tableName = e.getKey();

                String objectPath = path + tableName + ".csv";

                String objectKey = "raw_data/" + tableName + "/" + tableName + ".csv";

                FileWriter out = new FileWriter(objectPath);

                try (CSVPrinter printer = new CSVPrinter(out,
                        CSVFormat.DEFAULT.withDelimiter(delimiter).withHeader(getHeaders(schema, tableName)))) {

                    query = e.getValue();

                    // System.out.println(e.getKey() + " " + e.getValue());

                    try (PreparedStatement statement = connection.prepareStatement(query)) {

                        ResultSet rs = statement.executeQuery();

                        /*
                         * ResultSetMetaData rsmd = rs.getMetaData();
                         * 
                         * int colNumber = rsmd.getColumnCount();
                         */

                        printer.printRecords(rs);
                        printer.println();

                        // iterate row
                        /*
                         * while (rs.next()) {
                         * 
                         * // iterate columns for (int i = 1; i < colNumber; i++) {
                         * 
                         * rs.getString(i);
                         * 
                         * } // process data here
                         * 
                         * }
                         */

                    }
                }

                // upload to S3
                //new PutObject().putS3Object(objectKey, objectPath);

                //upload to GCS


            }
        }

        catch (SQLException ex) {

            ex.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    // write local file then copy to S3
    public static void getSourceDataRSHeader() {

        String schema = "Warehouse";

        String query = null;

        String path = "D:\\Downloads\\localSQLtoS3output_testing\\";

        String s = "|";

        char delimiter = s.charAt(0);

        Map<String, String> sourceQueries = sourceQueries();

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            for (Map.Entry<String, String> e : sourceQueries.entrySet()) {

                String tableName = e.getKey();

                String objectPath = path + tableName + ".csv";

                Path source = Paths.get(objectPath);
                Path target = Paths.get(objectPath + ".gz");

                String objectKey = "raw_data/" + tableName + "/" + tableName + ".csv";

                FileWriter out = new FileWriter(objectPath);

                query = e.getValue();

                // System.out.println(e.getKey() + " " + e.getValue());

                try (PreparedStatement statement = connection.prepareStatement(query)) {

                    ResultSet rs = statement.executeQuery();

                    //ResultSetMetaData rsmd = rs.getMetaData();

                    try (CSVPrinter printer = new CSVPrinter(out,
                            CSVFormat.DEFAULT.withDelimiter(delimiter).withHeader(rs))) {

                        // int colNumber = rsmd.getColumnCount();

                        printer.printRecords(rs);
                        printer.println();

                        // iterate row
                        /*
                         * while (rs.next()) {
                         * 
                         * // iterate columns for (int i = 1; i < colNumber; i++) {
                         * 
                         * rs.getString(i);
                         * 
                         * } // process data here
                         * 
                         * }
                         */

                    }
                }

                try {

                    GzipExample.compressGzip(source, target);
        
                } catch (IOException ex1) {
                    ex1.printStackTrace();
                }

                // upload to S3
                new PutObject().putS3Object(objectKey + ".gz", objectPath + ".gz");

                //upload to GCS
                /*Storage storage = StorageOptions.getDefaultInstance().getService();
                StorageSnippets ss = new StorageSnippets(storage);
                String bucketName = "adeptsentinalstaging20191216";
                String blobName = "localMSSQL/" + tableName + "/" + tableName + ".csv.gz";
                ss.createBlobFromByteArray(bucketName, blobName, objectPath + ".gz");*/

            }
        }

        catch (SQLException ex) {

            ex.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    public static Map<String, String> sourceQueries() {

        Map<String, String> queryDefs = new HashMap<>();

        queryDefs.put("StockItems", "\n" + "SELECT TOP (1000) [StockItemID] \n" + "     ,[StockItemName] \n"
                + "      ,[SupplierID] \n" + "      ,[ColorID] \n" + "      ,[UnitPackageID] \n"
                + "      ,[OuterPackageID] \n" + "      ,[Brand] \n" + "      ,[Size] \n" + "      ,[LeadTimeDays] \n"
                + "      ,[QuantityPerOuter] \n" + "      ,[IsChillerStock] \n" + "      ,[Barcode] \n"
                + "      ,[TaxRate] \n" + "      ,[UnitPrice] \n" + "      ,[RecommendedRetailPrice] \n"
                + "      ,[TypicalWeightPerUnit] \n" + "      ,[MarketingComments] \n" + "      ,[InternalComments] \n"
                + "      ,[Photo] \n" + "      ,[CustomFields] \n" + "      ,[Tags] \n" + "      ,[SearchDetails] \n"
                + "      ,[LastEditedBy] \n" + "      ,[ValidFrom] \n" + "      ,[ValidTo] \n"
                + "  FROM [WideWorldImporters].[Warehouse].[StockItems]");

        queryDefs.put("PackageTypes",
                "SELECT TOP (1000) [PackageTypeID] \n" + ",[PackageTypeName] \n" + ",[LastEditedBy] \n"
                        + ",[ValidFrom] \n" + ",[ValidTo] \n" + "FROM [WideWorldImporters].[Warehouse].[PackageTypes]");

        return queryDefs;

    }

    public static Map<String, String> getSQLSchema(String tableName) {

        // System.out.println("getting schema query");

        Map<String, String> m = new LinkedHashMap<>();

        String query = "select ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '"
                + tableName + "' order by 1 asc";

        try (Connection connection = DriverManager.getConnection(CONNECTIONURL)) {

            try (PreparedStatement statement = connection.prepareStatement(query)) {

                ResultSet rs = statement.executeQuery();

                while (rs.next()) {

                    // System.out.println(rs.getString(2) + rs.getString(3));

                    String col = rs.getString(2);
                    String type = rs.getString(3);

                    // System.out.println("Adding: " + col + " - " + type);

                    m.put(col, type);
                }

            }

        }

        catch (SQLException e) {

            e.printStackTrace();
        }

        return m;

    }

}
