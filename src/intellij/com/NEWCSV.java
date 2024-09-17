/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
/**
 *
 * @author Bhuvaneshwari
 */
public class NEWCSV {
    
    public static void main(String[] args) throws SQLException, Exception {

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HHmmss");
        String currentDateTime = sdf.format(new Date());

        // Construct the CSV file name with the current timestamp
        String csvFile = "PV01_Meter_Advice_" + currentDateTime + ".csv";
        final int batchSize = 1000; // Adjust this value based on your memory and performance needs

        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();
            String query = "select * from mco_consumer_meterinstallation()";
            rs = stmt.executeQuery(query);

            try (FileWriter writer = new FileWriter(csvFile)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Write CSV headers excluding the headers to be removed
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    if (!columnLabel.equalsIgnoreCase("distributionnodeid") && !columnLabel.equalsIgnoreCase("responselogid")) {
                        writer.append("\"").append(columnLabel).append("\"");
                        if (i < columnCount) {
                            writer.append(',');
                        }
                    }
                }
                writer.append('\n');

                // Regex pattern to identify ratios like 10/1, 10/5, etc.
                Pattern ratioPattern = Pattern.compile("^\\d+/\\d+$");

                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;

                    // Write data in batches
                    for (int i = 1; i <= columnCount; i++) {
                        String columnLabel = metaData.getColumnLabel(i);
                        if (!columnLabel.equalsIgnoreCase("distributionnodeid") && !columnLabel.equalsIgnoreCase("responselogid")) {
                            String value = rs.getString(i);
                            if (value == null) {
                                value = "";
                            } else {
                                // Escape newlines within the value (assuming double quotes are not used within data)
                                value = value.replaceAll("\n", "\\\\n");
                                // Wrap the value in double quotes to prevent misinterpretation by CSV parser
                                if (ratioPattern.matcher(value).matches()) {
                                    value = "'\"" + value + "\""; // Treat the ratio as a string by prepending a single quote
                                } else if (value.contains(",")) {
                                    value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
                                } else {
                                    value = "\"" + value + "\"";
                                }
                            }
                            writer.append(value);
                            if (i < columnCount) {
                                writer.append(',');
                            }
                        }
                    }
                    writer.append('\n');

                    // Flush data after every batch
                    if (rowCount % batchSize == 0) {
                        writer.flush();
                    }
                }

                // Flush any remaining data
                writer.flush();

                System.out.println("CSV file created successfully!");

            } catch (Exception e) {
                System.out.println("Error occurred: " + e);
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (con != null) {
                        con.close();
                    }
                } catch (SQLException e) {
                    System.out.println("Error closing resources: " + e);
                }
            }
        } catch (SQLException e) {
            System.out.println("Error occurred: " + e);
        }
    }
    
}


