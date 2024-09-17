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

/**
 *
 * @author bhuvaneshwari
 */
public class MVNL_Consumer_MCO_CSV {

    public static void main(String[] args) throws SQLException {

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
//        String csvFile = "consumerfour.csv";
          SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HHmmss");
    String currentDateTime = sdf.format(new Date());

    // Construct the CSV file name with the current timestamp
    String csvFile = "MV03_Meter_Advice_" + currentDateTime + ".csv";
        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();
            String query = "select * from mco_consumermeter_installation()";
            rs = stmt.executeQuery(query);

            try ( FileWriter writer = new FileWriter(csvFile)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                
                for (int i = 1; i <= columnCount; i++) {
                    writer.append(metaData.getColumnLabel(i));
                    if (i < columnCount) {
                        writer.append(',');
                    }
                }
                writer.append('\n');

                
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
              if (value == null) {
                        value = ""; 
                    } else if (value.contains(",")) {
                        value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
                    }
            writer.append(value);
            if (i < columnCount) {
              writer.append(',');
            }
          }
          writer.append('\n');
        }
      }

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
    }
}
