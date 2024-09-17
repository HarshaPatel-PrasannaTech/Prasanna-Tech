/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import org.json.JSONObject;
import java.sql.PreparedStatement;

/**
 *
 * @author Bhuvaneshwari
 */
public class Consumer_Mco_Polaris_SFTP_Gomti_Cluster {

    public static void main(String[] args) throws SQLException, Exception {
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HHmmss");
        String currentDateTime = sdf.format(new Date());

        // Construct the CSV file name with the current timestamp
        String csvFile = "MV02_Meter_Advice_" + currentDateTime + ".csv";
        final int batchSize = 1000; // Adjust this value based on your memory and performance needs

        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();

            // Polarise
            // String query = "select * from mco_consumermeter_installation()";
            String query = "select * from mco_consumermeter_installation() where \"AMISPCode\"='MV02' limit 1";

            rs = stmt.executeQuery(query);

            try ( FileWriter writer = new FileWriter(csvFile)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Write CSV headers excluding the headers to be removed
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    if (!columnLabel.equalsIgnoreCase("distnid") && !columnLabel.equalsIgnoreCase("sequence_no")) {
                        writer.append(columnLabel);
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

                    // Create a JSON object for the current row
                    JSONObject jsonObject = new JSONObject();
                    StringBuilder csvRow = new StringBuilder();
                    String meterNumber = "";
                    int distnid = 0;
                    int sequenceNo = 0;
                     String accountId="";
                    for (int i = 1; i <= columnCount; i++) {
                        String columnLabel = metaData.getColumnLabel(i);
                        String value = rs.getString(i);

                        // Extract distnid and sequence_no before skipping them
                        if (columnLabel.equalsIgnoreCase("distnid")) {
                            distnid = value != null ? Integer.parseInt(value) : 0; // Parse to int or use default value 0
                        }  if (columnLabel.equalsIgnoreCase("sequence_no")) {
                            sequenceNo = value != null ? Integer.parseInt(value) : 0; // Parse to int or use default value 0
                        }

                        // Check if the column should be skipped for CSV output
                        if (columnLabel.equalsIgnoreCase("distnid") || columnLabel.equalsIgnoreCase("sequence_no")) {
                            continue; // Skip these columns for CSV
                        }

                        if (columnLabel.equalsIgnoreCase("NewMeterSerialNo")) {
                            meterNumber = value != null ? value : "";
                        }
                        
                         if (columnLabel.equalsIgnoreCase("accountId")) {
                            accountId = value != null ? value : "";
                        }

                        if (value == null) {
                            value = "";
                        } else if (ratioPattern.matcher(value).matches()) {
                            // Add a tab character before the value to ensure Excel treats it as text
                            value = "\t" + value;
                        } else {
                            // Escape newlines within the value
                            value = value.replaceAll("\n", "\\\\n");
                            // Wrap the value in double quotes if it contains a comma or newline
                            if (value.contains(",") || value.contains("\n")) {
                                value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
                            }
                        }

                        // Add to JSON object
                        jsonObject.put(columnLabel, value);

                        // Write to CSV
                        csvRow.append(value.replace("\"", ""));
                        if (i < columnCount) {
                            csvRow.append(',');
                        }
                    }
                    writer.append(csvRow.toString());
                    writer.append('\n');

                    // Insert JSON object into database
                    insertRowIntoTable(con, jsonObject, 17, meterNumber, csvFile, "file_sent", 2, 1, distnid, sequenceNo,accountId);

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

//  GMOTI CONNECTION 
        try {
            JSch jsch = new JSch();

            Session session = jsch.getSession("wfm-gomati", "secure.files.gomatimvvnl.in", 22);

            session.setPassword("xyhdu2fysc#084d1");

            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            String remoteDir = "/upload/Input";

            sftpChannel.put(csvFile, remoteDir + "/" + csvFile);

            sftpChannel.exit();
            session.disconnect();

            System.out.println("File uploaded successfully to SFTP server!");
        } catch (JSchException | SftpException e) {
            System.out.println("Error occurred while uploading file to SFTP server: " + e);
        }
//
    }

    private static void insertRowIntoTable(Connection con, JSONObject jsonObject, int projected, String meter_number, String file_name, String Status, int status_type, int typeid, int distnid, int sequenceNo,String accountId) {

        String insertSQL = "INSERT INTO tblmco_pushlogs (request,projectid, meter_number,file_name,status,status_type, typeid,distributionnodeid,sequence_no,consumer_number) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try ( PreparedStatement pstmt = con.prepareStatement(insertSQL)) {
            pstmt.setString(1, jsonObject.toString());
            pstmt.setInt(2, projected);

            pstmt.setString(3, meter_number);
            pstmt.setString(4, file_name);
            pstmt.setString(5, Status);
            pstmt.setInt(6, status_type);
            pstmt.setInt(7, typeid);
            pstmt.setInt(8, distnid);
            pstmt.setInt(9, sequenceNo);
            pstmt.setString(10, accountId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("Error inserting row into table: " + e);
        }
    }
}
