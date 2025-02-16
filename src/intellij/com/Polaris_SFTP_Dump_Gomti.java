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

/**
 *
 * @author Bhuvaneshwari
 */
public class Polaris_SFTP_Dump_Gomti {

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

            //Polarise
//            String query = "select * from mco_consumermeter_installation()";
            String query = "select * from mco_consumermeter_installation() where \"AMISPCode\"='MV02'";

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

                    // Write data in batches
                    for (int i = 1; i <= columnCount; i++) {
                        String columnLabel = metaData.getColumnLabel(i);
                        if (!columnLabel.equalsIgnoreCase("distnid") && !columnLabel.equalsIgnoreCase("sequence_no")) {
                            String value = rs.getString(i);
                            if (value == null) {
                                value = "";
                            } else if (ratioPattern.matcher(value).matches()) {
                                // Add a tab character before the value to ensure Excel treats it as text
                                value = "\t" + value;
                            } else if (columnLabel.equalsIgnoreCase("accountid")) {
                                // Remove leading tab character and extra spaces from the accountid column value
                                value = value.trim();
                                // Escape newlines within the value
                                value = value.replaceAll("\n", "\\\\n");
                                // Wrap the value in double quotes if it contains a comma or newline
                                if (value.contains(",") || value.contains("\n")) {
                                    value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
                                }
                            } else {
                                // Escape newlines within the value
                                value = value.replaceAll("\n", "\\\\n");
                                // Wrap the value in double quotes if it contains a comma or newline
                                if (value.contains(",") || value.contains("\n")) {
                                    value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
                                }
                            }

                            writer.append(value.replace("\"", ""));
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
//  GMOTI CONNECTION 
//        try {
//            JSch jsch = new JSch();
//
//            Session session = jsch.getSession("wfm-gomati", "secure.files.gomatimvvnl.in", 22);
//
//            session.setPassword("xyhdu2fysc#084d1");
//
//            session.setConfig("StrictHostKeyChecking", "no");
//                 session.connect();
//            Channel channel = session.openChannel("sftp");
//            channel.connect();
//            ChannelSftp sftpChannel = (ChannelSftp) channel;
//
//            String remoteDir = "/upload/Input";
//
//            sftpChannel.put(csvFile, remoteDir + "/" + csvFile);
//
//            sftpChannel.exit();
//            session.disconnect();
//
//            System.out.println("File uploaded successfully to SFTP server!");
//        } catch (JSchException | SftpException e) {
//            System.out.println("Error occurred while uploading file to SFTP server: " + e);
//        }
////
    }
}
