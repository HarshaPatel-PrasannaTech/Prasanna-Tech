/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import com.jcraft.jsch.*;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * @author bhuvaneshwari
 */
public class Csvfile_sftp_phindmethod {

        public static void main(String[] args) throws SQLException, Exception {

        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HHmmss");
        String currentDateTime = sdf.format(new Date());

        // Construct the CSV file name with the current timestamp
        String csvFile = "MV03_Meter_Advice_" + currentDateTime + ".csv";
        final int batchSize = 1000; // Adjust this value based on your memory and performance needs

        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();
            
            //pvvnl
            String query = "select * from mco_consumermeter_installation()";
            
            //mvvnl
//            String query ="select * from mco_consumer_meterinstallation()";
            rs = stmt.executeQuery(query);

            try (FileWriter writer = new FileWriter(csvFile)) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Write CSV headers excluding the headers to be removed
                for (int i = 1; i <= columnCount; i++) {
                    String columnLabel = metaData.getColumnLabel(i);
                    if (!columnLabel.equalsIgnoreCase("distributionnodeid") && !columnLabel.equalsIgnoreCase("responselogid")) {
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
                        if (!columnLabel.equalsIgnoreCase("distributionnodeid") && !columnLabel.equalsIgnoreCase("responselogid")) {
                            String value = rs.getString(i);
                            if (value == null) {
                                value = "";
                            } else if (ratioPattern.matcher(value).matches()) {
                                // Add a tab character before the value to ensure Excel treats it as text
                                value = "\t" + value;
                            } else if (columnLabel.equalsIgnoreCase("accountid")) {
                                
                                if(value.startsWith("0"))
                                {
                                // Format accountid values to preserve leading zeros
                                value = "\"" + value + "\"";
                                }
                                
                                else{
                                       value = "\"" + value + "\"";
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
//                            writer.append(value);
//                            if (i < columnCount) {
//                                writer.append(',');
//                            }
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

        try {
            JSch jsch = new JSch();
    
//            Session session = jsch.getSession("Karthik","172.21.32.2");
//            Session session = jsch.getSession("ramchandra_kini", "172.22.32.4", 22);
            
//             Session session = jsch.getSession("sftpsewuser", "172.21.32.2", 22);
//                Session session = jsch.getSession("styrapp_polaris", "13.233.134.134", 22);    

       Session session = jsch.getSession("wfm-gomati", "secure.files.gomatimvvnl.in", 22); 
//            session.setPassword("Ramchandra@8923#$");

//              session.setPassword("5??RCC@84Tc9");

              session.setPassword("xyhdu2fysc#084d1");

//               session.setPassword("Karthik@6743#$");
               
            session.setConfig("StrictHostKeyChecking", "no");
//            session.connect();
//
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
//

//            String remoteDir = "/directory/Prod/rms_files/Input_Folder/";
            
             String remoteDir = "/upload/Input";
            
//               String remoteDir = "/s3://wfm-mco-up-test/Input/";
            
            
            sftpChannel.put(csvFile, remoteDir + "/" + csvFile);

            sftpChannel.exit();
            session.disconnect();
//
            System.out.println("File uploaded successfully to SFTP server!");
        } catch (JSchException | SftpException e) {
            System.out.println("Error occurred while uploading file to SFTP server: " + e);
        }
        
//         try {
//           ProcessBuilder pb = new ProcessBuilder("aws", "s3", "mv", "/directory/Prod/rms_files/Input_Folder/", "s3://wfm-mco-up-test/Input/", "--recursive");
//            Process process = pb.start();
//
//            // Capture and print the output and error streams
//            StringBuilder output = new StringBuilder();
//            StringBuilder error = new StringBuilder();
//
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    output.append(line).append("\n");
//                }
//            }
//
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    error.append(line).append("\n");
//                }
//            }
//
//            int exitCode = process.waitFor();
//            if (exitCode == 0) {
//                System.out.println("AWS CLI command executed successfully.");
//                System.out.println("Output: " + output.toString());
//            } else {
//                System.out.println("Failed to execute AWS CLI command. Exit code: " + exitCode);
//                System.out.println("Error: " + error.toString());
//            }
//        } catch (IOException | InterruptedException e) {
//            System.out.println("Error occurred while executing AWS CLI command: " + e);
//        }
    }
}
