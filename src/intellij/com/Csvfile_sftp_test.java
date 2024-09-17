///*
// * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
// * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
// */
//package intellij.com;
//import java.io.FileWriter;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.sql.Statement;
//import com.jcraft.jsch.*;
//import com.jcraft.jsch.ChannelSftp;
///**
// *
// * @author bhuvaneshwari
// */
//public class Csvfile_sftp_test {
//    public static void main(String[] args) throws SQLException {
//        Connection con = null;
//        Statement stmt = null;
//        ResultSet rs = null;
//        String csvFile = "consumerfour.csv";
//    
//
//    // SFTP details
//    String sftpHost = System.getenv("SFTP_HOST");
//    int sftpPort = Integer.parseInt(System.getenv("SFTP_PORT"));
//    String sftpUsername = System.getenv("SFTP_USERNAME");
//    String sftpPassword = System.getenv("SFTP_PASSWORD");
//    String sftpPath = "/path/to/remote/directory/consumerfour.csv"; // Remote file path
//
//    try (FileWriter writer = new FileWriter(csvFile)) {
//     try {
//            con = ConnectionDAO.getConnectionStyra();
//            stmt = con.createStatement();
//            String query = "select * from mco_consumer_meterinstallation()";
//            rs = stmt.executeQuery(query);
//
//          
//                ResultSetMetaData metaData = rs.getMetaData();
//                int columnCount = metaData.getColumnCount();
//
//                
//                for (int i = 1; i <= columnCount; i++) {
//                    writer.append(metaData.getColumnLabel(i));
//                    if (i < columnCount) {
//                        writer.append(',');
//                    }
//                }
//                writer.append('\n');
//
//                
//                while (rs.next()) {
//                    for (int i = 1; i <= columnCount; i++) {
//                    String value = rs.getString(i);
//              if (value == null) {
//                        value = ""; 
//                    } else if (value.contains(",")) {
//                        value = "\"" + value.replaceAll("\"", "\"\"") + "\"";  // Escape internal quotes
//                    }
//            writer.append(value);
//            if (i < columnCount) {
//              writer.append(',');
//            }
//          }
//          writer.append('\n');
//        }
//      }catch(Exception e){
//          System.out.println("error" +e);
//
//            System.out.println("CSV file created successfully!");
//
//        } finally {
//            try {
//                if (rs != null) {
//                    rs.close();
//                }
//                if (stmt != null) {
//                    stmt.close();
//                }
//                if (con != null) {
//                    con.close();
//                }
//            } catch (SQLException e) {
//                System.out.println("Error closing resources: " + e);
//            }
//        }
//    
//    } catch (Exception e) {
//        System.out.println("Error creating CSV file: " + e);
//        return;
//    }
//
//    // Upload CSV file to SFTP
////
////    try (SFTPClient sftpClient = new SFTPClient()) {
////        sftpClient.connect(sftpHost, sftpPort);
////        sftpClient.authPassword(sftpUsername, sftpPassword);
////        sftpClient.put(csvFile, sftpPath); // Upload the file
////        System.out.println("CSV file uploaded to SFTP successfully!");
//    } catch (Exception e) {
//        System.out.println("Error uploading CSV file: " + e);
//    }
//
//    // ... existing finally block to close resources ...
//}
//}
