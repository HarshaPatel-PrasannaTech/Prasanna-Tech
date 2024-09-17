/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;
import com.jcraft.jsch.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
public class Existing_sftp_push {
    public static void main(String[] args) throws SQLException, IOException, JSchException, SftpException {

        // Assuming the CSV file path is known and exists
        String localCsvFilePath = "C:\\Users\\Bhuvaneshwari\\Desktop\\MV01_Meter_Advice_14-06-2024 105039";

        // Read the existing CSV file
        FileInputStream fis = new FileInputStream(localCsvFilePath);
        byte[] fileContent = new byte[fis.available()];
        fis.read(fileContent);
        fis.close();

//       code to establish SFTP connection and session
        JSch jsch = new JSch();
        Session session = jsch.getSession("styra_mvvnl", "172.31.5.56", 22);
        session.setPassword("5P+365£2E<Uk");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();

        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;

        // Upload the existing CSV file to the SFTP server
        String remoteDir = "/data/Prod/rms_files/Input_Folder/";
        
//        String remoteDir="s3://wfm-mco-up-test/Input/";
//        sftpChannel.put(localCsvFilePath, remoteDir + "/" + "existing_csvfile.csv", fileContent);
         sftpChannel.put(new FileInputStream(localCsvFilePath), remoteDir + "/" + "PV01_Meter_Advice_14-05-2024 134834.csv");
        sftpChannel.exit();
        session.disconnect();

        System.out.println("Existing CSV file uploaded successfully to SFTP server!");
    }
}
