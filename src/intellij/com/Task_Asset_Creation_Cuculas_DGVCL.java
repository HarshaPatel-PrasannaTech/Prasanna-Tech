/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import Decoder.BASE64Encoder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Bhuvaneshwari
 */
public class Task_Asset_Creation_Cuculas_DGVCL implements Runnable{
     public Task_Asset_Creation_Cuculas_DGVCL (String input, String url, String authString, String meter_No) {
        this.input = input;
        this.url = url;
        this.authString = authString;
        this.meter_No = meter_No;
    }

    private String input;
    private String meter_No;
    private String url;
    private String authString;
    private static Client client = null;

    static {
        SSLContext ctx = null;
        try {
            //Protocol used is TLSv1.2. You may need other ..check target URL required Protocol
            ctx = SSLContext.getInstance("TLSv1.2");
            ctx.init(null, null, new SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
            client = Client.create();
        } catch (Exception e) {

        }
    }

    public void runservice(String url, String authString) throws JSONException, Exception {
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        //   System.out.println("counter ::  " + name);

        // client = Client.create();
        WebResource webResource = client.resource(url);
        try {

            String output = "";
            String status = "";
            String err = "";
            String trackid="";
            int httpStatusCode=0;
            int httpcode;
//            boolean success = false;
//            boolean exitprep = false;
            //     while (!success) {
            //    if (!success) {
            if (err.contains("Order date can not be less than last reading date")) {
                System.out.println("Order Date corrected..");

            }


            WebResource.Builder builder = webResource.getRequestBuilder();
            
            builder.header("Authorization", "Basic " + authStringEnc);
                     builder.header("Content-Type", MediaType.APPLICATION_JSON);



try{

            
       ClientResponse resp = builder.post(ClientResponse.class, '['+input+']');

            if (resp.getStatus() != 200) {
                System.err.println("Unable to connect to the server>>");
            }
            output = resp.getEntity(String.class);
            
//            output="{\"Endpoints\":[{\"DeviceId\":\"LNTMGSSEI0022150\",\"httpStatusCode\":\"ER201\",\"httpStatus\":\"Unauthorized\",\"message\":\"following endpoint is created\"}]}";
           //            output="{\"Endpoints\":[{\"DeviceId\":\"LNTMGTAEW1234569\",\"httpStatusCode\":\"201\",\"httpStatus\":\"Created\",\"message\":\"following endpoint is created\"}]}";
            System.out.println("Request : --->>> " + input + "--->>");
            System.out.println("Response : --->>> " + output);
     //       JSONObject job = new JSONObject(output);

            //--------------------------------------
            
//            if (output.contains("Endpoints") && job.has("output")) {
                JSONArray jsnObject = new JSONArray(output);
                JSONObject jsn2 = jsnObject.getJSONObject(0);
//                JSONObject job2 = endpoints.getJSONObject(0);
                if (jsn2.getString("httpStatus").equals("Success")) {
//                  if (job2.getInt("httpStatusCode") == 201){
                    status = "Success";
//                   
//                         httpStatusCode=job2.getInt("httpStatusCode");
                       err=jsn2.toString();
                       
                      trackid=jsn2.getString("deviceId"); 
                    pushDeviceCreatelog(status, err,meter_No, input, trackid);
//                    statusUpdate(new JSONObject(input).get("MeterSerialNo").toString());

                } else {
                     err=jsn2.toString();
                    status = "Fail";
                     trackid=jsn2.getString("deviceId"); 
//                      httpStatusCode=job2.getInt("httpStatusCode");
                    pushDeviceFailLogs(status, err, meter_No,input , trackid);

                }
//            } else {
////               
//                status = "Fail";
//                pushDeviceFailLogs(status, err, new JSONObject(input).get("Meter_No").toString(), input , trackid);
//            }
            
            try {

            } catch (Exception e) {
                //success = true;
                //out
            }

        } catch (Exception e) {
            System.out.println("expection " +e);
        }

} catch (Exception e) {
     System.out.println("expection " +e);

        }
    }


    
    
    public static void pushDeviceCreatelog(String status, String err, String serialno, String input, String trackid) throws Exception {
    System.out.println(status + " - " + err);
    try {
        err = err.replace("'", "");

        Connection con = ConnectionDAO.getConnectionStyra();

        // Prepare the first query
//        String updateQuery = "UPDATE TP_searialno_push_logs_bcits SET updated = true WHERE searial_no = ?";
//        PreparedStatement updateStmt = con.prepareStatement(updateQuery);
//        updateStmt.setString(1, serialno);
//        updateStmt.executeUpdate();

        // Prepare the second query
        String insertQuery = "INSERT INTO TP_deviceregistration_logs (trackid,  responsemsg, searial_no, status, input, type) VALUES (?, ?,  ?, ?, ?, 'Cuculus')";
        PreparedStatement insertStmt = con.prepareStatement(insertQuery);
        insertStmt.setString(1, trackid);
//        insertStmt.setInt(2, httpStatusCode);
        insertStmt.setString(2, err);
        insertStmt.setString(3, serialno);
        insertStmt.setString(4, status);
        insertStmt.setString(5, input);
        insertStmt.executeUpdate();

        // Close the statements and connection
        insertStmt.close();
//        updateStmt.close();
        con.close();
    } catch (SQLException e) {
        throw e;
    }
}

    public static void statusUpdate(String serialno) throws Exception {
        try {

            Connection con = ConnectionDAO.getConnectionStyra();
            String query = "update tblfactoryfiledetails set devicecreation_hes=true where \"Serial_Number\"='" + serialno + "'";
            // System.out.println("+++++++++++++++++++++++ " + query);
            Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
            con.close();
        } catch (Exception e) {
            throw e;
        }

    }

    public static void pushDeviceFailLogs(String status, String err, String serialno, String input, String trackid) throws Exception {
        System.out.println(status + " - " + err);
        try {
            err = err.replace("'", "");
        

            Connection con = ConnectionDAO.getConnectionStyra();


       String insertQuery = "INSERT INTO TP_deviceregistration_logs (trackid,  responsemsg, searial_no, status, input, type) VALUES (?, ?, ?, ?, ?,  'Cuculus')";
        PreparedStatement insertStmt = con.prepareStatement(insertQuery);
        insertStmt.setString(1, trackid);
//        insertStmt.setString(2, responsecode);
        insertStmt.setString(2, err);
        insertStmt.setString(3, serialno);
        insertStmt.setString(4, status);
        insertStmt.setString(5, input);
        insertStmt.executeUpdate();

        
        insertStmt.close();
        con.close();
    } catch (SQLException e) {
        throw e;
    }
}

    public void run() {
        try {
            //  updateLOG(distributionnodeid, new Gson().toJson(bobj).toString());
            runservice(url, authString);
        } catch (Exception ex) {
            Logger.getLogger(task.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}
