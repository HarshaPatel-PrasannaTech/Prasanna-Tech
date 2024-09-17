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
import java.util.Date;
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
public class Task_First_Breadth_Polaris implements Runnable {

    public Task_First_Breadth_Polaris(String url, String authString, int responseLogId, int distid, String meter_No, int meter_type) {

        this.url = url;
        this.authString = authString;
        this.responseLogId = responseLogId;
        this.distid = distid;
        this.meter_No = meter_No;
        this.meter_type = meter_type;

    }

    private String url;
    private String authString;
    private static Client client = null;
    private int responseLogId;
    private int distid;
    private String meter_No;

    private int meter_type;

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
            String trackid = "";
            int httpStatusCode = 0;
            int httpcode;

            String device_Type = "meter";

            String input = "";

            input = "{\n"
                    + "    \"device_type\": \"" + device_Type + "\",\n"
                    + "    \"device_number\": \"" + meter_No + "\"\n"
                    + "}";

//            if(meter_type==1)
//            {
//               meter_Type= ""
//            }
//            boolean success = false;
//            boolean exitprep = false;
            //     while (!success) {
            //    if (!success) {
            if (err.contains("Order date can not be less than last reading date")) {
                System.out.println("Order Date corrected..");

            }

            WebResource.Builder builder = webResource.getRequestBuilder();

       builder.header("Content-Type", MediaType.APPLICATION_JSON);

            ClientResponse resp = builder.post(ClientResponse.class, input);

            if (resp.getStatus() != 200) {
                System.err.println("Unable to connect to the server>>");
            }
            output = resp.getEntity(String.class);

//            output = "{\n"
//                    + "    \"success\": true,\n"
//                    + "    \"status_code\": 200,\n"
//                    + "    \"data\": {\n"
//                    + "        \"messasuccessge\": \"Meter is Live...\"\n"
//                    + "    }\n"
//                    + "}";

         System.out.println("Request : --->>> " + input + "--->>");
            System.out.println("Response : --->>> " + output);
            JSONObject job = new JSONObject(output);

            if (output.contains("success") && job.has("success")) {

                if (job.getBoolean("success") == true) {
                    status = "Success";
                   JSONObject newResponse = new JSONObject();
                newResponse.put("status", "success");
                newResponse.put("FBTime", new Date().toString());
                newResponse.put("Responsemsg", "Ping Test Pass");

                System.out.println("Modified Response: --->>> " + newResponse.toString());
                

                    pushLog(responseLogId,distid,newResponse.toString());

                } else {
                    status = "Fail";
                    
                    System.out.println("ping test is failed for this particular meter");

              }
            } else {
//               
                status = "Fail";
                
              System.out.println("ping test is failed for this particular meter");

            }

            try {

            } catch (Exception e) {
                //success = true;
                //out
            }

        } catch (Exception e) {

        }
    }

    public static void pushLog(int responseLogId, int distid,String json) throws Exception {

        try {

            Connection con = ConnectionDAO.getConnectionStyra();

            // Prepare the first querytbl_firstbreath_data
            
            String updateQuery = "UPDATE tbl_firstbreath_data SET success_flag = true WHERE responseid = ?";
            PreparedStatement updateStmt = con.prepareStatement(updateQuery);

            updateStmt.setInt(1, responseLogId);

            updateStmt.executeUpdate();

            // Prepare the second query
            
        String query = "UPDATE tblresponselogs SET response = jsonb_set(cast(response as jsonb), '{propertiesBean,500}', " +
                       "jsonb_build_object('value', ?, " +
                       "'valueId', 0, 'categoryId', 0, 'compTypeId', 0, 'subPropertyList', jsonb_build_array(), 'categorypropertyallocationid', 1781), true) " +
                       "WHERE responselogid = ?";
             
       PreparedStatement updateStmts = con.prepareStatement(query);
         updateStmts.setString(1, json);
        updateStmts.setInt(2, responseLogId);
        updateStmts.executeUpdate();
//                 updateStmts.executeUpdate();
                 
                 
                 String procQuery = "SELECT * FROM sp_processresponse_1(?, ?, ?, ?)";
      PreparedStatement   procStmt = con.prepareStatement(procQuery);
        procStmt.setInt(1, 17);
        procStmt.setInt(2, 1);
        procStmt.setInt(3, 72);
        procStmt.setInt(4, responseLogId);
        procStmt.execute();



            // Close the statements and connection
            updateStmts.close();
            updateStmt.close();
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
