/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.json.JSONObject;

/**
 *
 * @author Bhuvaneshwari
 */
public class Runit_Asset_Creation_Cuculus_MVVNL {
    public static void main(String[] args) throws Exception {
//        runTEST("1");
//        

//        runTest("1");
            runProd("1");

        if (args.length > 0) {
            
        } else {
            System.out.println("Pass params either 1 or 2");
        }
    }

   public static void runTest(String type) throws Exception {
        String url = "https://tndjiopowergenie.madhyanchalone.in/external/api/createDevice";
        String authString = "powergenie" + ":" + "Rjio@123";
        preparedata(url, authString, type);
    }
 
   
   public static void runProd(String type) throws Exception {
        String url = "https://jiopowergenie.madhyanchalone.in/external/api/swagger/createDevice";
        String authString = "powergenie" + ":" + "Rjio@123";
        preparedata(url, authString, type);
    }
 
 
  

    public static void preparedata(String url, String authString, String type) {

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        String input = "";
        int counter = 0;
        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();
            String query = "select * from mdm_cuculus_asset_creation() limit 1";
            System.out.println(query);
            rs = stmt.executeQuery(query);

            String output = "{\"Status\":\"NOTHING\",\"Error_Message\":\"Nothing\"}\n"
                    + " ";

            while (rs.next()) {

                boolean success = false;

                String err = "";
                String status = "";
                input = rs.getString("mdm_asset_creation");
                JSONObject json = new JSONObject(input);
                String meterNo = json.getString("meterNo");
                // JSONObject inputjsn= new JSONObject(input);
                 
              //   String dcuNumber= inputjsn.getString("dcuNumber");

                try {

                
                    if (!success) {
                        try { 

                         Task_Asset_Creation_Cuculus_MVVNL task1 = new Task_Asset_Creation_Cuculus_MVVNL(input, url, authString, meterNo);
                            executor.execute(task1);

                        } catch (Exception e) {
                            status = "fail";
                            err = "Passed " + input + " --  API Intergration Failed";

                            success = true;
                        }
                    }
                } catch (Exception e) {
                    System.out.println("+++++ Execption occurred");
                    success = true;
                }
               
            }
            executor.shutdown();
        } catch (Exception e) {
            System.err.print(e);
        } finally {
            try {
                con.close();
                stmt.close();
                rs.close();
            } catch (SQLException ex) {
            }
        }

    }  
}
