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
public class Runit_Mi_Updation_Cuculus_DGVCL {
     public static void main(String[] args) throws Exception {

//        
         runTest("1");

        if (args.length > 0) {
            
        } else {
            System.out.println("Pass params either 1 or 2");
        }
    }

   public static void runTest(String type) throws Exception {
        String url = "https://intelli-wfm-tnd-dgvcl.dgvclinfra.in/api/devices/update";
        String authString = "intellismart" + ":" + "fnywLj7mE8aS";
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
            String query = "select * from mdm_cuculus_mi_data_asset_creation() limit 1";
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
                String meter_no = json.getString("Meter_No");
                
//                input = "[{\n" +
//    "    \"Consumer_Number\": \"123\",\n" +
//    "    \"Consumer_Name\": \"ABC\",\n" +
//    "    \"Meter_No\": \"2302583\",\n" +
//    "    \"Meter_Phase\": \"1-PH\",\n" +
//    "    \"Meter_Manufacturer\": \"JPM\",\n" +
//    "    \"Latitude\": 54.54,\n" +
//    "    \"Longitude\": 97.97,\n" +
//    "    \"SIM_Number\": \"13233456211215\",\n" +
//    "    \"SIM_IP_address\": \"192.168.0.9\",\n" +
//    "    \"Status\": \"Installed\",\n" +
//    "    \"Status_Timestamp\": \"2024-07-30T07:21:46.416Z\"\n" +
//    "}]";

                try {

                    
                    if (!success) {
                        try {

                        Task_Mi_Updation_Cuculus_DGVCL task1 = new Task_Mi_Updation_Cuculus_DGVCL(input, url, authString,meter_no );
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
