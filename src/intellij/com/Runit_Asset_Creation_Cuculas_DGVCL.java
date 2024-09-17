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
public class Runit_Asset_Creation_Cuculas_DGVCL {
  public static void main(String[] args) throws Exception {
//        runTEST("1");
//        
         runTest("1");

        if (args.length > 0) {
            //runProd(args[0]);
            //  runProd(args[0]);
        } else {
            System.out.println("Pass params either 1 or 2");
        }
    }

   public static void runTest(String type) throws Exception {
        String url = "https://intelli-wfm-tnd-dgvcl.dgvclinfra.in/api/devices/create";
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
                String meter_No = json.getString("Meter_No");
                
                
                
//                input="[{\n" +
//    "    \"AK\": \"AK11\",\n" +
//    "    \"EK\": \"EK11\",\n" +
//    "    \"APN\": \"M2M.Intellismart.com\",\n" +
//    "    \"LLS\": \"MR12\",\n" +
//    "    \"HLS_(FW)\": \"FW32\",\n" +
//    "    \"HLS_(US)\": \"US12\",\n" +
//    "    \"Meter_No\": \"2302589\",\n" +
//    "    \"TCP_Port\": \"4059\",\n" +
//    "    \"Device_ID\": \"JPM2302589\",\n" +
//    "    \"Meter_Make\": \"JPM\",\n" +
//    "    \"Meter_Type\": \"WC\",\n" +
//    "    \"SIM_Number\": \"567WAS\",\n" +
//    "    \"HES_Service\": \"Cuculus\",\n" +
//    "    \"IMSI_Number\": \"123654987123\",\n" +
//    "    \"Meter_Phase\": \"1-PH\",\n" +
//    "    \"Meter_FW_Ver\": \"76-at-t.3.rt5\",\n" +
//    "    \"HES_Server_IP\": \"XXX.XXX.XXX.XXX\",\n" +
//    "    \"Module_FW_ver\": \"76-at-t.3.rt3\",\n" +
//    "    \"Current_Rating\": \"10-60A\",\n" +
//    "    \"SIM_IP_address\": \"13233456211112\",\n" +
//    "    \"Meter_Body_Seal_1\": \"T123216\",\n" +
//    "    \"Meter_Body_Seal_2\": \"T123217\",\n" +
//    "    \"Module_Make-_Model\": \"Quectel-EC200\",\n" +
//    "    \"Communication_medium\": \"GPRS\",\n" +
//    "    \"Display_Digits_Length\": \"7\",\n" +
//    "    \"Manufacturing_Month_Year\": \"Oct-23\",\n" +
//    "    \"IMEI_Number_(Cellular)/Node_MAC_Address_(RF)\": \"1.323357124\"\n" +
//    "}]";

                try {

                    // only request update       updateLOG( rs.getString("distributionnodeid"), input);
                    if (!success) {
                        try {

                        Task_Asset_Creation_Cuculas_DGVCL task1 = new Task_Asset_Creation_Cuculas_DGVCL(input, url, authString,meter_No);
                            executor.execute(task1);

                        } catch (Exception e) {
                            status = "fail";
                            err = "Passed " + input + " --  API Intergration Failed";

                            success = true;
                        }
                    }
                } catch (Exception e) {
                    //   pushlogOracle("Fail", "One or more request params are invalid ", json.getInt("SMOID") + "", input, Integer.parseInt(type));
                    System.out.println("+++++ Execption occurred");
                    success = true;
                }
                // }

                //  while (!success) {
                //}
                //   Thread.sleep(1000 * 3);
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
