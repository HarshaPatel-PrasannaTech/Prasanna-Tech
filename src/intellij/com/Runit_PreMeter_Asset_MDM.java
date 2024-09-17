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
public class Runit_PreMeter_Asset_MDM {
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
        String url = "https://mdms-asset.test.gomatimvvnl.in/smart_meter_asset_creation/";
        String authString = "Intellismart_acc" + ":" + "Web$oaAcc2023";
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
            String query = "select * from get_premimeter_asset() limit 1";
            System.out.println(query);
            rs = stmt.executeQuery(query);

            String output = "{\"Status\":\"NOTHING\",\"Error_Message\":\"Nothing\"}\n"
                    + " ";

            while (rs.next()) {

                boolean success = false;

                String err = "";
                String status = "";
                input = rs.getString("row_to_json");

                try {

                    // only request update       updateLOG( rs.getString("distributionnodeid"), input);
                    if (!success) {
                        try {

                         task_pre_meter_asset task1 = new task_pre_meter_asset(input, url, authString);
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
