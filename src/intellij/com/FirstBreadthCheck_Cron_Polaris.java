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

/**
 *
 * @author Bhuvaneshwari
 */
public class FirstBreadthCheck_Cron_Polaris {
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
        String url = "https://hes.integration.test.gomatimvvnl.in/api/ping/device-ping";
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
        int responseLogId=0;
        int distid=0;
        String meter_No="";
        int meter_type=0;
        int counter = 0;
        try {
            con = ConnectionDAO.getConnectionStyra();
            stmt = con.createStatement();
            String query = "select * from tbl_firstbreath_data where success_flag='false' and meter_type=1 and responseid=132724";
            System.out.println(query);
            rs = stmt.executeQuery(query);

            String output = "{\"Status\":\"NOTHING\",\"Error_Message\":\"Nothing\"}\n"
                    + " ";

            while (rs.next()) {

                boolean success = false;

                String err = "";
                String status = "";
              
                
              responseLogId = rs.getInt("responseid");
              
              distid=rs.getInt("distnid");
              
              meter_No=rs.getString("meter_id");
              
              
              meter_type = rs.getInt("meter_type");
              

                try {

                    // only request update       updateLOG( rs.getString("distributionnodeid"), input);
                    if (!success) {
                        try {

                           Task_First_Breadth_Polaris task1 = new Task_First_Breadth_Polaris( url, authString,responseLogId,distid,meter_No,meter_type);
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
