/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import static intellij.com.runit.pushlog;
import static intellij.com.runit_OracleMCO.pushlogOracle;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;
import org.json.JSONObject;
/**
 *
 * @author bhuvaneshwari
 */
public class task_Ora implements Runnable {
   private String name;
    private String input;
    private String distributionnodeid;
    private int typeid;
    private String url;
    private String authString;
    private int sequence_no;

    public int getSequence_no() {
        return sequence_no;
    }

    public void setSequence_no(int sequence_no) {
        this.sequence_no = sequence_no;
    }

    public int getTypeid() {
        return typeid;
    }

    public void setTypeid(int typeid) {
        this.typeid = typeid;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public static Client getClient() {
        return client;
    }

    public static void setClient(Client client) {
        task_Ora.client = client;
    }

    public String getDistributionnodeid() {
        return distributionnodeid;
    }

    public void setDistributionnodeid(String distributionnodeid) {
        this.distributionnodeid = distributionnodeid;
    }

    public task_Ora(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public task_Ora(String input, String distributionnodeid, String name, int typeid, String url, String auString, int sequence_no) {
        this.input = input;
        this.distributionnodeid = distributionnodeid;
        this.name = name;
        this.typeid = typeid;
        this.url = url;
        this.authString = auString;
        this.sequence_no = sequence_no;
    }

//    public void runProd() throws Exception {
//        String url = "https://portal.apdcl.co:8015/api/Info/Purbanchalsaveconsumerinformation";
//        String authString = "ApiUser" + ":" + "ApiPass";
//        runservice(url, authString);
//    }
//
//    public void runTEST() throws Exception {
//
//        String url = "https://apitest.apdclintellismartinfra.in/APDCL/SMAPI/WorkForce/SyncConsumer";
//        String authString = "ARMSUser" + ":" + "w9}bCp&po7";
//        runservice(url, authString);
//    }
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

    public void runservice(String url, String authString) {
       // String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
     //   System.out.println("counter ::  " + name);

        // client = Client.create();
        WebResource webResource = client.resource(url);
        try {
            String output = "";
            String status = "";
            String err = "";

//            boolean success = false;
//            boolean exitprep = false;
            //     while (!success) {
            //    if (!success) {
            if (err.contains("Order date can not be less than last reading date")) {
                System.out.println("Order Date corrected..");

            }

//                    ClientResponse resp = webResource.accept("application/json")
//                            .header("Authorization", "Basic " + authStringEnc)
//                            .post(ClientResponse.class, input);
            WebResource.Builder builder = webResource.getRequestBuilder();
            builder.header("Authorization", "Basic " + authString);
            builder.header("Content-Type", MediaType.APPLICATION_JSON);

            ClientResponse resp = builder.post(ClientResponse.class, input);

            if (resp.getStatus() != 200) {
                System.err.println("Unable to connect to the server>>" + distributionnodeid);
            }
            output = resp.getEntity(String.class);
            if (output.contains("Success")) {
                status = "Success";
                err = output;
            } else {
                err = output;
                status = "Fail";
            }
            System.out.println("response: " + distributionnodeid + " - " + output + "\n" + input + "\n++++++++++++++++\n");
            try {
//                        JSONObject jsonobj = new JSONObject(output);
//                        status = jsonobj.getString("Status");
//                        err = jsonobj.getString("Error_Message") + "";
//                status = output;

                pushlogOracle(status, err, distributionnodeid, input, typeid, sequence_no);
//                if (!success) {
//
//                }
            } catch (Exception e) {
                //success = true;
                //out
            }
//            if (exitprep == true) {
//                success = true;
//            }
//            exitprep = true;
            // }
            //        }
        } catch (Exception e) {
            e.printStackTrace();
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

