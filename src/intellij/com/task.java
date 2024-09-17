/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;
import Decoder.BASE64Encoder;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import static intellij.com.runit.pushlog;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;
import org.json.JSONObject;
//import sun.misc.BASE64Encoder;
/**
 *
 * @author bhuvaneshwari
 */
public class task implements Runnable{
    private String name;
    private billing_bean bobj;
    private String distributionnodeid;
    private int typeid;
    private int sequence_no;

    public int getTypeid() {
        return typeid;
    }

    public void setTypeid(int typeid) {
        this.typeid = typeid;
    }

    public billing_bean getBobj() {
        return bobj;
    }

    public void setBobj(billing_bean bobj) {
        this.bobj = bobj;
    }

    public String getDistributionnodeid() {
        return distributionnodeid;
    }

    public void setDistributionnodeid(String distributionnodeid) {
        this.distributionnodeid = distributionnodeid;
    }

    public task(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public task(billing_bean bobj, String distributionnodeid, String name, int typeid,int sequence_no) {
        this.bobj = bobj;
        this.distributionnodeid = distributionnodeid;
        this.name = name;
        this.typeid = typeid;
        this.sequence_no = sequence_no;
    }

    public void runProd() throws Exception {
        String url = "https://portal.apdcl.co:8015/api/Info/Purbanchalsaveconsumerinformation";
        String authString = "ApiUser" + ":" + "ApiPass";
        runservice(url, authString);
    }

    public void runTEST() throws Exception {

        String url = "http://wss1.rajdiscoms.com/SmartMeteringServices/api/CRMSmartMeteringIntegration/PullMCOData_single";
        String authString = "hcliusr" + ":" + "HCLI@2020";
        runservice(url, authString);
    }
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
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        System.out.println("counter ::  " + name);

        // client = Client.create();
        WebResource webResource = client.resource(url);
        try {
            String output = "";
            String status = "";
            String err = "";
            String input = "";
            boolean success = false;
            boolean exitprep = false;
            while (!success) {

                if (!success) {

                    if (err.contains("Order date can not be less than last reading date")) {
                        System.out.println("Order Date corrected..");

                    }

                    input = new Gson().toJson(bobj).toString();

                    System.out.println(input);
//                    ClientResponse resp = webResource.accept("application/json")
//                            .header("Authorization", "Basic " + authStringEnc)
//                            .post(ClientResponse.class, input);

                    WebResource.Builder builder = webResource.getRequestBuilder();
                    builder.header("Authorization", "Basic " + authStringEnc);
                    builder.header("Content-Type", MediaType.APPLICATION_JSON);

                    ClientResponse resp = builder.post(ClientResponse.class, input);

                    if (resp.getStatus() != 200) {
                        System.err.println("Unable to connect to the server>>" + distributionnodeid);
                    }
                    output = resp.getEntity(String.class);
                    System.out.println("response: " + distributionnodeid + " - " + output);
                    try {
//                        JSONObject jsonobj = new JSONObject(output);
//                        status = jsonobj.getString("Status");
//                        err = jsonobj.getString("Error_Message") + "";
                        status = output;
                        if (status.toLowerCase().equals("\"success\"")) {
                            success = true;
                            status = "Success";
                            err = "";
                        } else {
                            err = status;
                            status = "Fail";
                        }
                        pushlog(status, err, distributionnodeid, input, typeid,sequence_no);
                        if (!success) {

                        }
                    } catch (Exception e) {
                        success = true;
                        //out
                    }
                    if (exitprep == true) {
                        success = true;
                    }
                    exitprep = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            //  updateLOG(distributionnodeid, new Gson().toJson(bobj).toString());
            runProd();
        } catch (Exception ex) {
            Logger.getLogger(task.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
} 
