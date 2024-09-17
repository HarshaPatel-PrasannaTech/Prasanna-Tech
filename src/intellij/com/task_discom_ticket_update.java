///*
// * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
// * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
// */
//package intellij.com;
//
//import Decoder.BASE64Encoder;
//import com.sun.jersey.api.client.Client;
//import com.sun.jersey.api.client.ClientResponse;
//import com.sun.jersey.api.client.WebResource;
//import java.security.SecureRandom;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import javax.net.ssl.HttpsURLConnection;
//import javax.net.ssl.SSLContext;
//import javax.ws.rs.core.MediaType;
//import org.json.JSONException;
//import org.json.JSONObject;
//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.net.URLEncoder;
//import java.nio.charset.StandardCharsets;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.stream.Collectors;
//
///**
// *
// * @author bhuvaneshwari
// */
//public class task_discom_ticket_update  implements Runnable{
//    
//
//    public task_discom_ticket_update(String input, String url) {
//        this.input = input;
//        this.url = url;
//       
//    }
//
//    private String input;
//
//    private String url;
//   
//    private static Client client = null;
//
//    static {
//        SSLContext ctx = null;
//        try {
//            //Protocol used is TLSv1.2. You may need other ..check target URL required Protocol
//            ctx = SSLContext.getInstance("TLSv1.2");
//            ctx.init(null, null, new SecureRandom());
//            HttpsURLConnection.setDefaultSSLSocketFactory(ctx.getSocketFactory());
//            client = Client.create();
//        } catch (Exception e) {
//
//        }
//    }
//
//    public void runservice(String url) throws JSONException, Exception {
//
//       
//     
//     try{
//       HttpClient client = HttpClient.newHttpClient();
//
//        Map<String, String> formData = new HashMap<>();
//        formData.put("secret_key", "kfdkd0=t6rjjo^&tgql#+yph&h=e%i6o$^6hz=r!njfkerz!");
//        formData.put("token", "h&gccg8*2q8#s#jbcx@xk9#kl6zhr-hf#_q0ij4g(*52p8(#65");
//        formData.put("tag", "update_ComplaintStatus");
//        formData.put("p_compl_number", "59635");
//        formData.put("smo_compl_number", "240525");
//        formData.put("smo_compl_status", "Closed");
//        formData.put("smo_compl_action_reason", "SMO Team");
//        formData.put("smo_compl_action_description", "Issue Solved");
//        formData.put("smo_compl_action_datetime", "2023-12-29 12:45:09");
//
//       
//        String formDataAsString = formData.entrySet().stream()
//                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
//                .collect(Collectors.joining("&"));
//
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create(url)) // Use the provided URL
//                .header("Content-Type", "application/x-www-form-urlencoded")
//                .POST(HttpRequest.BodyPublishers.ofString(formDataAsString))
//                .build();
//
//        CompletableFuture<HttpResponse<String>> response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
//        response.thenAccept(resp -> {
//            System.out.println("Response: " + resp.body());
//        }).join();
//    
//           
//        } catch (Exception e) {
//            
//            e.printStackTrace();
//        }
//    }
//
//    
//    
////    public static void pushlog(String Status, String err, int distnid, String request, int typeid, int sequence_no, String trackid) throws Exception {
////    System.out.println(Status + " - " + err);
////    try {
////        err = err.replace("'", "");
////        
////        if (!(err.contains("Transaction count after EXECUTE indicates") || err.contains("ject reference not set to an instance of an o"))) {
////            Connection con = ConnectionDAO.getConnectionStyra();
////            String query = "insert into intellismart_mcologs (distributionnodeid, status, response, request, typeid, sequence_no, trackid) values (?, ?, ?, ?, ?, ?, ?)";
////            
////            PreparedStatement pstmt = con.prepareStatement(query);
////            pstmt.setInt(1, distnid);
////            pstmt.setString(2, Status);
////            pstmt.setString(3, err);
////            pstmt.setString(4, request);
////            pstmt.setInt(5, typeid);
////            pstmt.setInt(6, sequence_no);
////            pstmt.setString(7, trackid);
////            
////            pstmt.executeUpdate();
////            con.close();
////        }
////    } catch (Exception e) {
////        throw e;
////    }
////}
////
////    
////    
////    public static void statusUpdate(int distnid, int sequence_no) throws Exception {
////    try {
////        Connection con = ConnectionDAO.getConnectionStyra();
////        String query = "update intellismart_mcologs set mcopushed = true where distributionnodeid = ? and sequence_no = ?";
////        
////        PreparedStatement pstmt = con.prepareStatement(query);
////        pstmt.setInt(1, distnid);
////        pstmt.setInt(2, sequence_no);
////        
////        pstmt.executeUpdate();
////        con.close();
////    } catch (Exception e) {
////        throw e;
////    }
////}
//  
//
//    public void run() {
//        try {
//            //  updateLOG(distributionnodeid, new Gson().toJson(bobj).toString());
//            runservice(url);
//        } catch (Exception ex) {
//            Logger.getLogger(task.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }
//}
//
//    
//    
//
