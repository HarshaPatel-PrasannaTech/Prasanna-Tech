/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import Decoder.BASE64Encoder;
import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.json.JSONObject;
/**
 *
 * @author bhuvaneshwari
 */
public class runit_OracleMCO {
     public static void main(String[] args) throws Exception {
        //   runTEST("1");
        runProd("3");
        if (args.length > 0) {
            //runProd(args[0]);
            //  runProd(args[0]);
        } else {
            System.out.println("Pass params either 1 or 2");
        }
    }

    public static void runProd(String type) throws Exception {
        // String url = "http://30.0.0.53/APDCL/SMAPI/WorkForce/SyncConsumer";
        String url = "https://api.apdclintellismartinfra.in/APDCL/SMAPI/WorkForce/SyncConsumer";
        String authString = "StyraPRD" + ":" + "wX37VYim6r";
        preparedata(url, authString, type);
    }

    public static void runTEST(String type) throws Exception {
        String url = "https://apitest.apdclintellismartinfra.in/APDCL/SMAPI/WorkForce/SyncConsumer";
        String authString = "ARMSUser" + ":" + "w9}bCp&po7";
        preparedata(url, authString, type);
    }

    public static void preparedata(String url, String authString, String type) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        System.out.println("Base64 encoded auth string: " + authStringEnc);
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        String input = "";
        int counter = 0;
        try {
            con = getESddscORACLEConnectionCMS();
            stmt = con.createStatement();
            String query = "";

//            query = "WITH MIDATA AS (\n"
//                    + "select _16,_62,_26,_15,a.latitude,_10,to_date(a.surveydate,'dd/MM/yyyy')||' ' || a.surveytime midatdetime ,distributionnodename,distributionnodeid,a.distributionnodecode\n"
//                    + ",_57,_75,_67,_68,_98,_70,_71,_19,_21,_60,_63,_65,_17 ,_18,a.longitude,_39 ,_200,_201,_202,_5 ,_58\n"
//                    + "from etl_midata a\n"
//                    + "where responsestatusid=1 and distributionnodeid not in(select distributionnodeid from intellismart_mcologs where a.distributionnodeid=distributionnodeid and status='Success') )\n"
//                    + ",masterdata as (\n"
//                    + "select distinct  a.distributionnodeid,_80,_81 from etl_masterdata a inner join MIDATA b on a.distributionnodeid=b.distributionnodeid )\n"
//                    + "select  a.distributionnodeid, _16::jsonb->>'mobile_number' \"Mobileno\",5903 \"Login_TblRefID\" ,_62 newsr,(case when lower(_26)='na' then '' else coalesce(_26,'') end)\"Prepaidbalance\"\n"
//                    + ",_15 Address,a.latitude,coalesce(_80,'') \"INSTALLATION_NO\",(case _10 when 'Postpaid' then '3' when 'Prepaid' then '2' else _10 end)::int4 \"Payment_TblRefID\"\n"
//                    + ",midatdetime \"installtion_datetime\",(case when coalesce(_57,'null')='null' then _58 else _57::jsonb->>'meter_Id' end) \"New_serial_Number\",5 \"Circuit_TblRefId\",_75 \"Remark\",_67 \"Sealno_3\",_68 \"Sealno_4\",coalesce(c.distributionnodecode,_5) \"MRU\"\n"
//                    + ",_70 \"Sealno_1\",coalesce(_71,'') Sealno_2,replace(coalesce(_21,'0'),'na','0')::decimal \"LOAD_UNIT\",_19 \"CONS_NAME\",0 \"poleno\",1.0 \"Appversionno\",_200 \"Sealno_5\",1 \"FeederId\",1 \"DTRId\",coalesce(_21,'0') \"CONN_LOAD\"\n"
//                    + ",(case when coalesce(replace(_57::jsonb->>'meter_Phase','1Ph','1 PH'),'null')='null' then replace(_60,'1-PH','1 PH') else replace(_57::jsonb->>'meter_Phase','1Ph','1 PH') end) \"phase\",(case when _63='' then '0' else coalesce(_63,'0') end)::int4 \"kvah\",coalesce(c.distributionnodecode,_5) \"DTR_Name\",1 \"MeterStatus_tblrefid\",(case when _65='' then '0' else coalesce(_65,'0') end)::int4  \"meter_MF\"\n"
//                    + ",coalesce(_80,'') \"OLDrrnumber\",coalesce(_21,'0') \"Sanction_load\",(case coalesce(_60,replace(_57::jsonb->>'meter_Phase','1Ph','1-PH')) when '1-PH' then '1' when '3-PH' then '2' when 'LTCT' then '3' else '0' end)::int4 \"ServicePointMeterPhase_TblRefID\"\n"
//                    + ",(case when _201='na' then '0' else coalesce(_201,'0') end)::int4 \"MDKW\" ,coalesce(_17,'') \"emailID\",a.distributionnodename \"OldmeterSR\"\n"
//                    + ",replace(coalesce(_81,''),'- DOMESTIC-A','') \"ConnectionCategory_TblRefID\",a.distributionnodecode \"RRNumber\",a.longitude,(case when _81='48' then 'LT' when _81='49' then 'H_PWWS_N' when _81='74' then 'LT_DOM_A'\n"
//                    + "else replace(coalesce(_81,''),'- DOMESTIC-A','') end) \"TARIFF\"\n"
//                    + ",(case wfohen _202='na' then '0' else _202 end)::int4  \"PF\"\n"
//                    + ",1 \"CTratio_TblRefID\",coalesce(_39,'0') \"oldfr\",coalesce(_80,'') \"BUSINESS_PARTNER\",'LG' \"wire\",'INTELLISMART' \"company\"\n"
//                    + "from MIDATA a\n"
//                    + "left join tbldistributionnodes b on a.distributionnodeid=b.distributionnodeid\n"
//                    + "left join tbldistributionnodes c on c.distributionnodeid=b.underdistributionnodeid\n"
//                    + "left join masterdata d on a.distributionnodeid=d.distributionnodeid ";
            query = "select row_to_json(z)::text from(\n"
                    + "select mi.distributionnodeid, ol4.location as \"Discom\",ol3.location as \"Circle\", ol2.location \"Division\",ol1.location as \"SubDivision\", mst._2 as \"SubStation\",mst._4 as \"Feeder\",mst._6 as \"DTR\",mst._5 as \"DTRCode\",mi.distributionnodecode as \"ConsumerNumber\",mst._19 as \"ConsumerName\",\n"
                    + "        ci._14 as \"Landmark\",mst._15 as \"AddressFromUtilityMaster\",mi._86 as \"AddressCapturedfromField\" ,mi._16::jsonb->>'mobile_number' as \"MobileNumber\", coalesce(mi._212::jsonb->>'mobile_number','') as \"AlternateMobileNumber\",mi._10 as \"BillingType\",mi._206 as \"OldMeterNumber\",\n"
                    + "        (case when coalesce(_57,'null')='null' then _58 else _57::jsonb->>'meter_Id' end)  \"NewMeterNumber\", ci._81 as \"CategoryCode\",mi._23 as \"OldMeterManufacturer\",mi._21 as \"ContractedLoad\",mi._21 as \"LoadUnit\",\n"
                    + "        mi._42 as \"OldMeterMF\",mi._25 as \"OldMeterPhase\", mi._18 as \"AccountNo\",ci._82 as \"SubCategoryCode\",mi.latitude as \"Latitude\",mi.longitude as \"Longitude\",mi._11 as \"Type\",coalesce(mi._76,'') as \"OldmeterconditionRemarks\",\n"
                    + "        (case when _113='' then '0' else coalesce(_113,'0')end) as \"OldmeterCurrentKwhMI\",(case when _114='' then '0' else coalesce(_114,'0')end) as \"OldmeterCurrentKvahMI\",(case when _115='' then '0' else coalesce(_115,'0')end) as \"OldMeterCurrentKvaMI\",mi._62 \"NewMeterReading\",\n"
                    + "        (case when coalesce(replace(mi._57::jsonb->>'meter_Phase','1Ph','1-PH'),'null')='null' or coalesce(replace(mi._57::jsonb->>'meter_Phase','3Ph','3 PH'),'null')='null' then replace(mi._60,'-PH','-PH')\n"
                    + "        when _57::jsonb->>'meter_Phase' like '1P2W%' then '1-PH' when _57::jsonb->>'meter_Phase' like '3P4W%' then '3 PH WC' when _57::jsonb->>'meter_Phase' like '1 PH' then '1-PH'\n"
                    + "        when _57::jsonb->>'meter_Phase' like 'PCP11-Single%' then '1-PH'  when mi._57::jsonb->>'meter_Phase' like '1PH' then '1-PH' else replace(mi._57::jsonb->>'meter_Phase','Ph','-PH') end) \"NewMeterPhase\",'' as \"NewMeterMF\"\n"
                    + "         ,mi._207 as \"MRNNo\",(case when _57::jsonb->>'meter_Manufacturer' like '%SU%' then 'Crystal' when _57::jsonb->>'meter_Manufacturer' like '%SEIPL%' then 'Schneider' when _57::jsonb->>'meter_Manufacturer' like '%pcp%' then 'PCP International Limited' else '' end) as\"NewMeterManufacturer\",mi.surveydate||' '||mi.surveytime as \"NewMeterReadingDate\"\n"
                    + "        , mi._62 as \"NewMeterReadingKwh\", mi.surveydate||' '||mi.surveytime as \"NewMeterInstalledDate\",mi._67 \"MeterTerminalSealNo\",mst._83 as \"ReadingDigits\"\n"
                    + "        ,fl.tdc as \"TDCFlag\", fl1.tdc as \"PrepaidFlag\"\n"
                    + "\n"
                    + "\n"
                    + "from etl_midata mi\n"
                    + "left join etl_masterdata mst on mi.distributionnodeid=mst.distributionnodeid\n"
                    + "left join etl_cidata ci on mst.distributionnodeid=ci.distributionnodeid \n"
                    + "left join tblofficelocationallocation ol1 on mi.olid=ol1.officelocationallocationid  \n"
                    + "inner join tblofficelocationallocation ol2 ON ol2.officelocationallocationid=ol1.underlocationid\n"
                    + "inner join tblofficelocationallocation ol3 ON ol3.officelocationallocationid=ol2.underlocationid\n"
                    + "inner join tblofficelocationallocation ol4 ON ol4.officelocationallocationid=ol3.underlocationid \n"
                    + "\n"
                    + "LEFT JOIN LATERAL (select value as tdc,responselogid\n"
                    + "from tblresponselogs res1\n"
                    + "cross join lateral jsonb_to_recordset(res1.response::jsonb->'propertiesBean')as x(value text,categorypropertyallocationid int)\n"
                    + "where x.categorypropertyallocationid in(84)\n"
                    + "and res1.distnid=mi.distributionnodeid\n"
                    + ") as fl on true \n"
                    + "LEFT JOIN LATERAL (select value as tdc,responselogid\n"
                    + "from tblresponselogs res2\n"
                    + "cross join lateral jsonb_to_recordset(res2.response::jsonb->'propertiesBean')as x(value text,categorypropertyallocationid int)\n"
                    + "where x.categorypropertyallocationid in(85)\n"
                    + "and res2.distnid=mi.distributionnodeid) as fl1 on true\n"
                    + "\n"
                    + "\n"
                    + "where mi.distributionnodeid in(532787,546602,515984,126188,460070,504629,501987,311859,515820,329300)\n"
                    + ") as z";
//            if (type.equals("1")) {
//                query = "select * from getMCODetails() ";
//            } else if (type.equals("2")) {
//                query = "select * from getsm_sm_details() ";
//            }
            query = "select * from getmcodetails_oracle_v1()";
            System.out.println(query);
            rs = stmt.executeQuery(query);

            String output = "{\"Status\":\"NOTHING\",\"Error_Message\":\"Nothing\"}\n"
                    + " ";

            while (rs.next()) {
                counter++;
                //    System.out.println("Processing.... : " + rs.getString("distributionnodeid"));
                boolean success = false;
                boolean exitprep = false;
                String err = "";
                String status = "";
                input = rs.getString("row_to_json").replace("\\n", "");
                JSONObject json = new JSONObject(input);
                int distnid = json.getInt("SMOID");
                json.remove("SMOID");
                int sequence_no = json.getInt("Sequence_no");
                json.remove("Sequence_no");
                type = json.getInt("Type_id") + "";
                json.remove("Type_id");

                //  while (!success) {
                try {

                    // only request update       updateLOG( rs.getString("distributionnodeid"), input);
                    if (!success) {
                        try {
                            task_Ora task1 = new task_Ora(json.toString(), distnid + "", counter + "", Integer.parseInt(type), url, authString, sequence_no);
                            executor.execute(task1);

                        } catch (Exception e) {
                            status = "fail";
                            err = "Passed " + input + " --  API Intergration Failed";
                            pushlogOracle(status, err, json.getInt("SMOID") + "", input, Integer.parseInt(type), sequence_no);
                            success = true;
                        }
                    }
                } catch (Exception e) {
                    pushlogOracle("Fail", "One or more request params are invalid ", json.getInt("SMOID") + "", input, Integer.parseInt(type), sequence_no);
                    System.out.println("+++++ Execption occurred");
                    success = true;
                }
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

    public static void pushlogOracle(String status, String err, String distnodeid, String request, int typeid, int sequence_no) throws Exception {
        System.out.println(status + " - " + err);
        try {
            err = err.replace("'", "");
            //System.out.println("+++++++++++++++++++++++ " + distnodeid);
            if (!(err.contains("Transaction count after EXECUTE indicates") || err.contains("ject reference not set to an instance of an o"))) {
                Connection con = getESddscORACLEConnectionCMS();
                String query = "insert into intellismart_mcologs (distributionnodeid,status,response,request,typeid,sequence_no) values (" + distnodeid + ",'" + status + "','" + err + "','" + request + "'," + typeid + "," + sequence_no + ")";
                // System.out.println("+++++++++++++++++++++++ " + query);
                Statement stmt = con.createStatement();
                stmt.executeUpdate(query);
                con.close();
            }
        } catch (Exception e) {
            throw e;
        }

    }

    public static Connection getESddscORACLEConnectionCMS() throws Exception {
        Connection connect = null;
//        String url="jdbc:postgresql://108.170.53.66:5432/contractormanagementdee";//contractormanagementmay05";//+username+"&password="+password+"&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
//        String url = "jdbc:postgresql://localhost:5432/contractormanagementdee";
//        String url = "jdbc:postgresql://localhost:5432/contractormanagement";
//        String url = "jdbc:postgresql://108.170.53.74:5432/contractormanagementmay05";
        //       String url = "jdbc:postgresql://localhost:5432/snl_contractormanagement";//
//     
        //     String url = "jdbc:postgresql://localhost:5432/snl_contractormanagement_a";//
        //  String url = "jdbc:postgresql://108.170.53.66:5432/snl_contractormanagement";//
        //  String url = "jdbc:postgresql://localhost:5432/snl_contractormanagement_16_jan_2019r";//
        //  String url = "jdbc:postgresql://108.170.53.66:5432/snl_contractormanagement";//
        //   String url = "jdbc:postgresql://localhost:5432/snl_contractormanagement";
//        String url = "jdbc:postgresql://localhost:5432/contractormanagementmay05";
//        String url="jdsbc:postgresql://localhost:5432/contractmanagement_today";
//        String url = "jdbc:postgresql://localhost:5432/kotekarnov14";
//        String url = "jdbc:postgresql://108.170.53.66:5432/contractormanagementdee";
//        String url = "jdbc:postgresql:••••••••••//108.170.53.74:5432/kotekarnov14";
//        String url = "jdbc:postgresql://108.170.53.74:5432/kotekar5feb2018";
//        String url = "jdbc:postgresql://localhost:5432/kotekar5feb2018";
//        VITLA
//        String url = "jdbc:postgresql://108.170.53.74:5432/vitlacms_trasaction";
//        String url = "jdbc:postgresql://localhost:5432/vitlacms_trasaction";
//        String url = "jdbc:postgresql://localhost:5432/bantwalaexcelimport_74";
//        String url = "jdbc:postgresql://localhost:5432/bantwalatest_74";
//        String url = "jdbc:postgresql://108.170.53.66:5432/bantwala_74";
//        String url = "jdbc:postgresql://108.170.53.74:5432/bantwala";
//        String url = "jdbc:postgresql://108.170.53.74:5432/bantwala";
//        String url = "jdbc:postgresql://localhost:5432/bantwala_74";
//        String url = "jdbc:postgresql://108.170.53.74:5432/bantwalatest1";
//        String url = "jdbc:postgresql://108.170.53.74:5432/bantwalaexcelimport";
//        String url = "jdbc:postgresql://localhost:5432/bantwalaexcelimport";
//        String url = "jdbc:postgresql://localhost:5432/gescomstoresurvey";
//        String url = "jdbc:postgresql://108.170.53.66:5432/contractormanagementdee";
//        String url = "jdbc:postgresql://localhost:5432/contractormanagementdee";
        // String url = "jdbc:postgresql://184.95.45.74:5432/snl_contractormanagement_a";//
        //  String url = "jdbc:postgresql://199.241.138.185:5432/styra_beta";
        //  String url = "jdbc:postgresql://localhost:5432/styra_beta";
//        String url = "jdbc:postgresql://198.24.159.114:5432/styra_prod_mis";
        String url = "jdbc:postgresql://198.24.159.114:5432/styra_prod";
        //  String url = "jdbc:postgresql://localhost:5432/styra_prod";

        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {
            //   con = DriverManager.getConnection(url, "preetham", "6H9tFHfbi3DmQZ");
            //   con = DriverManager.getConnection(url, "pguser", "AvWHrzUbg6");
            //      connect = DriverManager.getConnection(url, "pguser", "y75+D>?nYSHJT#UE");
            //Intellsii prod
            url = "jdbc:postgresql://10.48.138.130:5432/styraiipl_apdcl_prod";
            connect = DriverManager.getConnection(url, "styra_prod", "5~y-g&C?7X>KuJc");
//
//            url = "jdbc:postgresql://10.48.137.132:5433/styraiipl_apdcl_test";
//            connect = DriverManager.getConnection(url, "styra_test", "IyLWpRnj");
        }
        // Connection connect = DriverManager.getConnection(url, "postgres", "admin@123");
        return connect;
    }

}

