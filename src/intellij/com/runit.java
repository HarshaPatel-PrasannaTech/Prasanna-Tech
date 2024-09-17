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
/**
 *
 * @author bhuvaneshwari
 */
public class runit {
    public static void main(String[] args) throws Exception {
        runProd("1");
        if (args.length > 0) {
            //runProd(args[0]);
            //  runProd(args[0]);
        } else {
            System.out.println("Pass params either 1 or 2");
        }
    }

    public static void runProd(String type) throws Exception {

        String url = "https://portal.apdcl.co:8015/api/Info/Purbanchalsaveconsumerinformation";
        String authString = "ApiUser" + ":" + "ApiPass";
        preparedata(url, authString, type);

    }

    public static void runTEST(String type) throws Exception {

        String url = "http://wss1.rajdiscoms.com/SmartMeteringServices/api/CRMSmartMeteringIntegration/PullMCOData_single";
        String authString = "hcliusr" + ":" + "HCLI@2020";
        preparedata(url, authString, type);
    }

    public static void preparedata(String url, String authString, String type) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        System.out.println("Base64 encoded auth string: " + authStringEnc);
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        String input = "{\"K_NO\":120112015794,\"Meter_Type\":\"2\",\"SERIAL_NO\":\"8027771\",\"METER_MAKE_ID\":\"1\",\"IS_PREPAID_METER\":\"false\",\"OLD_METER_STATUS_CODE\":\"OK\",\"METER_STATUS_CODE\":\"OK\",\"METER_PHASE\":\"2\",\"NO_DIGITS\":\"6\",\"VECTOR_TYPE\":\"A\",\"AMP_RATING\":\"10-60\",\"INSTALLATION_DATE\":\"2020-10-12\",\"RENT_CODE\":\"02\",\"OLD_SERIAL_NO\":\"5528822\",\"NUMERATORMF\":\"1\",\"DENOMINATORMF\":\"1\",\"ACCURACY_CLASS\":\"0.01s\",\"METERING_VOLTAGE_ID\":\"1\",\"SUPPLY_TYPE\":\"2\",\"BLOCK_SUPPLY_TYPE\":\"2\",\"OldMeter_PREV_READING_DATE\":\"06-Aug-2020\",\"OldMeter_CURRENT_READING_DATE\":\"12-Oct-2020\",\"OldMeter_PREVIOUS_KWH\":\"11\",\"OldMeter_PREVIOUS_KVAH\":\"22\",\"OldMeter_PREVIOUS_KVA\":\"23\",\"OldMeter_CURRENT_KWH\":\"30999\",\"OldMeter_CURRENT_KVAH\":\"00\",\"OldMeter_CURRENT_KVA\":\"00\",\"OldMeter_PF\":\"23\",\"OldMeter_NET_CONS\":\"233\",\"OldMeter_NET_MDI\":\"2332\",\"NewMeter_PREV_READING_DATE\":\"12-Oct-2020\",\"NewMeter_CURRENT_READING_DATE\":\"12-Oct-2020\",\"NewMeter_PREVIOUS_KWH\":\"0\",\"NewMeter_PREVIOUS_KVAH\":\"0\",\"NewMeter_PREVIOUS_KVA\":\"0\",\"NewMeter_CURRENT_KWH\":\"0\",\"NewMeter_CURRENT_KVAH\":\"0\",\"NewMeter_CURRENT_KVA\":\"0\",\"NewMeter_PF\":\"0\",\"NewMeter_NET_CONS\":\"0\",\"NewMeter_NET_MDI\":\"0\",\"BODY_SEAL_NO\":\"GP8027771/O169287/O193277\",\"TERMINAL_SEAL_NO\":\"GP8027771\",\"TERMINAL_SEAL_STATUS\":\"GP8027771/O169287/O193277\",\"TENDER_NO\":\"TN-IT-28\",\"FINALIZATION_DATE\":\"12-Oct-2020\",\"ORDER_DATE\":\"2021-08-02\",\"FINANCIAL_YEAR\":\"20-21\",\"OFFICE_ID\":\"3202230\",\"CURRENT_STATUS\":\"R\",\"MF\":\"1\",\"TIME_STAMP\":\"2021-08-06 20:00\"}";
        int counter = 0;
        try {
            con = getESddscConnectionCMS();
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
            query = "WITH MIDATA AS (\n"
                    + "select _16,_62,_26,_15,a.latitude,_10,to_date(a.surveydate,'dd/MM/yyyy')||' ' || a.surveytime midatdetime ,distributionnodename,distributionnodeid,a.distributionnodecode\n"
                    + ",_57,_75,_67,_68,_98,_70,_71,_19,_21,_60,_63,_65,_17 ,_18,a.longitude,_113 ,_200,_201,_202,_5 ,_58,_212\n"
                    + "from etl_midata a\n"
                    + "where responsestatusid=1 and distributionnodeid not in(select distributionnodeid from intellismart_mcologs where a.distributionnodeid=distributionnodeid and status='Success') )\n"
                    + ",masterdata as (\n"
                    + "select distinct  a.distributionnodeid,_80,_81 from etl_masterdata a inner join MIDATA b on a.distributionnodeid=b.distributionnodeid )\n"
                    + "select  a.distributionnodeid, coalesce(_16::jsonb->>'mobile_number',_212::jsonb->>'mobile_number')  \"Mobileno\",5903 \"Login_TblRefID\" ,_62 newsr,(case when lower(_26)='na' then '' else coalesce(_26,'') end)\"Prepaidbalance\"\n"
                    + ",_15 Address,a.latitude,coalesce(_80,'') \"INSTALLATION_NO\",(case _10 when 'Postpaid' then '3' when 'Prepaid' then '2' else _10 end)::int4 \"Payment_TblRefID\"\n"
                    + ",midatdetime \"installtion_datetime\",(case when coalesce(_57,'null')='null' then _58 else _57::jsonb->>'meter_Id' end) \"New_serial_Number\",5 \"Circuit_TblRefId\",_75 \"Remark\",_67 \"Sealno_3\",_68 \"Sealno_4\",coalesce(c.distributionnodecode,_5) \"MRU\"\n"
                    + ",_70 \"Sealno_1\",coalesce(_71,'') Sealno_2,replace(coalesce(_21,'0'),'na','0')::decimal \"LOAD_UNIT\",_19 \"CONS_NAME\",0 \"poleno\",1.0 \"Appversionno\",_200 \"Sealno_5\",1 \"FeederId\",1 \"DTRId\",coalesce(_21,'0') \"CONN_LOAD\"\n"
                    + ", (case when coalesce(replace(_57::jsonb->>'meter_Phase','1Ph','1 PH'),'null')='null' or coalesce(replace(_57::jsonb->>'meter_Phase','3Ph','3 PH'),'null')='null' then replace(_60,'-PH',' PH') \n"
                    + "   else replace(_57::jsonb->>'meter_Phase','Ph',' PH') end) \"phase\",(case when _63='' then '0' else coalesce(_63,'0') end)::int4 \"kvah\",coalesce(c.distributionnodecode,_5) \"DTR_Name\",1 \"MeterStatus_tblrefid\",(case when _65='' then '0' else coalesce(_65,'0') end)::int4  \"meter_MF\"\n"
                    + ",coalesce(_80,'') \"OLDrrnumber\",coalesce(_21,'0') \"Sanction_load\","
                    + " (case (case when coalesce(replace(_57::jsonb->>'meter_Phase','1Ph','1 PH'),'null')='null' or coalesce(replace(_57::jsonb->>'meter_Phase','3Ph','3 PH'),'null')='null' then replace(_60,'-PH',' PH') \n"
                    + "   else replace(_57::jsonb->>'meter_Phase','Ph',' PH') end)\n"
                    + "when '1 PH' then '1' when '3 PH WC' then '2' when 'LTCT' then '3' else '0' end)::int4 "
                    + "\"ServicePointMeterPhase_TblRefID\"\n"
                    + ",(case when _201='na' then '0' else coalesce(_201,'0') end)::int4 \"MDKW\" ,coalesce(_17,'') \"emailID\",a.distributionnodename \"OldmeterSR\"\n"
                    + ",replace(coalesce(_81,''),'- DOMESTIC-A','') \"ConnectionCategory_TblRefID\",a.distributionnodecode \"RRNumber\",a.longitude ,(case when _81='48' then 'LT' when _81='49' then 'H_PWWS_N' when _81='74' then 'LT_DOM_A' when _81='50' then 'H_TCRS_T'\n"
                    + "else replace(coalesce(_81,''),'- DOMESTIC-A','') end) \"TARIFF\""
                    + ",(case when _202='na' then '0' else _202 end)::int4  \"PF\"\n"
                    + ",1 \"CTratio_TblRefID\",coalesce(_113,'0') \"oldfr\",coalesce(_80,'') \"BUSINESS_PARTNER\",(case when _10='Prepaid' then '2' else 'LG'  end ) \"wire\",'INTELLISMART' \"company\"\n"
                    + "from MIDATA a\n"
                    + "left join tbldistributionnodes b on a.distributionnodeid=b.distributionnodeid\n"
                    + "left join tbldistributionnodes c on c.distributionnodeid=b.underdistributionnodeid\n"
                    + "left join masterdata d on a.distributionnodeid=d.distributionnodeid "
                    + "where a.distributionnodeid in (109321,111101,111187,111515,117215,119452,119900,120542,120549,120568,120827,120968,121636,121670,121790,122016,122493,123255,124034,307997,309946,310707,310903,311588,312135,312406,313996,314826,315891,317224,317918,320000,320852,322206,323032,323410,324415,325141,325149,326189,327628,328038,328607,329021,329197,372978,373256,373312,373612,373838,374076,374132,374273,374450,375720,375737,375989,376532,377460,377877,378658,378659,379151,379335,379720,379815,380078,380432,380755,380970,381287,382064,382719,382901,383018,383314,383326,317359,335179,382644,381519,321721,381945,378048,321841,311075,313345,310755,324131,380135,121208,376117,122169,317497,122477,372543,325207,376555,122506,359620,312008,314143,321640,314893,333829,115769,118353,311955,122333,324506,317468,311832,122153,311160,313538,311975,314590,299880,115838,116464,309714,121884,325122,310339,120951,319683,122659,311067,116457,317545,381064,379403,122527,333997,310746,311765,116517,312408,122472,313662,309533,311035,311948,122440,311296,309493,317876,123159,124091,314929,320417,123227,309710,380534,122904,309742,311277,318841,383761,116031,120449,318652,374171,123244,122554,319838,319592,123277,313763,121107,119343,122513,120785,373208,118083,115767,122275,122982,122727,320626,313677,318491,122418,373052,121150,124163,309150,314522,123212,119916,120675,124110,373987,309518,319376,116360,120410,324066,322417,116342,123774,122345,124253,313774,312629,124107,312277,314227,123901,312942,116556,124025,115826,320027,378753,382135,376079,380749,121323,121595,380527,324697,317421,124215,118082,123802,315286,120691,315174,123829,121935,124019,316805,324699,310756,116019,380634,122288,122795,376951,314779,380277,324154,378558,324694,115742,116972,120394,309909,121934,318667,381404,123281,326188,115713,307977,118358,318196,122088,320504,121657,122479,120747,319812,124262,116554,124256,122747,321982,383328,122925,309200,309519,312478,313525,115984,122805,120225,116748,124219,383960,318900,122830,373578,309879,122724,119326,115715,312845,122674,123292,115704,378537,122093,122574,120177,122570,121882,122787,121906,122822,314729,116589,318011,116555,115882,373283,313667,123964,381526,122005,122419,122970,121729,122181,122748,116643,310186,310633,321149,119940,115770,116456,122782,308905,378022,381647,117955,321310,318650,383716,120173,310318,120087,374455,116040,121989,121045,373409,120348,374174,122648,321703,121766,115910,120175,117509,121894,312883,115949,334054,121909,115777,310263,115781,318392,116571,379961,376452,118133,121834,121600,321030,121389,317971,123973,115772,115768,115726,116439,379915,309109,115861,122007,116553,115980,120355,375383,381346,121249,120794,382940,120367,116180,120174,379608,123132,120854,121828,377187,379467,378304,115771,122945,381867,310336,374287,120676,116021,116299,124168,373383,372976,121847,122171,376046,119471,383908,120668,313983,123046,379415,373264,374198,101342,102246,124121,125170,125306,125688,126135,127317,127535,128251,128968,130453,130591,130771,131264,131278,133670,136809,136984,137509,138678,142228,176746,177080,177853,177910,178122,178317,178764,179446,180461,180654,181189,181309,181521,181559,181714,181746,181786,181853,181885,182240,182568,182730,182740,182754,229150,230152,230412,232197,232627,233082,233895,234496,234847,237397,238690,238826,241600,242298,242667,243700,244365,244482,246821,324492,337407,345064,349533,352305,354081,354311,356851,359354,365383,365555,366003,366640,367184,367393,370893,387598,387606,387614,387622,387660,387711,387719,387779,387784,387828,387842,387862,387919,387934,387940,387946,387951,387979,387982,387995,388068,388091,388144,388147,388212,388232,388266,388267,388273,388281,388288,388294,388319,388322,388330,388351,388390,388391,388404,388413,388417,388447,388480,388482,388525,388555,388572,388585,388590,388607,388616,388618,388629,388646,388648,388649,388694,388697,388700,388719,388726,388761,388814,388848,388917,388920,388960,388980,388987,388991,389036,389039,389093,389129,389138,389164,389236,389240,389273,389308,389319,389322,389324,389332,389405,389416,389440,389472,389478,389483,389495,389511,389560,389562,389576,389710,389741,389746,389754,389755,389764,389793,389814,389864,389948,389957,389962,389999,390055,390071,390085,390098,390099,390125,390147,390152,390155,390164,390198,390222,390244,390252,390264,390303,390311,390353,390363,390479,390538,390547,390552,390556,390573,390582,390666,390676,390678,390682,390689,390722,390753,390863,390865,390896,390903,390941,390976,391004,391005,391011,391075,391112,391113,391141,391154,391208,391209,391239,391266,391277,391286,391306,391315,391363,391427,391447,391511,391513,391552,391578,391582,391594,391605,391614,391639,391642,391649,391670,391705,391748,391766,391837,391865,391899,391965,391969,391986,392026,392050,392091,392104,392106,392130,392168,392243,392340,392347,392354,392371,392420,392434,392458,392484,392521,392539,392597,392605,392622,392642,392699,392739,392744,392752,392780,392818,392845,392868,392892,392898,392900,392918,392954,392970,392996,393039,393049,393050,393052,393064,393082,393090,393098,393105,393198,393200,393251,393298,393301,393310,393312,393320,393332,393374,393402,393406,393421,393461,393502,393529,393725,393818,393831,393888,393890,393907,393929,393950,393963,393972,394021,394033,394052,394082,394133,394134,394136,394177,394184,394211,394290,394292,394313,394319,394344,394373,394436,394450,394478,394525,394552,394561,394684,394695,394727,394742,394747,394748,394779,394836,394853,394900,394967,394974,395003,395006,395012,395060,395073,395093,395125,395126,395136,395144,395151,395167,395172,395176,395218,395233,395236,395311,395318,395346,395369,395437,395439,395451,395584,395594,395612,395620,395622,395623,395690,395700,395784,395852,395869,395902,395905,395916,395941,395942,395953,395974,395988,396025,396091,396123,396148,396150,396164,396186,396189,396243,396278,396285,396305,396330,396339,396368,396432,396569,396574,396676,396686,396719,396746,396758,396789,396837,396838,396839,396850,396897,396920,396927,396997,397017,397034,397084,397125,397169,397182,397205,397217,397227,397230,397231,397239,397245,397271,397277,397297,397319,397563)\n"
                    + ""
                    + ""
                    + "";
            if (type.equals("1")) {
                query = "select * from getMCODetails() ";
            } else if (type.equals("2")) {
                query = "select * from getsm_sm_details() ";
            }
            System.out.println(query);
            rs = stmt.executeQuery(query);

            String output = "{\"Status\":\"NOTHING\",\"Error_Message\":\"Nothing\"}\n"
                    + " ";

            while (rs.next()) {
                counter++;
                //  System.out.println("Processing.... : " + rs.getString("distributionnodeid"));

                boolean success = false;
                boolean exitprep = false;
                String err = "";
                String status = "";
                billing_bean bobj = new billing_bean();

                //  while (!success) {
                try {

                    bobj.Mobileno = rs.getString("Mobileno");
                    bobj.newsr = rs.getString("newsr");
                    bobj.Prepaidbalance = rs.getString("Prepaidbalance");
                    bobj.Address = rs.getString("address");
                    bobj.latitude = rs.getString("latitude");
                    bobj.INSTALLATION_NO = rs.getString("INSTALLATION_NO");
                    bobj.installtion_datetime = rs.getString("installtion_datetime");
                    bobj.New_serial_Number = rs.getString("New_serial_Number");
                    bobj.Remark = rs.getString("Remark");
                    bobj.Sealno_3 = rs.getString("Sealno_3");
                    bobj.Sealno_4 = rs.getString("Sealno_4");
                    bobj.MRU = rs.getString("MRU");
                    bobj.Sealno_1 = rs.getString("Sealno_1");
                    bobj.Sealno_2 = rs.getString("sealno_2");
                    bobj.CONS_NAME = rs.getString("CONS_NAME");
                    bobj.poleno = rs.getString("poleno");
                    bobj.Sealno_5 = rs.getString("Sealno_5");
                    bobj.CONN_LOAD = rs.getString("CONN_LOAD");
                    bobj.phase = rs.getString("phase");
                    bobj.DTR_Name = rs.getString("DTR_Name");
                    bobj.OLDrrnumber = rs.getString("OLDrrnumber");
                    bobj.Sanction_load = rs.getString("Sanction_load");
                    bobj.emailID = rs.getString("emailID");
                    bobj.OldmeterSR = rs.getString("OldmeterSR");
                    bobj.RRNumber = rs.getString("RRNumber");
                    bobj.logtiude = rs.getString("longitude");
                    bobj.TARIFF = rs.getString("TARIFF");
                    bobj.CTratio_TblRefID = rs.getString("CTratio_TblRefID");
                    bobj.oldfr = rs.getString("oldfr");
                    bobj.BUSINESS_PARTNER = rs.getString("BUSINESS_PARTNER");
                    bobj.wire = rs.getString("wire");
                    bobj.company = rs.getString("company");
                    try {
                        bobj.meter_MF = rs.getInt("meter_MF");
                        bobj.FeederId = rs.getInt("FeederId");
                        bobj.DTRId = rs.getInt("DTRId");
                        bobj.kvah = rs.getInt("kvah");
                        bobj.MeterStatus_tblrefid = rs.getInt("MeterStatus_tblrefid");
                        bobj.ServicePointMeterPhase_TblRefID = rs.getInt("ServicePointMeterPhase_TblRefID");
                        bobj.MDKW = rs.getInt("MDKW");
                        bobj.PF = rs.getInt("PF");
                        bobj.Appversionno = rs.getString("Appversionno");
                        bobj.Payment_TblRefID = rs.getInt("Payment_TblRefID");
                        bobj.LOAD_UNIT = rs.getInt("LOAD_UNIT");
                        bobj.ConnectionCategory_TblRefID = rs.getInt("ConnectionCategory_TblRefID");
                        bobj.Circuit_TblRefId = rs.getInt("Circuit_TblRefId");
                        bobj.Login_TblRefID = rs.getInt("Login_TblRefID");
                    } catch (Exception e) {
                        status = "Fail";
                        err = "Passed " + e.toString() + " --  Serial No is not an Integer";
                        pushlog(status, err, rs.getString("distributionnodeid"), input, 1, rs.getInt("sequence_no"));
                        success = true;
                    }

                    input = new Gson().toJson(bobj).toString();
                    System.out.println(input);
                    // only request update       updateLOG( rs.getString("distributionnodeid"), input);

                    if (!success) {
                        try {
                            task task1 = new task(bobj, rs.getString("distributionnodeid"), counter + "", Integer.parseInt(type), rs.getInt("sequence_no"));
                            executor.execute(task1);

                        } catch (Exception e) {
                            status = "fail";
                            err = "Passed " + bobj.INSTALLATION_NO + " --  API Intergration Failed";
                            pushlog(status, err, rs.getString("distributionnodeid"), input, 1, rs.getInt("sequence_no"));
                            success = true;
                        }
                    }
                } catch (Exception e) {
                    pushlog("Fail", "One or more request params are invalid ", rs.getString("distributionnodeid"), input, 1, rs.getInt("sequence_no"));
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

    public static void pushlog(String status, String err, String distnodeid, String request, int typeid, int sequence_no) throws Exception {
        try {
            err = err.replace("'", "");
            //System.out.println("+++++++++++++++++++++++ " + distnodeid);
            if (!(err.contains("Transaction count after EXECUTE indicates") || err.contains("ject reference not set to an instance of an o"))) {
                Connection con = getESddscConnectionCMS();
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

    public static void updateLOG(String distnodeid, String request) throws Exception {
        try {

            Connection con = getESddscConnectionCMS();
            String query = "update bosch_mcologs set request='" + request + "' ,response='updated' where distributionnodeid=" + distnodeid + " and status='Success' and request is null ";
            Statement stmt = con.createStatement();
            stmt.executeUpdate(query);
            con.close();

        } catch (Exception e) {
            throw e;
        }

    }

    public static Connection getESddscConnectionCMS() throws Exception {
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
//            url = "jdbc:postgresql://10.48.138.130:5432/styraiipl_apdcl_prod";
//            connect = DriverManager.getConnection(url, "styra_prod", "5~y-g&C?7X>KuJc");
            //   con = DriverManager.getConnection(url, "preetham", "6H9tFHfbi3DmQZ");
            //   con = DriverManager.getConnection(url, "pguser", "AvWHrzUbg6");
            //      connect = DriverManager.getConnection(url, "pguser", "y75+D>?nYSHJT#UE");
            //demo

            url = "jdbc:postgresql://10.48.137.132:5433/styraiipl_apdcl_test";
            connect = DriverManager.getConnection(url, "styra_test", "IyLWpRnj");

        }
        // Connection connect = DriverManager.getConnection(url, "postgres", "admin@123");
        return connect;
    }

} 

