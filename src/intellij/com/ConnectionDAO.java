/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import java.sql.DriverManager;

/**
 *
 * @author bhuvaneshwari
 */
public class ConnectionDAO {

    public static java.sql.Connection getConnectionStyra() throws Exception {

//        return getConnectionStyra_Mvvnl_Test();
//         return getConnectionStyra_Polarise_Test();
//         
   // return getConnectionStyra_DGVCL();
   return getConnectionStyra_MVVNL_Prod();
//         return getConnectionStyra_PVVNL_prod();
       //  return getConnectionStyra_DGVClLIve();
//         return getConnectionStyra_MVVNL_Prod();

//   return getConnectionStyra_PVVNL() ;

//return getConnectionStyra_Polarise_prod();

    }

    public static java.sql.Connection getConnectionStyra_Mvvnl_Test() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //MGVCL T&D
            url = "jdbc:postgresql://172.31.5.109:5432/styra_mvvnl_tnd";
            connect = DriverManager.getConnection(url, "styra_mvvnl", "WuTi3r18<44%9Ut");
        }

        return connect;
    }

    public static java.sql.Connection getConnectionStyra_MVVNL_Prod() throws Exception {        
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //DGVCL T&D
            url = "jdbc:postgresql://172.31.1.119:5432/styraiipl_mvvnl_prod";
            connect = DriverManager.getConnection(url, "styramvvnl_prod", "#92Fq6s3KqMm6*3");

        }

        return connect;
    }
    
        public static java.sql.Connection getConnectionStyra_Polarise_Test() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //MGVCL T&D
            url = "jdbc:postgresql://65.0.133.210:5432/styra_polaris_test";
            connect = DriverManager.getConnection(url, "styra_polaris", "nWtLp9&5>k21");
        }

        return connect;
    }
        
           public static java.sql.Connection getConnectionStyra_PVVNL_prod() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //MGVCL T&D
            url = "jdbc:postgresql://172.21.33.3:5432/styraiipl_pvvnl_prod";
            connect = DriverManager.getConnection(url, "styrapvvnl_prod", "63aI-l.Q8+eN");

        }

        return connect;
    }
           
           
    public static java.sql.Connection getConnectionStyra_PVVNL() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //MGVCL T&D
            url = "jdbc:postgresql://172.22.33.3:5432/styraiipl_pvvnl_test";
            connect = DriverManager.getConnection(url, "styra_pvvnl", "Zgx61:I?&u2H");

        }

        return connect;
    }
    
    public static java.sql.Connection getConnectionStyra_DGVCL() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //DGVCL T&D
            url = "jdbc:postgresql://172.28.49.15:5432/styra_dgvcl_test";
            connect = DriverManager.getConnection(url, "styra_dgvcl", "5£8M5D%6#!xT");

        }

        return connect;
    }
  
    public static java.sql.Connection getConnectionStyra_DGVClLIve() throws Exception {
        java.sql.Connection connect = null;

        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if (connect == null) {

            //DGVCL T&D
            url = "jdbc:postgresql://172.28.66.15:5432/styraiipl_dgvcl_prod";
            connect = DriverManager.getConnection(url, "styra_dgvclprod", "Pc#7wii+X8m6");
        }

        return connect;
    }
    
    public static java.sql.Connection getConnectionStyra_MVVNL() throws Exception {
        java.sql.Connection connect = null;
        
        String url = "";
        //demo
        Class.forName("org.postgresql.Driver").newInstance();
        if(connect == null){
            
            //MVVNL T&D
            url = "jdbc:postgresql://172.31.5.109/styra_mvvnl_tnd";
            connect = DriverManager.getConnection(url, "styra_mvvnl", "WuTi3r18<44%9Ut");
            
        }
        return connect;
    }

}
