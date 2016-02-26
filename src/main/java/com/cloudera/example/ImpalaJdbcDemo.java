package com.cloudera.example;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by charles on 1/4/16.
 */
public class ImpalaJdbcDemo {

    // here is an example query based on one of the Hue Beeswax sample tables
    private static final String SQL_STATEMENT = "SELECT phoneno FROM emay_data.parquet_hbase_tags_1223 LIMIT 10";

    private static final String IMPALAD_HOST = "edc4";

    private static final String IMPALAD_JDBC_PORT = "21050";

    private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/emay_data;auth=noSasl";

    public static final String IMPALA_CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/emay_data;auth=noSasl";

    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private static boolean checkTags(String tablename, List<String> tagColumns) {
        Connection con = null;
        List<String> tags = new ArrayList<String>();
        try {
            ResultSet rs = null;

            Class.forName(JDBC_DRIVER_NAME);
            con = DriverManager.getConnection(CONNECTION_URL);
            Statement stmt = con.createStatement();
            rs = stmt.executeQuery("describe " + tablename);

            while(rs.next()) {
                tags.add(rs.getString(1).toLowerCase());
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }

        for (String utf8tag : tagColumns) {
            if (!tags.contains(changeStrEncode(utf8tag).toLowerCase())) {
                System.out.println(utf8tag);
                return false;
            }
        }
        return true;
    }

	private static String changeStrEncode(String str) {
		// Example: 1个护化妆 <=> 1_E4_B8_AA_E6_8A_A4_E5_8C_96_E5_A6_86
		try {
			return URLEncoder.encode(str, "UTF-8").replaceAll("\\+", "SPACE").replaceAll("%", "_");
		} catch (UnsupportedEncodingException e ) {
			System.err.println("String format error");
			return "";
		}
	}


    public static void main(String[] args) {

        System.out.println("\n=============================================");
        System.out.println("Cloudera Impala JDBC Demo");
        System.out.println("Using Connection URL: " + CONNECTION_URL);
        System.out.println("Running Query: " + SQL_STATEMENT);

        /*
        Connection conn = null;

        try {

            Class.forName(JDBC_DRIVER_NAME);

            conn = DriverManager.getConnection(IMPALA_CONNECTION_URL);
            Statement stmt = conn.createStatement();

			//stmt.setPoolable(true);
            // boolean res = stmt.execute("CREATE TABLE IF NOT EXISTS tmp_segment_tb (id STRING) STORED AS PARQUET");
            // System.out.println("NUM: " + res);
            ResultSet ret = stmt.executeQuery("SELECT count(phoneno) FROM emay_data.parquet_hbase_test");
            ret.next();
            System.out.println("NUM: " + ret.getString(1));
            System.out.println("NUM: " + stmt.getWarnings());
			//stmt.executeUpdate("");

			//stmt.setPoolable(true);

            ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

            System.out.println("\n== Begin Query Results ======================");

            // print the results to the console
            while (rs.next()) {
                // the example query returns one String column
                System.out.println(rs.getString(1));
            }

            System.out.println("== End Query Results =======================\n\n");

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                // swallow
            }
        }
        */
        List<String> tags = new ArrayList<String>();
        tags.add("P网购达人");

        System.out.println(checkTags("emay_data.parquet_hbase_tags", tags));
    }
}
