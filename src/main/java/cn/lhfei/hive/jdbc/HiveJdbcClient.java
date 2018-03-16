package cn.lhfei.hive.jdbc;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by lhfei on 3/5/18.
 */
public class HiveJdbcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveJdbcClient.class);

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://master1.cloud.cn:10000/lhfei", "lhfei", "");
        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";

        stmt.execute("drop table " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");

        // show tables
        String sql = "show tables '" + tableName + "'";
        LOGGER.info("Running: {}", sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            LOGGER.info(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        LOGGER.info("Running: {}", sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            LOGGER.info(res.getString(1) + "\t" + res.getString(2));
        }

        sql = "select * from lhfei.bank limit 10";
        LOGGER.info("Running: {}", sql);
        res = stmt.executeQuery(sql);

        while (res.next()) {
            LOGGER.info("==== {}", res.getString(1));
        }

    }
}
