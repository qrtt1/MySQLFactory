package net.qty.db.mysql.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SqlHelper {

    static Log logger = LogFactory.getLog(SqlHelper.class);
    private String connectionUrl;
    private String username;
    private String password;

    public SqlHelper(String connectionUrl, String username, String password) {
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
    }

    public ArrayList<Map<String, Object>> exeucteQuery(String sql) throws Exception {
        Connection connection = DriverManager.getConnection(connectionUrl, username, password);
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            return makeResult(resultSet);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private ArrayList<Map<String, Object>> makeResult(ResultSet resultSet) throws SQLException {
        ArrayList<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        final int columns = metaData.getColumnCount();
        while (resultSet.next()) {
            HashMap<String, Object> map = new HashMap<String, Object>();
            for (int i = 1; i <= columns; i++) {
                map.put(metaData.getColumnLabel(i), resultSet.getObject(i));
            }
            results.add(map);
        }
        return results;
    }

}
