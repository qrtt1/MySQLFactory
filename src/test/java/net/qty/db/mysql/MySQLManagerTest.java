package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.Test;

public class MySQLManagerTest extends AbsMysqlFactoryTestCase {

    @Test
    public void testCreateMySQLInstance() throws Exception {
        MySQLInstance instance = manager.createDatabase();
        assertTrue(isPassJdbcTest(instance));
    }

    private boolean isPassJdbcTest(MySQLInstance instance) throws Exception {
        Connection connection = DriverManager.getConnection(instance.getBaseConnectionUrl());
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT NOW()");
            resultSet.next();
            Timestamp now = resultSet.getTimestamp(1);
            logger.info("timestamp: " + now);
        } catch (Exception e) {
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return true;
    }

}
