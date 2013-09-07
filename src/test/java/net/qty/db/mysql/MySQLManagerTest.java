package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLManagerTest {

    private static final File MYSQL_SOFTWARE_PATH = new File("/opt/mysql");
    
    static Log logger = LogFactory.getLog(MySQLManagerTest.class);

    MySQLManager manager;

    @Before
    public void setUp() {
        manager = new MySQLManager(MYSQL_SOFTWARE_PATH);
    }

    @After
    public void tearDown() {
        manager.close();
    }

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
