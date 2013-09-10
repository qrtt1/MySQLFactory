package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;

import net.qty.db.mysql.util.SqlHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLInstanceTest {

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
    public void testRunSqlScript() throws Exception {
        MySQLInstance instance = manager.createDatabase();
        final String dbName = "test_" + ((int) (Math.random() * 1000));
        instance.runSqlScript("CREATE DATABASE " + dbName + ";");

        SqlHelper result = new SqlHelper(instance.getBaseConnectionUrl(), "root", instance.getPassword());
        verifyFoundCreatedDatabase(result.exeucteQuery("SHOW DATABASES;"), dbName);
    }

    protected void verifyFoundCreatedDatabase(List<Map<String, Object>> results, String dbName) {
        boolean found = false;
        for (Map<String, Object> map : results) {
            if (dbName.equals(map.values().toArray()[0])) {
                found = true;
                break;
            }
        }

        assertTrue(found);
    }

}
