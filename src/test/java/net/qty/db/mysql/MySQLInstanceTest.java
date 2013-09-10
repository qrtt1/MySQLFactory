package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import net.qty.db.mysql.util.SqlHelper;

import org.junit.Test;

public class MySQLInstanceTest extends AbsMysqlFactoryTestCase {

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
