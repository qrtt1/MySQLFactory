package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import net.qty.db.mysql.util.SqlHelper;

import org.junit.Test;

public class MySQLInstanceTest extends AbsMysqlFactoryTestCase {

    @Test
    public void testRunSqlScript() throws Exception {
        MySQLInstance instance = manager.createDatabase(null);
        final String dbName = "test_" + ((int) (Math.random() * 1000));
        instance.createDatabase(dbName);

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
    
    @Test
    public void testDefaultDatabase() throws Exception {
        MySQLInstance instance = manager.createDatabase(null);
        instance.setDefaultDatabase("test");
        String tableName = "table_" + System.currentTimeMillis();
        instance.runSqlScript("create table " + tableName + "(`column1` int);");
        
        SqlHelper helper = new SqlHelper(instance.getBaseConnectionUrl() + "/test", "root", instance.getPassword());
        String resultTable = (String) helper.exeucteOneResultQuery("show tables").values().iterator().next();
        assertEquals(tableName, resultTable);
    }

}
