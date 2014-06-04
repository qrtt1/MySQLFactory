package net.qty.db.mysql;

import static org.junit.Assert.*;

import java.sql.Timestamp;
import java.util.Map;

import net.qty.db.mysql.util.SqlHelper;

import org.junit.Test;

public class MySQLManagerTest extends AbsMysqlFactoryTestCase {

    @Test
    public void testCreateMySQLInstance() throws Exception {
        MySQLInstance instance = manager.createDatabase(null);

        SqlHelper helper = new SqlHelper(instance.getBaseConnectionUrl(), "root", instance.getPassword());
        Map<String, Object> map = helper.exeucteOneResultQuery("SELECT NOW() AS WHAT_TIME_IS_IT");

        logger.info(map);
        assertEquals("WHAT_TIME_IS_IT", map.keySet().toArray()[0]);
        assertTrue(map.values().toArray()[0] instanceof Timestamp);
    }

}
