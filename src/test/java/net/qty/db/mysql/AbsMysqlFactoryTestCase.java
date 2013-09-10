package net.qty.db.mysql;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

public class AbsMysqlFactoryTestCase {
    private static final File MYSQL_SOFTWARE_PATH = new File("/opt/mysql");

    protected static Log logger = LogFactory.getLog(MySQLManagerTest.class);

    protected MySQLManager manager;

    @Before
    public void setUp() {
        manager = new MySQLManager(MYSQL_SOFTWARE_PATH);
    }

    @After
    public void tearDown() {
        manager.close();
    }

}
