package net.qty.db.mysql;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MySQLManager {

    static Log logger = LogFactory.getLog(MySQLManager.class);

    public static final String MYSQL_INSTALL_SCRIPT = "scripts/mysql_install_db";
    public static final String MYSQL_MYSQLD_SAFE = "bin/mysqld_safe";
    public static final String MYSQL_MYSQLADMIN = "bin/mysqladmin";

    protected ExternalApplicationInvoker invoker;
    private File mysqlPath;
    private Set<MySQLInstance> instances = new HashSet<MySQLInstance>();

    public MySQLManager(File mysqlPath) {
        this.mysqlPath = mysqlPath;
        this.invoker = new ExternalApplicationInvoker(mysqlPath);
        
        validateOperatorSystem();
        validateMySQLTools();
    }

    private void validateMySQLTools() {
        checkExecutable(location(MYSQL_INSTALL_SCRIPT));
        checkExecutable(location(MYSQL_MYSQLD_SAFE));
        checkExecutable(location(MYSQL_MYSQLADMIN));
    }
    
    public void validateOperatorSystem() {
        if (System.getProperty("os.name") != null) {
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                throw new RuntimeException("not support the Windows");
            }
        }
    }

    private void checkExecutable(File path) {
        boolean executable = path.isFile() && path.canExecute();
        if (!executable) {
            throw new RuntimeException(String.format("Executable file not found at %s", path));
        }
    }

    protected File location(String executable) {
        return new File(mysqlPath, executable);
    }

    public MySQLInstance createDatabase() {
        File datadir = prepareDatadir();
        instalMySQLData(datadir);
        MySQLInstance instance = launchMySQLInstance(datadir);
        instances.add(instance);
        return instance;
    }

    private File prepareDatadir() {
        try {
            File dir = File.createTempFile(MySQLManager.class.getSimpleName(), "_datadir");
            dir.delete();
            dir.mkdirs();
            return dir;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void instalMySQLData(File datadir) {
        String dataDirArgs = String.format("--datadir=%s", datadir.getAbsoluteFile());
        invoker.invoke(location(MYSQL_INSTALL_SCRIPT), dataDirArgs);
    }

    private MySQLInstance launchMySQLInstance(File datadir) {
        MySQLInstance instance = new MySQLInstance(this, datadir);
        invoker.detachableInvoke(location(MYSQL_MYSQLD_SAFE), instance.getLaunchArgs());
        instance.waitForDatabaseReady();
        return instance;
    }

    public void close() {
        for (MySQLInstance instance : instances) {
            try {
                instance.shutdown();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
