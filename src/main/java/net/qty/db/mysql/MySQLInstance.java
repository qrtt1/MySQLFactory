package net.qty.db.mysql;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class MySQLInstance {

    private MySQLManager manager;
    private File datadir;
    private int port;

    public MySQLInstance(MySQLManager manager, File datadir) {
        this.manager = manager;
        this.datadir = datadir;
        port = availablePort();
    }

    private int availablePort() {
        int port = 1024 + (int) (Math.random() * 10000);
        if (!isInUsedPort(port)) {
            return port;
        }
        return availablePort();
    }

    private boolean isInUsedPort(int uncheckPort) {
        try {
            ServerSocket server = new ServerSocket(uncheckPort);
            server.close();
        } catch (Exception e) {
            return true;
        }
        return false;
    }

    public String[] getLaunchArgs() {
        return new String[] { 
                String.format("--datadir=%s", datadir.getAbsoluteFile()),
                String.format("--port=%d", getPort()),
                String.format("--socket=%s", getSockFile())
                };
    }

    private int getPort() {
        return port;
    }

    private String getSockFile() {
        return new File(datadir, "my.sock").getAbsolutePath();
    }
    
    public void waitForDatabaseStarted() {
        try {
            int count = 15;
            while (count-- > 0) {
                if (!testConnect()) {
                    Thread.sleep(1000);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Waiting for database timed out.", e);
        }
    }

    protected boolean testConnect() throws IOException {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(getPort()));
            socket.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void shutdown() {
        manager.shutdownBySocketFile(getSockFile());
    }

    public String getBaseConnectionUrl() {
        return String.format("jdbc:mysql://127.0.0.1:%d", getPort());
    }

}
