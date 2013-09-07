package net.qty.db.mysql;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExternalApplicationInvoker {

    static Log logger = LogFactory.getLog(ExternalApplicationInvoker.class);
    private File workingPath;

    public ExternalApplicationInvoker(File workingPath) {
        this.workingPath = workingPath;
    }

    public void detachableInvoke(File location, String... args) {
        ArrayList<String> command = new ArrayList<String>();
        command.add(location.getAbsolutePath());
        command.addAll(Arrays.asList(args));
        StringBuilder sb = new StringBuilder();
        for (String s : command) {
            sb.append(s).append(" ");
        }
        sb.append("&");

        try {
            delegateInvoke(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void delegateInvoke(String content) throws Exception {
        /* wrap the non-terminated external application in the script and run in the background */
        File script = File.createTempFile(ExternalApplicationInvoker.class.getSimpleName(), ".sh");
        FileWriter writer = new FileWriter(script);
        writer.write(content);
        writer.close();
        
        logger.info("execute in script: " + content);
        invoke(new File("/bin/sh"), script.getAbsolutePath());
    }

    public int invoke(File location, String... args) {
        ArrayList<String> command = new ArrayList<String>();
        command.add(location.getAbsolutePath());
        command.addAll(Arrays.asList(args));

        logger.info("start to invoke the command: " + command);
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(workingPath);
        return execute(builder);
    }

    protected int execute(ProcessBuilder builder) {
        try {
            Process process = builder.start();
            int returnCode = process.waitFor();
            showApplicationResponse(process);
            return returnCode;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private void showApplicationResponse(Process process) throws Exception {
        showContent("[STDERR] ", process.getErrorStream());
        showContent("[STDOUT] ", process.getInputStream());
    }

    private void showContent(String label, InputStream in) throws Exception {
        if (in.available() == 0) {
            return;
        }

        ByteArrayOutputStream rawContent = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        while (true) {
            int count = in.read(buf);
            if (count == -1) {
                break;
            }
            rawContent.write(buf, 0, count);
        }

        String content = new String(rawContent.toByteArray());
        if (content.length() > 0) {
            logger.info(label + content);
        }
    }



}
