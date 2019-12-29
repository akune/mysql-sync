package de.kune.tunnel;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import de.kune.mysqlsync.SynchronizerCli;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Logger;

public class SshTunnel implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(SshTunnel.class.getName());

    private static final String STRICT_HOST_KEY_CHECKING_KEY = "StrictHostKeyChecking";
    private static final String STRICT_HOST_KEY_CHECKING_VALUE = "no";
    private static final String CHANNEL_TYPE = "shell";
    private int forwardedPort;

    private Session session;

    public SshTunnel(String user, String host, int port, String targetHost, int targetPort, File privateKeyFile) throws IOException {
        this(user, host, port, targetHost, targetPort, Files.readAllBytes(privateKeyFile.toPath()));
    }

    public SshTunnel(String user, String host, int port, String targetHost, int targetPort, byte[] privateKey) {
        JSch jsch = new JSch();
        try {
            jsch.addIdentity(user, privateKey, (byte[])null, (byte[])null);
            session = jsch.getSession(user, host, port);
            jsch.setConfig(STRICT_HOST_KEY_CHECKING_KEY, STRICT_HOST_KEY_CHECKING_VALUE);
            forwardedPort = session.setPortForwardingL(0, targetHost, targetPort);
            LOGGER.info("Tunneling localhost:" + forwardedPort + " -> " + host + ":" + port + " -> " + targetHost + ":" + targetPort);
            session.connect();
            LOGGER.info("Connection state: " + session.isConnected());
            Channel channel = session.openChannel(CHANNEL_TYPE);
            channel.connect();
        } catch (JSchException e) {
            throw new RuntimeException(e);
        }
    }

    public int getForwardedPort() {
        return forwardedPort;
    }

    public boolean isConnected() {

        return session.isConnected();
    }

    @Override
    public void close() {
        if (session.isConnected()) {
            LOGGER.info("disconnecting");
            session.disconnect();
        }
    }

    public static void main(String[] args) {
        try (SshTunnel tunnel = new SshTunnel("ansible", "job1.prod.report.deposit", 22, "localhost", 3306, new File("./id_rsa"))) {
            System.out.println(tunnel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
