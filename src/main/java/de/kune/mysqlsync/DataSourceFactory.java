package de.kune.mysqlsync;

import com.mysql.cj.jdbc.MysqlDataSource;
import de.kune.tunnel.SshTunnel;
import org.w3c.dom.CDATASection;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

public abstract class DataSourceFactory implements AutoCloseable {

    public static TunneledDataSourceFactory tunneled() {
        return new TunneledDataSourceFactory();
    }

    public static SimpleDataSourceFactory simple() {
        return new SimpleDataSourceFactory();
    }

    private String hostname = "localhost";
    private int port = 3306;
    private String user = System.getProperty("user.name");
    private String password;

    public abstract DataSource build();

    public DataSourceFactory hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public DataSourceFactory port(int port) {
        this.port = port;
        return this;
    }

    public DataSourceFactory user(String user) {
        this.user = user;
        return this;
    }

    public DataSourceFactory password(String password) {
        this.password = password;
        return this;
    }

    @Override
    public abstract void close();

    static class SimpleDataSourceFactory extends DataSourceFactory {

        @Override
        public DataSource build() {
            String sourceUrl = "jdbc:mysql://"+super.hostname+":"+ super.port+"?useUnicode=true&characterEncoding=utf-8&verifyServerCertificate=false&useSSL=false&requireSSL=false";
            String sourceUser = super.user;
            String sourcePassword = super.password;
            DataSource dataSource = new MysqlDataSource();
            ((MysqlDataSource) dataSource).setUrl(sourceUrl);
            ((MysqlDataSource) dataSource).setUser(sourceUser);
            ((MysqlDataSource) dataSource).setPassword(sourcePassword);
            return dataSource;
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    static class TunneledDataSourceFactory extends DataSourceFactory {
        private String userAtJumpHost = System.getProperty("user.name");
        private String jumpHost;
        private int jumpHostPort = 22;
        private File identityFile = new File("~/.ssh/id_rsa");
        private SshTunnel tunnel;
        private String identityFilePassphrase;

        @Override
        public DataSource build() {
            try {
                this.tunnel = new SshTunnel(userAtJumpHost, jumpHost, jumpHostPort, super.hostname, super.port, identityFile, identityFilePassphrase);
                return DataSourceFactory.simple()
                        .user(super.user)
                        .password(super.password)
                        .hostname("localhost")
                        .port(tunnel.getForwardedPort())
                        .build();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (tunnel != null) {
                tunnel.close();
            }
        }

        public TunneledDataSourceFactory jumpHost(String jumpHost) {
            this.jumpHost = jumpHost;
            return this;
        }

        public TunneledDataSourceFactory userAtJumpHost(String userAtJumpHost) {
            this.userAtJumpHost = userAtJumpHost;
            return this;
        }

        public TunneledDataSourceFactory jumpHostPort(int jumpHostPort) {
            this.jumpHostPort = jumpHostPort;
            return this;
        }

        public TunneledDataSourceFactory identityFile(File identityFile) {
            this.identityFile = identityFile;
            return this;
        }

        public TunneledDataSourceFactory identityFilePassphrase(String identityFilePassphrase) {
            this.identityFilePassphrase = identityFilePassphrase;
            return this;
        }
    }

}
