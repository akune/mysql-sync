package de.kune.mysqlsync;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.kune.mysqlsync.DatabaseUtil.query;
import static de.kune.mysqlsync.DatabaseUtil.update;

public final class TestUtil {

    private TestUtil() {
    }

    public static DataSource dataSource(MySQLContainer dbContainer) {
        try {
            MysqlDataSource ds = new MysqlDataSource();
            ds.setServerName(dbContainer.getContainerIpAddress());
            ds.setPort(dbContainer.getMappedPort(3306));
            ds.setUser(dbContainer.getUsername());
            ds.setPassword(dbContainer.getPassword());
            ds.setDatabaseName(dbContainer.getDatabaseName());
            ds.setUseSSL(false);
            return ds;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void run(MySQLContainer database, String schema, String... scripts) throws SQLException {
        String sql = (Stream.of(scripts).map(script -> TestUtil.class.getClassLoader().getResource("scripts/" + script + ".sql")).map(u -> read(u)).map(s -> Arrays.stream(s.split("\n")).map(String::trim).collect(Collectors.joining(" "))).map(s -> s.replace(";", ";\n")).collect(Collectors.joining("\n")) + "\ncommit;").replace("\n\n", "\n");
        update(dataSource(database), sql);
    }

    public static void init(MySQLContainer database, String schema, String... scripts) throws SQLException {
        String sql = ("drop database if exists " + schema + ";\ncreate database " + schema + ";\nuse " + schema + ";\n" + Stream.of(scripts).map(script -> TestUtil.class.getClassLoader().getResource("scripts/" + script + ".sql")).map(u -> read(u)).map(s -> Arrays.stream(s.split("\n")).map(String::trim).collect(Collectors.joining(" "))).map(s -> s.replace(";", ";\n")).collect(Collectors.joining("\n")) + "\ncommit;").replace("\n\n", "\n");
        update(dataSource(database), sql);
    }

    public static List<Map<String, String>> queryAll(MySQLContainer db, String schema, String table) throws SQLException {
        return query(dataSource(db), "select * from " + schema + "." + table);
    }

    private static String read(URL url) {
        try {
            return IOUtils.toString(url, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
