package de.kune.mysqlsync;

import com.mysql.cj.jdbc.MysqlDataSource;
import de.kune.mysqlsync.anonymizer.FieldAnonymizer;
import org.apache.commons.cli.*;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class SynchronizerCli {
    private static final Logger LOGGER = Logger.getLogger(SynchronizerCli.class.getName());
    public static final String DEFAULT_MAX_CHUNK_SIZE = "500000";

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option hostname = new Option("h", "hostname", true, "database hostname");
        hostname.setRequired(true);
        options.addOption(hostname);

        Option port = new Option("P", "port", true, "database port");
        options.addOption(port);

        Option user = new Option("u", "user", true, "database user");
        user.setRequired(true);
        options.addOption(user);

        Option password = new Option("p", "password", true,"database password");
        password.setRequired(true);
        options.addOption(password);

        Option targetHostname = new Option("th", "target-hostname", true, "target database hostname");
        options.addOption(targetHostname);

        Option targetPort = new Option("tP", "target-port", true, "target database port");
        options.addOption(targetPort);

        Option targetUser = new Option("tu", "target-user", true, "target database user");
        options.addOption(targetUser);

        Option targetPassword = new Option("tp", "target-password", true,"target database password");
        options.addOption(targetPassword);

        Option source = new Option("s", "source", true, "source database");
        source.setRequired(true);
        options.addOption(source);

        Option target = new Option("t", "target", true, "target database");
        target.setRequired(true);
        options.addOption(target);

        Option dryRun = new Option("d", "dry-run", false, "do not perform any changes to the target database");
        options.addOption(dryRun);

        Option incremental = new Option("i", "incremental", false, "perform an incremental update based on creation and last modified dates");
        options.addOption(incremental);

        Option outputFile = new Option("o", "output-file", true, "the path or filename to write synchronization statements to");
        options.addOption(outputFile);

        Option maxRowsPerChunk = new Option("r", "max-rows-per-chunk", true, "the max number of rows to be retrieved from DB in one chunk");
        options.addOption(maxRowsPerChunk);
        
        Option anonymize = new Option("a", "anonymize", false, "anonymize personal data");
        options.addOption(anonymize);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            String sourceUrl = "jdbc:mysql://"+cmd.getOptionValue(hostname.getOpt())+":"+cmd.getOptionValue(port.getOpt(), "3306")+"?useUnicode=true&characterEncoding=utf-8&verifyServerCertificate=false&useSSL=false&requireSSL=false";
            String sourceUser = cmd.getOptionValue(user.getOpt());
            String sourcePassword = cmd.getOptionValue(password.getOpt());
            String targetUrl = "jdbc:mysql://"+cmd.getOptionValue(targetHostname.getOpt(), cmd.getOptionValue(hostname.getOpt()))+":"+cmd.getOptionValue(targetPort.getOpt(), cmd.getOptionValue(port.getOpt(), "3306"))+"?useUnicode=true&characterEncoding=utf-8&verifyServerCertificate=false&useSSL=false&requireSSL=false";
            String tUser = cmd.getOptionValue(targetUser.getOpt(), cmd.getOptionValue(user.getOpt()));
            String tPassword = cmd.getOptionValue(targetPassword.getOpt(), cmd.getOptionValue(password.getOpt()));
            DataSource dataSource = new MysqlDataSource();
            ((MysqlDataSource) dataSource).setUrl(sourceUrl);
            ((MysqlDataSource) dataSource).setUser(sourceUser);
            ((MysqlDataSource) dataSource).setPassword(sourcePassword);
            String sourceSchema = cmd.getOptionValue(source.getOpt());
            String targetSchema = cmd.getOptionValue(target.getOpt());
            boolean isDryRun = cmd.hasOption(dryRun.getOpt());
            boolean isIncremental = cmd.hasOption(incremental.getOpt());
            String outputFileName = cmd.getOptionValue(outputFile.getOpt());
            if (outputFileName != null && sourceUrl.equals(targetUrl) && user.equals(tUser) && password.equals(tPassword)) {
                new DatabaseSynchronizer(dataSource).sync(sourceSchema, targetSchema, isDryRun);
            } else {
                DataSource targetDataSource = new MysqlDataSource();
                ((MysqlDataSource) targetDataSource).setUrl(targetUrl);
                ((MysqlDataSource) targetDataSource).setUser(tUser);
                ((MysqlDataSource) targetDataSource).setPassword(tPassword);
                Map<Pattern, FieldAnonymizer> anonymizers = cmd.hasOption(anonymize.getOpt()) ? FieldAnonymizer.DEFAULT_ANONYMIZERS : Collections.emptyMap();
                new DataSourceSynchronizer(dataSource, targetDataSource, anonymizers).sync(sourceSchema, targetSchema, outputFileName,
                        isDryRun, isIncremental, Integer.valueOf(cmd.getOptionValue(maxRowsPerChunk.getOpt(), DEFAULT_MAX_CHUNK_SIZE)));
            }
        } catch (ParseException e) {
            LOGGER.severe(e.getMessage());
            formatter.printHelp("Database Synchronizer", options);

            System.exit(1);
        }

    }
}
