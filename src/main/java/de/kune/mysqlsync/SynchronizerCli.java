package de.kune.mysqlsync;

import com.mysql.cj.jdbc.MysqlDataSource;
import de.kune.mysqlsync.anonymizer.FieldAnonymizer;
import org.apache.commons.cli.*;

import javax.sql.DataSource;
import java.io.File;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SynchronizerCli {
    private static final Logger LOGGER = Logger.getLogger(SynchronizerCli.class.getName());
    public static final String DEFAULT_MAX_CHUNK_SIZE = "500000";

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option hostname = new Option("h", "hostname", true, "database hostname");
        hostname.setRequired(true);
        options.addOption(hostname);

        Option port = new Option("P", "port", true, "database port");
        port.setType(Number.class);
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
        options.addOption(target);

        Option dryRun = new Option("d", "dry-run", false, "do not perform any changes to the target database");
        options.addOption(dryRun);

        Option incremental = new Option("i", "incremental", false, "perform an incremental update based on creation and last modified dates");
        options.addOption(incremental);

        Option outputFile = new Option("o", "output-file", true, "the path or filename to write synchronization statements to");
        options.addOption(outputFile);

        Option maxRowsPerChunk = new Option("r", "max-rows-per-chunk", true, "the max number of rows to be retrieved from DB in one chunk");
        options.addOption(maxRowsPerChunk);
        
        Option anonymize = new Option("a", "anonymize", true, "anonymize personal data");
        anonymize.setRequired(false);
        options.addOption(anonymize);

        Option compress = new Option("c", "compress", false, "compress output file (if specified)");
        options.addOption(compress);

        Option exclusion = new Option("x", "exclude", true, "exclude this pattern");
        options.addOption(exclusion);

        Option jumpHost = new Option("J", "jump-host", true, "the SSH user name and jump host to connect the source database");
        options.addOption(jumpHost);

        Option identityFile = new Option("I", "identity-file", true, "the SSH id_rsa file to authenticate with the jump host");
        options.addOption(identityFile);

        Option splitByTable = new Option("S", "split-by-table", false, "split by table");
        options.addOption(splitByTable);

        Option dropAndRecreateTables = new Option("D", "drop-and-recreate-tables", false, "drop and recreate tables");
        options.addOption(dropAndRecreateTables);

        Option allowParallel = new Option("mt", "multi-threaded", false, "allows to synchronize multiple tables in parallel if split-by-table was specified");
        options.addOption(allowParallel);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);

            DataSourceFactory sourceDSF = null;
            try {
                if (cmd.getOptionValue(jumpHost.getOpt()) != null) {
                    Pattern userPattern = Pattern.compile("^((.*?)@)?(.*?)(:(.*))?$");
                    Matcher userMatcher = userPattern.matcher(cmd.getOptionValue(jumpHost.getOpt()));
                    userMatcher.matches();
                    sourceDSF = DataSourceFactory.tunneled().jumpHost(userMatcher.group(3));
                    if (userMatcher.group(2) != null) {
                        ((DataSourceFactory.TunneledDataSourceFactory) sourceDSF).userAtJumpHost(userMatcher.group(2));
                    }
                    if (userMatcher.group(5) != null) {
                        ((DataSourceFactory.TunneledDataSourceFactory) sourceDSF).jumpHostPort(Integer.parseInt(userMatcher.group(5)));
                    }
                    if (cmd.getOptionValue(identityFile.getOpt()) != null) {
                        ((DataSourceFactory.TunneledDataSourceFactory) sourceDSF).identityFile(new File(cmd.getOptionValue(identityFile.getOpt())));
                    }
                } else {
                    sourceDSF = DataSourceFactory.simple();
                }
                sourceDSF.hostname(cmd.getOptionValue(hostname.getOpt()));
                sourceDSF.port(Optional.ofNullable((int) (long) cmd.getParsedOptionValue(port.getOpt())).orElse(3306));
                sourceDSF.user(cmd.getOptionValue(user.getOpt()));
                sourceDSF.password(cmd.getOptionValue(password.getOpt()));
                DataSource dataSource = sourceDSF.build();

                String targetUrl = "jdbc:mysql://" + cmd.getOptionValue(targetHostname.getOpt(), cmd.getOptionValue(hostname.getOpt())) + ":" + Optional.ofNullable(cmd.getParsedOptionValue(targetPort.getOpt())).orElse(cmd.getOptionValue(port.getOpt(), "3306")) + "?useUnicode=true&characterEncoding=utf-8&verifyServerCertificate=false&useSSL=false&requireSSL=false";
                String tUser = cmd.getOptionValue(targetUser.getOpt(), cmd.getOptionValue(user.getOpt()));
                String tPassword = cmd.getOptionValue(targetPassword.getOpt(), cmd.getOptionValue(password.getOpt()));
                String sourceSchema = cmd.getOptionValue(source.getOpt());
                String targetSchema = cmd.getOptionValue(target.getOpt());
                boolean isDryRun = cmd.hasOption(dryRun.getOpt());
                boolean isIncremental = cmd.hasOption(incremental.getOpt());
                String outputFileName = cmd.getOptionValue(outputFile.getOpt());
                DataSource targetDataSource = new MysqlDataSource();
                ((MysqlDataSource) targetDataSource).setUrl(targetUrl);
                ((MysqlDataSource) targetDataSource).setUser(tUser);
                ((MysqlDataSource) targetDataSource).setPassword(tPassword);
                boolean isCompress = cmd.hasOption(compress.getOpt());
                boolean isSplitByTable = cmd.hasOption(splitByTable.getOpt());
                boolean isDropAndRecreateTables = cmd.hasOption(dropAndRecreateTables.getOpt());
                Map<Pattern, FieldAnonymizer> anonymizers = Collections.emptyMap();
                if (cmd.hasOption(anonymize.getOpt())) {
                    if (cmd.getOptionValues(anonymize.getOpt()).length == 0) {
                        anonymizers =  FieldAnonymizer.DEFAULT_ANONYMIZERS;
                    } else {
                        anonymizers = buildAnonymizers(cmd.getOptionValues(anonymize.getOpt()));
                    }
                }
                List<Pattern> exclusions = Optional.ofNullable(cmd.getOptionValues(exclusion.getOpt())).map(Arrays::stream).map(s -> s.map(Pattern::compile).collect(Collectors.toList())).orElse(Collections.emptyList());
                DataSourceSynchronizer.builder()
                        .source(dataSource)
                        .target(targetDataSource)
                        .anonymizerMap(anonymizers)
                        .exclusions(exclusions)
                        .build()
                        .sync(sourceSchema,
                                targetSchema,
                                outputFileName,
                                isCompress,
                                isSplitByTable,
                                isDropAndRecreateTables,
                                isDryRun,
                                isIncremental,
                                cmd.hasOption(allowParallel.getOpt()), Integer.valueOf(cmd.getOptionValue(maxRowsPerChunk.getOpt(), DEFAULT_MAX_CHUNK_SIZE)));
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "", e);
            } finally {
                if (sourceDSF != null) {sourceDSF.close();}
            }
        } catch (ParseException e) {
            LOGGER.severe(e.getMessage());
            formatter.printHelp("Database Synchronizer", options);

            System.exit(1);
        }

    }

    private static Map<Pattern, FieldAnonymizer> buildAnonymizers(String[] anonymizers) {
        Map<Pattern, FieldAnonymizer> result = new HashMap<>();
        for (String a: anonymizers) {
            Matcher matcher = Pattern.compile("^\\/(?<regexp>.*)\\/:(?<anonymizer>.*)$")
                    .matcher(a);
            if (!matcher.find()) {
                throw new IllegalArgumentException();
            }
            result.put(Pattern.compile(matcher.group("regexp")), buildAnonymizer(matcher.group("anonymizer")));
        }
        return result;
    }

    private static FieldAnonymizer buildAnonymizer(String anonymizer) {
        return FieldAnonymizer.findByName(anonymizer);
    }

}
