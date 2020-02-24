package de.kune.mysqlsync;

import de.kune.mysqlsync.anonymizer.FieldAnonymizer;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.*;

public class DataSourceSynchronizer {

    private static final Logger LOGGER = Logger.getLogger(DataSourceSynchronizer.class.getName());

    private final DataSource source, target;
    private final Map<Pattern, FieldAnonymizer> anonymizerMap;
    private final List<Pattern> exclusions;
    private Date creationDate = new Date();

    public static class Factory {
        private DataSource source, target;
        private Map<Pattern, FieldAnonymizer> anonymizerMap = Collections.emptyMap();
        private List<Pattern> exclusions = new ArrayList<>();

        public DataSourceSynchronizer build() {
            return new DataSourceSynchronizer(source, target, anonymizerMap, exclusions);
        }

        public Factory source(DataSource source) {
            this.source = source;
            return this;
        }

        public Factory target(DataSource target) {
            this.target = target;
            return this;
        }

        public Factory anonymizerMap(Map<Pattern, FieldAnonymizer> anonymizerMap) {
            this.anonymizerMap = anonymizerMap;
            return this;
        }

        public Factory exclusions(List<Pattern> exclusions) {
            this.exclusions = new ArrayList<>(exclusions);
            return this;
        }

        public Factory exclude(Pattern exclusion) {
            this.exclusions.add(exclusion);
            return this;
        }

        public Factory exclude(String exclusionPattern) {
            this.exclusions.add(Pattern.compile(exclusionPattern));
            return this;
        }
    }

    public static Factory builder() {
        return new Factory();
    }

    public DataSourceSynchronizer(DataSource source, DataSource target, Map<Pattern, FieldAnonymizer> anonymizerMap, List<Pattern> exclusions) {
        assert (source != null);
        assert (anonymizerMap != null);
        assert (exclusions != null);
        this.anonymizerMap = new HashMap<>(anonymizerMap);
        this.source = source;
        this.target = target;
        this.exclusions = new ArrayList<>(exclusions);
        LOGGER.info("Created data source synchronizer with anonymizers: " + anonymizerMap);
    }

    private static Set<String> determineTables(DataSource dataSource, String schema) throws SQLException {
        return DatabaseUtil.query(dataSource, "SELECT tt.TABLE_NAME\n" +
                "  FROM INFORMATION_SCHEMA.TABLES tt\n" +
                "  WHERE tt.TABLE_SCHEMA='" + schema + "' and tt.table_name not in ('schema_version', 'flyway_schema_history')").stream().map(e -> e.get("TABLE_NAME")).collect(toSet());
    }

    private Set<String> determineSyncTables(String sourceSchema, String targetSchema) throws SQLException {
        Set<String> result = determineTables(source, sourceSchema);
        if (targetSchema != null) {
            result.retainAll(determineTables(target, targetSchema));
        }
        return result;
    }

    private Map<String, Set<String>> determinePrimaryKeysOfSyncTables(String sourceSchema, String targetSchema, Set<String> tables) throws SQLException {
        Map<String, Set<String>> sourceIdsByTable = DatabaseUtil.query(source, "SELECT t.table_name, c.column_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                "  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key = 'PRI'\n" +
                "  WHERE t.TABLE_SCHEMA='" + sourceSchema + "' and t.table_name in (" + tables.stream().map(s -> "'" + s + "'").collect(joining(", ")) + ")")
                .stream()
                .collect(Collectors.toMap(e -> e.get("TABLE_NAME"), e -> new HashSet<String>(asList(e.get("COLUMN_NAME")).stream().filter(Objects::nonNull).collect(toList())), (e1, e2) -> {
                    HashSet<String> result = new HashSet<>(e1.stream().filter(Objects::nonNull).collect(toList()));
                    result.addAll(e2.stream().filter(Objects::nonNull).collect(toList()));
                    return result;
                }));
        if (targetSchema != null) {
            Map<String, Set<String>> targetIdsByTable = DatabaseUtil.query(target, "SELECT t.table_name, c.column_name\n" +
                    "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                    "  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key = 'PRI'\n" +
                    "  WHERE t.TABLE_SCHEMA='" + targetSchema + "' and t.table_name in (" + tables.stream().map(s -> "'" + s + "'").collect(joining(", ")) + ")")
                    .stream()
                    .collect(Collectors.toMap(e -> e.get("TABLE_NAME"), e -> new HashSet<String>(asList(e.get("COLUMN_NAME")).stream().filter(Objects::nonNull).collect(toList())), (e1, e2) -> {
                        HashSet<String> result = new HashSet<>(e1.stream().filter(Objects::nonNull).collect(toList()));
                        result.addAll(e2.stream().filter(Objects::nonNull).collect(toList()));
                        result.remove(null);
                        return result;
                    }));
            if (!sourceIdsByTable.equals(targetIdsByTable)) {
                throw new IllegalStateException(format("synch tables have different primary keys source=%s, target=%s", sourceIdsByTable, targetIdsByTable));
            }
        }
        return sourceIdsByTable;
    }

    private Map<String, Set<String>> determineSyncColumnsOfSyncTables(String sourceSchema, String targetSchema, Set<String> tables) throws SQLException {
        Map<String, Set<String>> sourceColumnsByTable = DatabaseUtil.query(source, "SELECT t.table_name, c.column_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                "  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key <> 'PRI'\n" +
                "  WHERE t.TABLE_SCHEMA='" + sourceSchema + "' and t.table_name in (" + tables.stream().map(DatabaseUtil::toValue).collect(joining(", ")) + ")").stream()
                .collect(Collectors.toMap(e -> e.get("TABLE_NAME"), e -> new HashSet<>(asList(e.get("COLUMN_NAME")).stream().filter(x->x != null).collect(toList())), (e1, e2) -> Stream.concat(e1.stream(), e2.stream()).collect(toSet())));
        if (targetSchema == null) {
            return withoutExclusions(sourceColumnsByTable);
        } else {
            Map<String, List<String>> targetColumnsByTable = DatabaseUtil.query(target, "SELECT t.table_name, c.column_name\n" +
                    "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                    "  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key <> 'PRI'\n" +
                    "  WHERE t.TABLE_SCHEMA='" + targetSchema + "' and t.table_name in (" + tables.stream().map(DatabaseUtil::toValue).collect(joining(", ")) + ")").stream()
                    .collect(Collectors.toMap(e -> e.get("TABLE_NAME"), e -> asList(e.get("COLUMN_NAME")), (e1, e2) -> Stream.concat(e1.stream(), e2.stream()).collect(toList())));
            Map<String, Set<String>> result = new HashMap<>();
            for (String table : tables) {
                Set<String> columns = new LinkedHashSet<>(sourceColumnsByTable.get(table));
                columns.retainAll(targetColumnsByTable.get(table));
                columns.remove(null);
                result.put(table, columns);
            }
            return withoutExclusions(result);
        }
    }

    private Map<String, Set<String>> withoutExclusions(Map<String, Set<String>> columnsByTable) {
        return columnsByTable.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()
                        .stream()
                        .filter(f -> !exclusions.stream().anyMatch(x -> x.matcher(e.getKey() + "." + f).matches())).collect(toSet())
                ));
    }

    public void sync(String sourceSchema, String targetSchema, String outputFileInput, boolean compress, boolean splitByTable, boolean dropAndRecreateTables, boolean dryRun, boolean incremental, boolean allowParallel, int maxNumberOfRows) throws SQLException, IOException {
        if (targetSchema == null) {
            dryRun = true;
        }
        String outputFile = outputFile(sourceSchema, targetSchema, outputFileInput, compress, incremental, null);
        Set<String> tables = determineSyncTables(sourceSchema, targetSchema);
        LOGGER.info(format("Starting synchronization for source schema: %s", sourceSchema));
        LOGGER.info(format("Configured chunk size is: %d", maxNumberOfRows));
        LOGGER.info(tables.toString());
        if (tables.isEmpty()) {
            LOGGER.info("No tables found to sync");
        } else {
            Map<String, Set<String>> primaryKeyByTable = determinePrimaryKeysOfSyncTables(sourceSchema, targetSchema, tables);
            LOGGER.info(primaryKeyByTable.toString());
            Map<String, Set<String>> columnsByTable = determineSyncColumnsOfSyncTables(sourceSchema, targetSchema, tables);
            LOGGER.info(columnsByTable.toString());

            PrintWriter oneWriter = splitByTable ? null : openWriter(outputFile, compress);
            try (Connection targetConnection = dryRun ? null : target.getConnection()) {
                if (!dryRun) {
                    targetConnection.setReadOnly(dryRun);
                    targetConnection.setAutoCommit(false);
                }
                Statement stmt = dryRun ? null : targetConnection.createStatement();
                if (!dryRun) {
                    stmt.executeQuery("USE " + DatabaseUtil.armor(targetSchema));
                }

                final StringBuilder buf = new StringBuilder();

                if (!splitByTable) {
                    writeHeader(stmt, oneWriter, buf);
                }

                if (splitByTable && allowParallel) {
                    tables.stream().parallel().forEach(
                            synchronizeTable(sourceSchema, targetSchema, outputFileInput, compress, splitByTable, dropAndRecreateTables, incremental, maxNumberOfRows, primaryKeyByTable, columnsByTable, oneWriter, stmt, buf));
                } else {
                    tables.stream().sorted().forEachOrdered(synchronizeTable(sourceSchema, targetSchema, outputFileInput, compress, splitByTable, dropAndRecreateTables, incremental, maxNumberOfRows, primaryKeyByTable, columnsByTable, oneWriter, stmt, buf));
                }

                if (!splitByTable) {
                    writeFooter(stmt, oneWriter, buf);
                    closeWriter(oneWriter);
                }
            }
        }
        LOGGER.info(format("Finished synchronization for source schema: %s", sourceSchema));
    }

    private Consumer<String> synchronizeTable(String sourceSchema, String targetSchema, String outputFileInput, boolean compress, boolean splitByTable, boolean dropAndRecreateTables, boolean incremental, int maxNumberOfRows, Map<String, Set<String>> primaryKeyByTable, Map<String, Set<String>> columnsByTable, PrintWriter oneWriter, Statement stmt, StringBuilder buf) {
        return table -> {
    Set<String> columns = new LinkedHashSet<>();
    columns.addAll(primaryKeyByTable.get(table));
    columns.addAll(columnsByTable.get(table));
    LOGGER.info("Synchronizing " + table);
    try {
        PrintWriter writer;
        StringBuilder localBuf = buf;
        if (splitByTable) {
            localBuf = targetSchema == null ? null : new StringBuilder();
            writer = openWriter(outputFile(sourceSchema, targetSchema, outputFileInput, compress, incremental, table), compress);
            writeHeader(stmt, writer, localBuf);
        } else {
            writer = oneWriter;
        }
        if (incremental) {
            loadIncrementally(sourceSchema, targetSchema, table, primaryKeyByTable.get(table), columns,
                    fullLoadRowConsumer(writer, stmt, localBuf, table, columns),
                    incrementalNewRowConsumer(writer, stmt, localBuf, table, columns),
                    incrementalUpdateRowConsumer(writer, stmt, localBuf, table, columns, primaryKeyByTable.get(table)), maxNumberOfRows);
        } else {
            if (dropAndRecreateTables) {
                dropAndRecreateTable(writer, stmt, localBuf, sourceSchema, targetSchema, table);
            }
            processTable(sourceSchema, table, columns, fullLoadRowConsumer(writer, stmt, localBuf, table, columns), maxNumberOfRows);
        }
        if (splitByTable) {
            writeFooter(stmt, writer, localBuf);
            closeWriter(writer);
        }
    } catch (SQLException e) {
        throw new RuntimeException(e);
    }
};
    }

    private void dropAndRecreateTable(PrintWriter writer, Statement stmt, StringBuilder localBuf, String sourceSchema, String targetSchema, String table) throws SQLException {
        String createTable = DatabaseUtil.query(targetSchema == null ? source : target, "show create table " + (targetSchema == null ? sourceSchema : targetSchema) + "." + table).get(0).get("Create Table");
        executeAndWriteLn("drop table if exists " + table + ";", stmt, writer, localBuf);
        executeAndWriteLn(createTable + ";", stmt, writer, localBuf);
    }


    private String outputFile(String sourceSchema, String targetSchema, String outputFileInput, boolean compress, boolean incremental, String table) {
        if (outputFileInput == null) {
            return null;
        }
        String outputFile = null;
        if (outputFileInput != null) {
            File f = new File(outputFileInput);
            if (f.exists() && f.isDirectory()) {
                outputFile = new File(f, (incremental ? "incr-" : "full-") + sourceSchema + (targetSchema == null ? "" : "-" + targetSchema) + "-" + new SimpleDateFormat("YYYY-MM-dd-HH-mm-ss-z").format(creationDate) + (anonymizerMap.isEmpty() ? "" : "_anon") + (table == null ? extension(compress) : "")).getAbsolutePath();
            } else {
                outputFile = outputFileInput;
            }
        }
        if (table == null) {
            new File(outputFile).getParentFile().mkdirs();
            return outputFile;
        } else {
            new File(new File(outputFile), table).getParentFile().mkdirs();
            return new File(new File(outputFile), table + extension(compress)).getAbsolutePath();
        }
    }

    private String extension(boolean compress) {
        return ".sql" + (compress ? ".gz" : "");
    }

    private void closeWriter(PrintWriter writer) {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

    private PrintWriter openWriter(String outputFile, boolean compress) {
        try {
            return outputFile == null ? null : new PrintWriter(
                    new OutputStreamWriter(
                            compress ? new GZIPOutputStream(new FileOutputStream(outputFile))
                                    : new FileOutputStream(outputFile), "UTF-8")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeFooter(Statement stmt, PrintWriter writer, StringBuilder buf) throws SQLException {
        executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
        executeAndWriteLn("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;", null, writer, null);
        executeAndWriteLn("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;", null, writer, null);
        executeAndWriteLn("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;", null, writer, null);
        executeAndWriteLn("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;", null, writer, null);
        executeAndWriteLn("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;", null, writer, null);
        executeAndWriteLn("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;", null, writer, null);
        executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
    }

    private void writeHeader(Statement stmt, PrintWriter writer, StringBuilder buf) throws SQLException {
        executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
        executeAndWriteLn("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;", stmt, writer, buf);
        executeAndWriteLn("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;", stmt, writer, buf);
        executeAndWriteLn("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;", stmt, writer, buf);
        executeAndWriteLn("/*!40101 SET NAMES utf8 */;", stmt, writer, buf);
        executeAndWriteLn("SET NAMES utf8mb4;", stmt, writer, buf);
        executeAndWriteLn("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;", stmt, writer, buf);
        executeAndWriteLn("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;", stmt, writer, buf);
        executeAndWriteLn("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;", stmt, writer, buf);
        executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
    }


    private DatabaseUtil.RowConsumer incrementalUpdateRowConsumer(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Set<String> columns, Set<String> primaryKeyColumn) {
        return (row, rs) -> update(writer, stmt, buf, table, row, rs, primaryKeyColumn);
    }

    private DatabaseUtil.RowConsumer incrementalNewRowConsumer(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Set<String> columns) {
        return (row, rs) -> insert(writer, stmt, buf, table, columns, row, rs);
    }

    private DatabaseUtil.RowConsumer fullLoadRowConsumer(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Set<String> columns) {
        return (row, rs) -> {
            truncate(writer, stmt, buf, table, rs);
            insert(writer, stmt, buf, table, columns, row, rs);
        };
    }

    private void truncate(PrintWriter writer, Statement stmt, StringBuilder buf, String table, DatabaseUtil.ResultContext rs) throws SQLException {
        if (rs.isFirstRow() && rs.isFirstChunk()) {
            executeAndWriteLn("TRUNCATE " + DatabaseUtil.armor(table) + ";", stmt, writer, buf);
        }
    }

    private ConcurrentMap<String, Optional<FieldAnonymizer>> cachedAnonymizers = new ConcurrentHashMap<>();

    private Optional<FieldAnonymizer> getCachedAnonymizer(String cand) {
        return cachedAnonymizers.computeIfAbsent(cand, c -> determineAnonymizer(c));
    }

    private Optional<FieldAnonymizer> determineAnonymizer(String c) {
        Optional<FieldAnonymizer> result = anonymizerMap.entrySet().stream().filter(e -> e.getKey().matcher(c).matches()).findFirst().map(e -> e.getValue());
        LOGGER.info("determined anonymizer for input " + c + ": " + result);
        return result;
    }

    private Object anonymize(String table, String column, Object value, Map<String, Object> row) {
        String cand = table + "." + column;
        return getCachedAnonymizer(cand).map(e -> (Object) e.anonymize(column, value, row)).orElse(value);
    }

    private void update(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Map<String, Object> row, DatabaseUtil.ResultContext rs, Set<String> primaryKeyColumns) throws SQLException {
        executeAndWriteLn("UPDATE " + DatabaseUtil.armor(table)
                + " SET " + row.entrySet().stream().filter(e -> !primaryKeyColumns.contains(e.getKey())).map(e -> DatabaseUtil.armor(e.getKey()) + "=" + DatabaseUtil.toValue(anonymize(table, e.getKey(), e.getValue(), row))).collect(joining(","))
                + " WHERE " + primaryKeyColumns.stream().map(primaryKeyColumn->DatabaseUtil.armor(primaryKeyColumn) + "=" + DatabaseUtil.toValue(row.get(primaryKeyColumn))).collect(joining(" AND ")) + ";", stmt, writer, buf);
    }

    private void insert(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Set<String> columns, Map<String, Object> row, DatabaseUtil.ResultContext rs) throws SQLException {
        if (rs.isFirstRow()) {
//            executeAndWriteLn("ALTER TABLE " + DatabaseUtil.armor(table) + " DISABLE KEYS;", stmt, writer, buf);
            executeAndWriteLn("LOCK TABLES " + DatabaseUtil.armor(table) + " WRITE;", stmt, writer, buf);
            executeAndWriteLn("/*!40000 ALTER TABLE " + DatabaseUtil.armor(table) + " DISABLE KEYS */;", stmt, writer, buf);
            executeAndWriteLn("INSERT " + DatabaseUtil.armor(table) + " (" + columns.stream().map(DatabaseUtil::armor).collect(joining(",")) + ") VALUES ", stmt, writer, buf);

        }
        executeAndWrite("  (" + row.entrySet().stream()
                .map(e->anonymize(table, e.getKey(), e.getValue(), row))
                .map(DatabaseUtil::toValue)
                .collect(joining(",")) + ")", stmt, writer, buf);
        if (rs.isLastRow()) {
            executeAndWriteLn(";", stmt, writer, buf);
//            executeAndWriteLn("ALTER TABLE " + DatabaseUtil.armor(table) + " ENABLE KEYS;", stmt, writer, buf);
            executeAndWriteLn("/*!40000 ALTER TABLE " + DatabaseUtil.armor(table) + " ENABLE KEYS */;", stmt, writer, buf);
            executeAndWriteLn("UNLOCK TABLES;", stmt, writer, buf);
        } else if (rs.getRow() > 1 && (rs.getRow() - 1) % 150 == 0) {
            executeAndWriteLn(";", stmt, writer, buf);
            executeAndWriteLn("INSERT " + DatabaseUtil.armor(table) + " (" + columns.stream().map(DatabaseUtil::armor).collect(joining(",")) + ") VALUES ", stmt, writer, buf);
        } else {
            executeAndWriteLn(",", stmt, writer, buf);
        }
    }

    private void executeAndWrite(String s, Statement stmt, PrintWriter writer, StringBuilder buf) throws SQLException {
        if (buf != null) {
            buf.append(s.trim());
            if (buf.charAt(buf.length() - 1) == ';') {
                LOGGER.fine("executing " + buf.toString());
                if (stmt != null) {
                    stmt.executeLargeUpdate(buf.toString());
                }
                buf.delete(0, buf.length());
            } else {
                LOGGER.finer("appending " + s.trim());
                buf.append(" ");
            }
        }
        if (writer != null) {
            writer.print(s);
        }
    }

    private void executeAndWriteLn(String s, Statement stmt, PrintWriter writer, StringBuilder buf) throws SQLException {
        if (buf != null) {
            buf.append(s.trim());
            if (buf.charAt(buf.length() - 1) == ';') {
                LOGGER.fine("executing " + buf.toString());
                if (stmt != null) {
                    stmt.executeLargeUpdate(buf.toString());
                }
                buf.delete(0, buf.length());
            } else {
                LOGGER.fine("appending " + s.trim());
                buf.append(" ");
            }
        }
        if (writer != null) {
            writer.println(s);
            writer.flush();
        }
    }

    private void loadIncrementally(String sourceSchema, String targetSchema, String table, Set<String> primaryKeys, Set<String> columns, DatabaseUtil.RowConsumer fullRowConsumer, DatabaseUtil.RowConsumer newRowConsumer, DatabaseUtil.RowConsumer modifiedRowConsumer, int maxNumberOfRows) throws SQLException {
        if (!primaryKeys.isEmpty() && (columns.contains("creationDate") || columns.contains("creation_date")) && (columns.contains("lastModifiedDate") || columns.contains("last_modified_date"))) {
            final String creationDateColumn = columns.contains("creationDate") ? "creationDate" : "creation_date";
            final String lastModifiedDateColumn = columns.contains("lastModifiedDate") ? "lastModifiedDate" : "last_modified_date";
            // TODO: Determine max creation and last modified date in target table -> maxDate
            Optional<String> maxDate = DatabaseUtil.query(target, "select greatest(ifnull(max(" + DatabaseUtil.armor(lastModifiedDateColumn) + "), '0000-01-01 00:00:00'), ifnull(max(" + DatabaseUtil.armor(creationDateColumn) + "), '0000-01-01 00:00:00')) as maxDate from " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table)).stream().findAny().map(e -> e.get("maxDate"));
//            Optional<String> sourceMaxDate = query(source, "select greatest(max(" + armor(lastModifiedDateColumn) + "), max(" + armor(creationDateColumn) + ")) as maxDate from " + armor(sourceSchema) + "." + armor(table)).stream().findAny().map(e->e.get("maxDate"));

            if (maxDate.isPresent()) {
                // TODO: Fetch all entries from the source table with creation date before maxDate and insert them into the target table
                DatabaseUtil.query(source, "SELECT " + columns.stream().map(DatabaseUtil::armor).collect(joining(", ")) + " FROM " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table) + " where " + DatabaseUtil.armor(creationDateColumn) + " > '" + maxDate.get() + "'", newRowConsumer, true);
                // TODO: Fetch all entries from the source table with lastModified date before maxDate
                DatabaseUtil.query(source, "SELECT " + columns.stream().map(DatabaseUtil::armor).collect(joining(", ")) + " FROM " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table) + " where " + DatabaseUtil.armor(lastModifiedDateColumn) + " > '" + maxDate.get() + "'", modifiedRowConsumer, true);
            } else {
                LOGGER.info("Could not determine maximum creation date and last modified date for table " + table + " falling back to full sync");
                processTable(sourceSchema, table, columns, fullRowConsumer, maxNumberOfRows);
            }
        } else {
            if (primaryKeys.isEmpty()) {
                LOGGER.info("Table " + table + " has no primary key column, falling back to full sync");
            } else {
                LOGGER.info("Could not determine creation date or last modified date column or no primary key for table " + table + ", falling back to full sync");
            }
            processTable(sourceSchema, table, columns, fullRowConsumer, maxNumberOfRows);
        }
    }

    private void processTable(String sourceSchema, String table, Set<String> columns, DatabaseUtil.RowConsumer fullLoadRowConsumer, int maxNumberOfRows) throws SQLException {
        int startingRow = 0;
        boolean isFirstChunk = true;
        while (loadFully(sourceSchema, table, columns, fullLoadRowConsumer, startingRow, maxNumberOfRows, isFirstChunk) > 0) {
            startingRow += maxNumberOfRows;
            isFirstChunk = false;
        }
    }

    private long loadFully(String sourceSchema, String table, Set<String> columns, DatabaseUtil.RowConsumer rowConsumer, int startingRow, int maxNumberOfRows, boolean isFirstChunk) throws SQLException {
        LOGGER.info("Fetching a maximum of " + maxNumberOfRows + " from " + table + " starting with row " + startingRow);
        return DatabaseUtil.query(source,
                "SELECT " + columns.stream().map(DatabaseUtil::armor).collect(joining(", "))
                        + " FROM " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table)
                        + " LIMIT " + startingRow + "," + maxNumberOfRows, rowConsumer, isFirstChunk);
    }

}
