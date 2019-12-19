package de.kune.mysqlsync;

import de.kune.mysqlsync.anonymizer.FieldAnonymizer;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class DataSourceSynchronizer {

    private static final Logger LOGGER = Logger.getLogger(DataSourceSynchronizer.class.getName());

    private final DataSource source, target;
    private final Map<Pattern, FieldAnonymizer> anonymizerMap;

    public DataSourceSynchronizer(DataSource source, DataSource target, Map<Pattern, FieldAnonymizer> anonymizerMap) {
        this.anonymizerMap = anonymizerMap;
        this.source = source;
        this.target = target;
    }

    public DataSourceSynchronizer(DataSource source, DataSource target) {
        this(source, target, Collections.emptyMap());
    }

    private static Set<String> determineTables(DataSource dataSource, String schema) throws SQLException {
        return DatabaseUtil.query(dataSource, "SELECT tt.TABLE_NAME\n" +
                "  FROM INFORMATION_SCHEMA.TABLES tt\n" +
                "  WHERE tt.TABLE_SCHEMA='" + schema + "' and tt.table_name not in ('schema_version', 'flyway_schema_history')").stream().map(e -> e.get("TABLE_NAME")).collect(Collectors.toSet());
    }

    private Set<String> determineSyncTables(String sourceSchema, String targetSchema) throws SQLException {
        Set<String> result = determineTables(source, sourceSchema);
        result.retainAll(determineTables(target, targetSchema));
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
        return sourceIdsByTable;
    }

    private Map<String, Set<String>> determineSyncColumnsOfSyncTables(String sourceSchema, String targetSchema, Set<String> tables) throws SQLException {
        Map<String, List<String>> sourceColumnsByTable = DatabaseUtil.query(source, "SELECT t.table_name, c.column_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                "  LEFT JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key <> 'PRI'\n" +
                "  WHERE t.TABLE_SCHEMA='" + sourceSchema + "' and t.table_name in (" + tables.stream().map(DatabaseUtil::toValue).collect(joining(", ")) + ")").stream()
                .collect(Collectors.toMap(e -> e.get("TABLE_NAME"), e -> asList(e.get("COLUMN_NAME")), (e1, e2) -> Stream.concat(e1.stream(), e2.stream()).collect(toList())));
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
        return result;
    }

    public void sync(String sourceSchema, String targetSchema, String outputFile, boolean dryRun, boolean incremental, int maxNumberOfRows) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
        if (outputFile != null) {
            File f = new File(outputFile);
            if (f.exists() && f.isDirectory()) {
                outputFile = new File(f, (incremental ? "incr-" :"full-") + sourceSchema + "-" + targetSchema + "-" + new SimpleDateFormat("YYYY-MM-dd-HH-mm-ss-z").format(new Date()) + ".sql").getAbsolutePath();
            }
        }
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

            PrintWriter writer = outputFile == null ? null : new PrintWriter(outputFile, "UTF-8");
            try (Connection targetConnection = target.getConnection()) {
                targetConnection.setReadOnly(dryRun);
                targetConnection.setAutoCommit(false);
                Statement stmt = dryRun ? null : targetConnection.createStatement();
                if (!dryRun) {
                    stmt.executeQuery("USE " + DatabaseUtil.armor(targetSchema));
                }

                StringBuilder buf = new StringBuilder();
                executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
                executeAndWriteLn("SET AUTOCOMMIT=0;", stmt, writer, buf);
                executeAndWriteLn("SET UNIQUE_CHECKS=0;", stmt, writer, buf);
                executeAndWriteLn("SET FOREIGN_KEY_CHECKS=0;", stmt, writer, buf);
                executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);

                tables.stream().sorted().forEachOrdered(table -> {
                    Set<String> columns = new LinkedHashSet<>();
                    columns.addAll(primaryKeyByTable.get(table));
                    columns.addAll(columnsByTable.get(table));
                    LOGGER.info("Synchronizing " + table);
                    try {
                        if (incremental) {
                            loadIncrementally(sourceSchema, targetSchema, table, primaryKeyByTable.get(table), columns,
                                    fullLoadRowConsumer(writer, stmt, buf, table, columns),
                                    incrementalNewRowConsumer(writer, stmt, buf, table, columns),
                                    incrementalUpdateRowConsumer(writer, stmt, buf, table, columns, primaryKeyByTable.get(table)), maxNumberOfRows);
                        } else {
                            processTable(sourceSchema, table, columns, fullLoadRowConsumer(writer, stmt, buf, table, columns), maxNumberOfRows);
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);
                executeAndWriteLn("COMMIT;", stmt, writer, buf);
                executeAndWriteLn("SET AUTOCOMMIT=1;", stmt, writer, buf);
                executeAndWriteLn("SET UNIQUE_CHECKS=1;", stmt, writer, buf);
                executeAndWriteLn("SET FOREIGN_KEY_CHECKS=1;", stmt, writer, buf);
                executeAndWriteLn("-- -----------------------------------------------------------------", null, writer, null);

                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            }
        }
        LOGGER.info(format("Finished synchronization for source schema: %s", sourceSchema));
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
        return cachedAnonymizers.computeIfAbsent(cand, c ->anonymizerMap.entrySet().stream().filter(e -> e.getKey().matcher(c).matches()).findFirst().map(e -> e.getValue()));
    }

    private Object anonymize(String table, String column, Object value) {
        String cand = table + "." + column;
        return anonymizerMap.entrySet().stream().filter(e -> e.getKey().matcher(cand).matches()).findFirst().map(e -> (Object) e.getValue().anonymize(column, value)).orElse(value);
//        return anonymizerMap.entrySet().stream().filter(e -> e.getKey().matcher(cand).matches()).findFirst().map(e -> (Object) e.getValue().anonymize(column, value)).orElse(value);
    }

    private void update(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Map<String, Object> row, DatabaseUtil.ResultContext rs, Set<String> primaryKeyColumns) throws SQLException {
        executeAndWriteLn("UPDATE " + DatabaseUtil.armor(table)
                + " SET " + row.entrySet().stream().filter(e -> !primaryKeyColumns.contains(e.getKey())).map(e -> DatabaseUtil.armor(e.getKey()) + "=" + DatabaseUtil.toValue(anonymize(table, e.getKey(), e.getValue()))).collect(joining(","))
                + " WHERE " + primaryKeyColumns.stream().map(primaryKeyColumn->DatabaseUtil.armor(primaryKeyColumn) + "=" + DatabaseUtil.toValue(row.get(primaryKeyColumn))).collect(joining(" AND ")) + ";", stmt, writer, buf);
    }

    private void insert(PrintWriter writer, Statement stmt, StringBuilder buf, String table, Set<String> columns, Map<String, Object> row, DatabaseUtil.ResultContext rs) throws SQLException {
        if (rs.isFirstRow()) {
            executeAndWriteLn("INSERT " + DatabaseUtil.armor(table) + " (" + columns.stream().map(DatabaseUtil::armor).collect(joining(",")) + ") VALUES ", stmt, writer, buf);
        }
        executeAndWrite("  (" + row.entrySet().stream()
                .map(e->anonymize(table, e.getKey(), e.getValue()))
                .map(DatabaseUtil::toValue)
                .collect(joining(",")) + ")", stmt, writer, buf);
        if (rs.isLastRow()) {
            executeAndWriteLn(";", stmt, writer, buf);
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
        return DatabaseUtil.query(source,
                "SELECT " + columns.stream().map(DatabaseUtil::armor).collect(joining(", "))
                        + " FROM " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table)
                        + " LIMIT " + startingRow + "," + maxNumberOfRows, rowConsumer, isFirstChunk);
    }

}
