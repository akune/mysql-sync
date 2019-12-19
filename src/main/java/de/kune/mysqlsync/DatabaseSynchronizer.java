package de.kune.mysqlsync;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Provides functionality to synchronize data between similar schemas in the same database management system.
 */
public class DatabaseSynchronizer {

    private static final Logger LOGGER = Logger.getLogger(DatabaseSynchronizer.class.getName());

    private final DataSource dataSource;

    public DatabaseSynchronizer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Set<String> determineSyncTables(String sourceSchema, String targetSchema) throws SQLException {
        return DatabaseUtil.query(dataSource,"SELECT tt.table_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES tt\n" +
                "  JOIN INFORMATION_SCHEMA.TABLES ts on tt.table_name = ts.table_name and ts.table_schema = '"+sourceSchema+"'\n" +
                "  WHERE tt.TABLE_SCHEMA='"+targetSchema+"' and tt.table_name not in ('schema_version', 'flyway_schema_history')").stream().map(e->e.get("TABLE_NAME")).collect(Collectors.toSet());

    }

    private Map<String, String> determinePrimaryKeysOfSyncTables(String sourceSchema, String targetSchema) throws SQLException {
        return DatabaseUtil.query(dataSource,"SELECT t.table_name, c.column_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                "  JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key = 'PRI'\n" +
                "  JOIN INFORMATION_SCHEMA.COLUMNS cs on cs.table_name = c.table_name and cs.column_name = c.column_name and cs.table_schema = '"+sourceSchema+"' and c.column_key = 'PRI'\n" +
                "  WHERE t.TABLE_SCHEMA='"+targetSchema+"' and t.table_name not in ('schema_version', 'flyway_schema_history')")
                .stream()
                .collect(Collectors.toMap(e->e.get("TABLE_NAME"), e->e.get("COLUMN_NAME")));
    }

    private Map<String, List<String>> determineSyncColumnsOfSyncTables(String sourceSchema, String targetSchema) throws SQLException {
        return DatabaseUtil.query(dataSource, "SELECT t.table_name, c.column_name\n" +
                "  FROM INFORMATION_SCHEMA.TABLES t\n" +
                "  JOIN INFORMATION_SCHEMA.COLUMNS c on c.table_name = t.table_name and c.table_schema = t.table_schema and c.column_key <> 'PRI'\n" +
                "  JOIN INFORMATION_SCHEMA.COLUMNS cs on cs.table_name = c.table_name and cs.column_name = c.column_name and cs.table_schema = '"+sourceSchema+"' and c.column_key <> 'PRI'\n" +
                "  WHERE t.TABLE_SCHEMA='"+targetSchema+"' and t.table_name not in ('schema_version', 'flyway_schema_history')").stream()
                .collect(Collectors.toMap(e->e.get("TABLE_NAME"), e->Arrays.asList(e.get("COLUMN_NAME")), (e1, e2)-> Stream.concat(e1.stream(), e2.stream()).collect(Collectors.toList())));
    }

    private static List<String> generateSyncScriptForTable(String sourceSchema, String targetSchema, String table, String id, List<String> cols) {
        List<String> result = new ArrayList<>();
        result.add("-- Sync " + table);
        result.add(generateScriptToDeleteRowsFromTargetThatAreMissingInSource(sourceSchema, targetSchema, table, id));
        result.add(generateScriptToDeleteRowsInTargetThatDifferFromSource(sourceSchema, targetSchema, table, id, cols));
        result.add(generateScriptToInsertMissingRowsIntoTargetThatExistInSource(sourceSchema, targetSchema, table, id, cols));
        return result;
    }

    private static String generateSyncScript(String sourceSchema, String targetSchema, Set<String> tables, Map<String, String> primaryKeyByTable, Map<String, List<String>> columnsByTable) {
        StringBuilder result = new StringBuilder();
        result.append("-- -----------------------------------------------------------------\n");
        result.append("-- Sync script to update " + targetSchema + " from " + sourceSchema + "\n");
        result.append("-- -----------------------------------------------------------------\n");
        result.append("SET autocommit=0;\n");
        result.append("SET FOREIGN_KEY_CHECKS=0;\n");
        result.append("-- -----------------------------------------------------------------\n");
        for (String table: tables) {
            String id = primaryKeyByTable.get(table);
            List<String> cols = columnsByTable.getOrDefault(table, Collections.emptyList());
            for (String s: generateSyncScriptForTable(sourceSchema, targetSchema, table, id, cols)) {
                result.append(s + "\n");
            }
            result.append("-- -----------------------------------------------------------------\n");
        }
        result.append("COMMIT;\n");
        result.append("SET autocommit=1;\n");
        result.append("SET FOREIGN_KEY_CHECKS=1;\n");
        result.append("-- -----------------------------------------------------------------\n");
        return result.toString();
    }

    private static String generateScriptToInsertMissingRowsIntoTargetThatExistInSource(String sourceSchema, String targetSchema, String table, String id, List<String> cols) {
        // insert entries that do not exist in the target table:
        return "insert into " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " (" + id + ", " + cols.stream().collect(Collectors.joining(", ")) + ") (select s." + id + ", " + cols.stream().map(e -> "s." + e).collect(Collectors.joining(", ")) + " from " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table) + " s left join " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " t on s." + DatabaseUtil.armor(id) + " = t." + DatabaseUtil.armor(id) + " where t." + DatabaseUtil.armor(id) + " is null);";
    }


    private static String generateScriptToDeleteRowsInTargetThatDifferFromSource(String sourceSchema, String targetSchema, String table, String id, List<String> cols) {
        // delete entries that are different in source and target table:
        return "delete from " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " where " + DatabaseUtil.armor(id) + " in (select im." + DatabaseUtil.armor(id) + " from (select t." + DatabaseUtil.armor(id) + " from " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " t join " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table) + " s on s." + DatabaseUtil.armor(id) + " = t." + DatabaseUtil.armor(id) + " and (" + cols.stream().map(DatabaseUtil::armor).map(e -> "s." + e + " <> t." + e).collect(Collectors.joining(" or ")) + ")) im);";
    }

    private static String generateScriptToDeleteRowsFromTargetThatAreMissingInSource(String sourceSchema, String targetSchema, String table, String id) {
        // delete entries that do not exist in the source table:
        return "delete from " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " where " + DatabaseUtil.armor(id) + " in (select im." + DatabaseUtil.armor(id) + " from (select t." + DatabaseUtil.armor(id) + " from " + DatabaseUtil.armor(targetSchema) + "." + DatabaseUtil.armor(table) + " t left join " + DatabaseUtil.armor(sourceSchema) + "." + DatabaseUtil.armor(table) + " s on s." + DatabaseUtil.armor(id) + " = t." + DatabaseUtil.armor(id) + " where s." + DatabaseUtil.armor(id) + " is null) im);";
    }

    /**
     * Synchronize the data content of a target schema with a source schema.
     * This database synchronizer must be configured with a data source that
     * has read access to INFORMATION_SCHEMA,
     * has read access to the specified source schema,
     * has write access (data only) to the specified target schema.
     *
     * @param sourceSchema the database schema to use as a source
     * @param targetSchema the database schema that will be updated
     * @param dryRun if true, nothing will be updated in the target schema
     * @throws SQLException
     */
    public void sync(String sourceSchema, String targetSchema, boolean dryRun) throws SQLException {
        Set<String> tables = determineSyncTables(sourceSchema, targetSchema);
        Map<String, String> primaryKeyByTable = determinePrimaryKeysOfSyncTables(sourceSchema, targetSchema);
        Map<String, List<String>> columnsByTable = determineSyncColumnsOfSyncTables(sourceSchema, targetSchema);
        if (primaryKeyByTable.size() != tables.size()) {
            throw new IllegalStateException(format("Could not determine a primary key for every table. Tables: %s. Primary keys: %s", tables, primaryKeyByTable));
        }
        String syncScript = generateSyncScript(sourceSchema, targetSchema, tables, primaryKeyByTable, columnsByTable);
        if (!dryRun) {
            LOGGER.info("Running \n" + syncScript);
            long changes = DatabaseUtil.update(dataSource, syncScript);
            LOGGER.info(format("Performed %s changes", changes));
        }
    }

}
