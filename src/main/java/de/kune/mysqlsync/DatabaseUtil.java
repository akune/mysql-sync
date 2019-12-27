package de.kune.mysqlsync;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import static java.lang.String.format;

public final class DatabaseUtil {

    @FunctionalInterface
    public interface RowConsumer {

        /**
         * Performs this operation on the given row and result context.
         *
         * @param row the row content
         * @param context the result context
         */
        void accept(Map<String, Object> row, ResultContext context) throws SQLException;
    }

    public static class ResultContext {
        private final boolean isFirstRow;
        private final boolean isLastRow;
        private final long row;
        private boolean isFirstChunk;

        private ResultContext(boolean isFirstRow, boolean isLastRow, long row) {
            this.isFirstRow = isFirstRow;
            this.isLastRow = isLastRow;
            this.row = row;
        }

        public ResultContext(ResultSet rs, boolean isFirstChunk) throws SQLException {
            this(rs.isFirst(), rs.isLast(), rs.getRow());
            this.isFirstChunk = isFirstChunk;
        }

        public boolean isFirstRow() {
            return isFirstRow;
        }

        public boolean isFirstChunk() {
            return isFirstChunk;
        }

        public boolean isLastRow() {
            return isLastRow;
        }

        public long getRow() {
            return row;
        }
    }

    private static final Logger LOGGER = Logger.getLogger(DatabaseUtil.class.getName());

    private DatabaseUtil() {
    }

    public static Map<String, List<Map<String, String>>> query(DataSource dataSource, Set<String> queries) throws SQLException {
        Map<String, List<Map<String, String>>> result = new HashMap<>();
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            for (String q : queries) {
                result.put(q, query(connection, q));
            }
        } finally {
            connection.close();
        }
        return result;
    }


    private static long query(Connection connection, String query, RowConsumer rowConsumer, boolean isFirstChunk) throws SQLException {
        Statement stmt = connection.createStatement();
        long count = 0;
        try {
            LOGGER.fine(format("Query: %s", query));
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                count++;
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (md.getColumnType(i) == Types.TIMESTAMP) {
                        Timestamp timestamp = rs.getTimestamp(i);
                        row.put(md.getColumnName(i), Optional.ofNullable(timestamp).map(t ->
                                new Timestamp(t.getTime() - TimeZone.getDefault().getOffset(t.getTime()))).orElse(null));
                    } else {
                        row.put(md.getColumnName(i),
                                rs.getObject(md.getColumnName(i)));
                    }
                }
                rowConsumer.accept(row, new ResultContext(rs, isFirstChunk));
            }
            return count;
        } finally {
            stmt.close();
        }
    }

    private static List<Map<String, String>> query(Connection connection, String query) throws SQLException {
        Statement stmt = connection.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData md = rs.getMetaData();
            List<Map<String, String>> result = new LinkedList<>();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    row.put(md.getColumnName(i),
                            rs.getString(md.getColumnName(i)));
                }
                result.add(row);
            }
            return result;
        } catch (RuntimeException|SQLException e) {
            throw new RuntimeException(format("Error while executing query %s", query), e);
        } finally {
            stmt.close();
        }
    }

    public static List<Map<String, String>> query(DataSource dataSource, String query) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            return query(connection, query);
        } finally {
            connection.close();
        }
    }

    public static long query(DataSource dataSource, String query, RowConsumer rowConsumer, Boolean isFirstChunk) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);
            return query(connection, query, rowConsumer, isFirstChunk);
        }
    }

    public static long update(DataSource dataSource, String updateQuery) throws SQLException {
        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(false);
            // connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            Statement stmt = connection.createStatement();
            long result = 0;
            for (String update : updateQuery.split("\n")) {
                LOGGER.fine(update);
                result += stmt.executeUpdate(update);
            }
            return result;
        } finally {
            connection.close();
        }
    }

    public static String toValue(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof Long
                || input instanceof Integer
                || input instanceof Boolean
                || input instanceof BigDecimal) {
            return input.toString();
        }
        if (input instanceof String
                || input instanceof Timestamp
                || input instanceof Date) {
            return "'" + input.toString().replace("'", "''").replace("\\", "\\\\") + "'";
        }
        if (input instanceof byte[]) {
            return "X'" + bytesToHex((byte[])input) + "'";
        }
        LOGGER.warning("No explicit mapping for value type " + input.getClass());
        return "'" + input.toString().replace("'", "''").replace("\\", "\\\\") + "'";
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Escapes an expression if necessary.
     *
     * @param expression the original expression
     * @return the armored expression
     */
    public static String armor(String expression) {
        if (!expression.startsWith("`") && !expression.endsWith("`") && (expression.contains("-") || expression.contains("."))) {
            return "`" + expression + "`";
        } else
            return expression;
    }


}
