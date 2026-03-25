import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class GaussDbCli {
    public static void main(String[] args) {
        try {
            CliArgs cliArgs = CliArgs.parse(args);
            Config config = Config.load(cliArgs.configPath);
            Class.forName(config.driverClassName);

            try (Connection connection = DriverManager.getConnection(config.jdbcUrl, config.username, config.password)) {
                connection.setAutoCommit(false);
                Object result = run(cliArgs, config, connection);
                connection.commit();
                printJson(result);
            }
        } catch (Throwable throwable) {
            Map<String, Object> error = new LinkedHashMap<String, Object>();
            error.put("ok", false);
            error.put("errorType", throwable.getClass().getName());
            error.put("message", throwable.getMessage());
            printJson(error);
            System.exit(1);
        }
    }

    private static Object run(CliArgs cliArgs, Config config, Connection connection) throws Exception {
        if ("tables".equals(cliArgs.command)) {
            return runTables(connection, cliArgs, config);
        }
        if ("describe".equals(cliArgs.command)) {
            return runDescribe(connection, cliArgs);
        }
        if ("sql".equals(cliArgs.command)) {
            return runSql(connection, cliArgs, config);
        }
        throw new IllegalArgumentException("Unsupported command: " + cliArgs.command);
    }

    private static Map<String, Object> runTables(Connection connection, CliArgs cliArgs, Config config) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, "%", new String[] {"TABLE", "VIEW"});
        try {
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            int maxRows = config.resolveMaxRows(cliArgs.maxRows);
            while (resultSet.next() && rows.size() < maxRows) {
                String schema = safeString(resultSet.getString("TABLE_SCHEM"));
                if (isSystemSchema(schema)) {
                    continue;
                }
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                row.put("TABLE_SCHEM", schema);
                row.put("TABLE_NAME", safeString(resultSet.getString("TABLE_NAME")));
                row.put("TABLE_TYPE", safeString(resultSet.getString("TABLE_TYPE")));
                rows.add(row);
            }

            List<String> columns = new ArrayList<String>();
            columns.add("TABLE_SCHEM");
            columns.add("TABLE_NAME");
            columns.add("TABLE_TYPE");

            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("ok", true);
            payload.put("command", "tables");
            payload.put("kind", "query");
            payload.put("columns", columns);
            payload.put("rowCount", rows.size());
            payload.put("maxRows", maxRows);
            payload.put("truncated", rows.size() >= maxRows);
            payload.put("rows", rows);
            return payload;
        } finally {
            resultSet.close();
        }
    }

    private static Map<String, Object> runDescribe(Connection connection, CliArgs cliArgs) throws SQLException {
        if (cliArgs.tableName == null || cliArgs.tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("describe requires --table <table> or --table <schema.table>");
        }

        String schemaName = null;
        String tableName = cliArgs.tableName;
        if (cliArgs.tableName.contains(".")) {
            String[] parts = cliArgs.tableName.split("\\.", 2);
            schemaName = parts[0];
            tableName = parts[1];
        }

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getColumns(null, schemaName, tableName, "%");
        try {
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            while (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                row.put("TABLE_SCHEM", safeString(resultSet.getString("TABLE_SCHEM")));
                row.put("TABLE_NAME", safeString(resultSet.getString("TABLE_NAME")));
                row.put("ORDINAL_POSITION", resultSet.getInt("ORDINAL_POSITION"));
                row.put("COLUMN_NAME", safeString(resultSet.getString("COLUMN_NAME")));
                row.put("TYPE_NAME", safeString(resultSet.getString("TYPE_NAME")));
                row.put("COLUMN_SIZE", resultSet.getInt("COLUMN_SIZE"));
                row.put("NULLABLE", resultSet.getInt("NULLABLE"));
                row.put("COLUMN_DEF", safeString(resultSet.getString("COLUMN_DEF")));
                rows.add(row);
            }

            List<String> columns = new ArrayList<String>();
            columns.add("TABLE_SCHEM");
            columns.add("TABLE_NAME");
            columns.add("ORDINAL_POSITION");
            columns.add("COLUMN_NAME");
            columns.add("TYPE_NAME");
            columns.add("COLUMN_SIZE");
            columns.add("NULLABLE");
            columns.add("COLUMN_DEF");

            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("ok", true);
            payload.put("command", "describe");
            payload.put("kind", "query");
            payload.put("columns", columns);
            payload.put("rowCount", rows.size());
            payload.put("maxRows", rows.size());
            payload.put("truncated", false);
            payload.put("rows", rows);
            return payload;
        } finally {
            resultSet.close();
        }
    }

    private static Map<String, Object> runSql(Connection connection, CliArgs cliArgs, Config config) throws Exception {
        String sql = cliArgs.sqlText;
        if (sql == null && cliArgs.sqlFile != null) {
            sql = new String(Files.readAllBytes(Paths.get(cliArgs.sqlFile)), StandardCharsets.UTF_8);
        }
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("sql command requires --sql <text> or --file <path>");
        }

        int maxRows = config.resolveMaxRows(cliArgs.maxRows);
        int timeoutSeconds = config.resolveTimeoutSeconds(cliArgs.timeoutSeconds);

        Statement statement = connection.createStatement();
        statement.setQueryTimeout(timeoutSeconds);
        statement.setMaxRows(maxRows);

        try {
            boolean hasResultSet = statement.execute(sql);
            List<Map<String, Object>> statements = new ArrayList<Map<String, Object>>();
            int statementIndex = 0;

            while (true) {
                Map<String, Object> current = new LinkedHashMap<String, Object>();
                current.put("statementIndex", statementIndex);

                if (hasResultSet) {
                    ResultSet resultSet = statement.getResultSet();
                    current.putAll(queryPayload(resultSet, maxRows));
                    resultSet.close();
                } else {
                    int updateCount = statement.getUpdateCount();
                    if (updateCount == -1) {
                        break;
                    }
                    current.put("kind", "update");
                    current.put("affectedRows", updateCount);
                }

                statements.add(current);
                statementIndex += 1;
                hasResultSet = statement.getMoreResults();
            }

            Map<String, Object> payload = new LinkedHashMap<String, Object>();
            payload.put("ok", true);
            payload.put("command", "sql");
            payload.put("statementCount", statements.size());
            payload.put("results", statements);
            return payload;
        } catch (SQLException sqlException) {
            connection.rollback();
            throw sqlException;
        } finally {
            statement.close();
        }
    }

    private static Map<String, Object> queryPayload(ResultSet resultSet, int maxRows) throws SQLException {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("kind", "query");

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<String> columns = new ArrayList<String>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(metaData.getColumnLabel(i));
        }
        payload.put("columns", columns);

        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount += 1;
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(columns.get(i - 1), normalizeValue(resultSet.getObject(i)));
            }
            rows.add(row);
        }

        payload.put("rowCount", rowCount);
        payload.put("maxRows", maxRows);
        payload.put("truncated", rowCount >= maxRows);
        payload.put("rows", rows);
        return payload;
    }

    private static Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Double
            || value instanceof Float || value instanceof Boolean || value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant().toString();
        }
        if (value instanceof LocalDateTime) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return value.toString();
    }

    private static String safeString(String value) {
        return value == null ? "" : value;
    }

    private static boolean isSystemSchema(String schemaName) {
        if (schemaName == null) {
            return false;
        }
        String lower = schemaName.toLowerCase(Locale.ROOT);
        return lower.startsWith("pg_")
            || "information_schema".equals(lower)
            || "sys".equals(lower)
            || "system".equals(lower);
    }

    private static void printJson(Object value) {
        System.out.println(toJson(value));
    }

    private static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof Map) {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            boolean first = true;
            for (Object entryObject : ((Map<?, ?>) value).entrySet()) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entryObject;
                if (!first) {
                    builder.append(",");
                }
                builder.append(toJson(String.valueOf(entry.getKey())));
                builder.append(":");
                builder.append(toJson(entry.getValue()));
                first = false;
            }
            builder.append("}");
            return builder.toString();
        }
        if (value instanceof List) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) {
                    builder.append(",");
                }
                builder.append(toJson(item));
                first = false;
            }
            builder.append("]");
            return builder.toString();
        }
        return toJson(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            switch (current) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (current < 0x20) {
                        builder.append(String.format(Locale.ROOT, "\\u%04x", (int) current));
                    } else {
                        builder.append(current);
                    }
            }
        }
        return builder.toString();
    }

    private static final class Config {
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private final String driverClassName;
        private final int maxRows;
        private final int timeoutSeconds;

        private Config(String jdbcUrl, String username, String password, String driverClassName, int maxRows, int timeoutSeconds) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.driverClassName = driverClassName;
            this.maxRows = maxRows;
            this.timeoutSeconds = timeoutSeconds;
        }

        private static Config load(String configPath) throws IOException {
            if (configPath == null) {
                throw new IllegalArgumentException("Missing --config");
            }

            Properties properties = new Properties();
            Path path = Paths.get(configPath);
            if (!Files.exists(path)) {
                throw new IllegalArgumentException("Config file does not exist: " + configPath);
            }

            InputStream inputStream = Files.newInputStream(path);
            try {
                properties.load(inputStream);
            } finally {
                inputStream.close();
            }

            String driverClassName = firstNonBlank(
                properties.getProperty("jdbc.driver.class"),
                System.getenv("GAUSS_DB_DRIVER_CLASS"),
                "com.huawei.gauss.jdbc.ZenithDriver"
            );
            String jdbcUrl = firstNonBlank(properties.getProperty("jdbc.url"), buildJdbcUrl(properties, driverClassName));
            String username = required(properties, "jdbc.username");
            String password = required(properties, "jdbc.password");
            int maxRows = Integer.parseInt(properties.getProperty("jdbc.max.rows", "200"));
            int timeoutSeconds = Integer.parseInt(properties.getProperty("jdbc.query.timeout.seconds", "60"));
            return new Config(jdbcUrl, username, password, driverClassName, maxRows, timeoutSeconds);
        }

        private static String buildJdbcUrl(Properties properties, String driverClassName) {
            String host = firstNonBlank(properties.getProperty("db.host"), properties.getProperty("jdbc.host"));
            if (host == null) {
                throw new IllegalArgumentException("Missing config value: jdbc.url or db.host");
            }

            String port = firstNonBlank(properties.getProperty("db.port"), properties.getProperty("jdbc.port"), defaultPort(driverClassName));
            if (isOpenGaussDriver(driverClassName)) {
                String database = firstNonBlank(properties.getProperty("db.database"), properties.getProperty("jdbc.database"), "postgres");
                return "jdbc:opengauss://" + host + ":" + port + "/" + database;
            }

            return "jdbc:zenith:@" + host + ":" + port;
        }

        private static boolean isOpenGaussDriver(String driverClassName) {
            return driverClassName != null && driverClassName.toLowerCase(Locale.ROOT).contains("opengauss");
        }

        private static String defaultPort(String driverClassName) {
            return isOpenGaussDriver(driverClassName) ? "5432" : "1888";
        }

        private static String required(Properties properties, String key) {
            String value = properties.getProperty(key);
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing config value: " + key);
            }
            return value.trim();
        }

        private static String firstNonBlank(String... values) {
            for (String value : values) {
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
            return null;
        }

        private int resolveMaxRows(Integer override) {
            return override != null ? override : maxRows;
        }

        private int resolveTimeoutSeconds(Integer override) {
            return override != null ? override : timeoutSeconds;
        }
    }

    private static final class CliArgs {
        private final String command;
        private final String configPath;
        private final String sqlText;
        private final String sqlFile;
        private final String tableName;
        private final Integer maxRows;
        private final Integer timeoutSeconds;

        private CliArgs(String command, String configPath, String sqlText, String sqlFile, String tableName, Integer maxRows, Integer timeoutSeconds) {
            this.command = command;
            this.configPath = configPath;
            this.sqlText = sqlText;
            this.sqlFile = sqlFile;
            this.tableName = tableName;
            this.maxRows = maxRows;
            this.timeoutSeconds = timeoutSeconds;
        }

        private static CliArgs parse(String[] args) {
            if (args.length == 0) {
                throw new IllegalArgumentException("Usage: gauss-db [--config path] <sql|tables|describe> [options]");
            }

            String configPath = null;
            String command = null;
            String sqlText = null;
            String sqlFile = null;
            String tableName = null;
            Integer maxRows = null;
            Integer timeoutSeconds = null;

            int index = 0;
            while (index < args.length) {
                String current = args[index];
                if ("--config".equals(current)) {
                    index += 1;
                    configPath = requireValue(args, index, "--config");
                } else if ("--sql".equals(current)) {
                    index += 1;
                    sqlText = requireValue(args, index, "--sql");
                } else if ("--file".equals(current)) {
                    index += 1;
                    sqlFile = requireValue(args, index, "--file");
                } else if ("--table".equals(current)) {
                    index += 1;
                    tableName = requireValue(args, index, "--table");
                } else if ("--max-rows".equals(current)) {
                    index += 1;
                    maxRows = Integer.valueOf(requireValue(args, index, "--max-rows"));
                } else if ("--timeout-seconds".equals(current)) {
                    index += 1;
                    timeoutSeconds = Integer.valueOf(requireValue(args, index, "--timeout-seconds"));
                } else if (command == null) {
                    command = current;
                } else {
                    throw new IllegalArgumentException("Unexpected argument: " + current);
                }
                index += 1;
            }

            if (command == null) {
                throw new IllegalArgumentException("Missing command. Use sql, tables, or describe.");
            }

            return new CliArgs(command, configPath, sqlText, sqlFile, tableName, maxRows, timeoutSeconds);
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }
    }
}
