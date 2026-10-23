import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Two independent JDBC connections held open until both aggregate queries finish. */
public final class ConcurrentClients {
    private static final int QUERY_TIMEOUT_SECONDS = 240;
    private static final List<List<String>> EXPECTED = Arrays.asList(
        Arrays.asList("2026-10-01", "3", "400.00"),
        Arrays.asList("2026-10-02", "3", "500.00"));

    private static String now() { return Instant.now().toString(); }

    private static String quoted(String value) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': out.append("\\\\"); break;
                case '"': out.append("\\\""); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) out.append(String.format("\\u%04x", (int)c));
                    else out.append(c);
            }
        }
        return out.append('"').toString();
    }

    private static String json(Object value) {
        if (value == null) return "null";
        if (value instanceof String) return quoted((String)value);
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map) {
            List<String> fields = new ArrayList<>();
            for (Map.Entry<?, ?> field : ((Map<?, ?>)value).entrySet()) {
                fields.add(quoted(field.getKey().toString()) + ":" + json(field.getValue()));
            }
            return "{" + String.join(",", fields) + "}";
        }
        if (value instanceof Iterable) {
            List<String> items = new ArrayList<>();
            for (Object item : (Iterable<?>)value) items.add(json(item));
            return "[" + String.join(",", items) + "]";
        }
        throw new IllegalArgumentException("Unsupported JSON value");
    }

    private static String remoteDatabase(String url) throws Exception {
        if (!url.startsWith("jdbc:hive2://")) throw new IllegalArgumentException("Remote Hive JDBC URL required");
        URI target = new URI(url.substring(5));
        if (target.getHost() == null || target.getPort() < 1 || target.getPort() > 65535
                || target.getUserInfo() != null || target.getQuery() != null || target.getFragment() != null
                || target.getPath() == null || !target.getPath().matches("/[a-z][a-z0-9_]{0,39}")) {
            throw new IllegalArgumentException("Explicit host, port and database required; embedded URLs rejected");
        }
        return target.getPath().substring(1);
    }

    private static void fail(Map<String, Object> result, String error) {
        result.put("ok", false);
        String previous = (String)result.get("error");
        result.put("error", previous == null ? error : previous + "; " + error);
    }

    private static Map<String, Object> runClient(String name, String url, String user, String query,
            CyclicBarrier ready, CountDownLatch finished, AtomicInteger active, AtomicInteger maximum) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("client", name);
        result.put("ok", false);
        Connection connection = null;
        boolean counted = false;
        try {
            connection = DriverManager.getConnection(url, user, "");
            result.put("opened_at", now());
            counted = true;
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            // Hive alpha2 does not implement DatabaseMetaData.getURL().
            result.put("requested_jdbc_url", url);
            // No SQL, including SET, is issued until both connections have opened.
            ready.await(90, TimeUnit.SECONDS);
            result.put("both_connections_ready_at", now());
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                statement.execute("SET hive.execution.engine=mr");
                statement.execute("SET hive.fetch.task.conversion=none");
                statement.execute("SET hive.compute.query.using.stats=false");
                statement.execute("SET hive.query.results.cache.enabled=false");
                statement.execute("SET hive.exec.mode.local.auto=false");
                result.put("query_started_at", now());
                List<List<String>> rows = new ArrayList<>();
                try (ResultSet records = statement.executeQuery(query)) {
                    while (records.next()) {
                        BigDecimal total = records.getBigDecimal(3);
                        if (total == null) throw new IllegalStateException("SUM(amount) returned NULL");
                        rows.add(Arrays.asList(records.getString(1), Long.toString(records.getLong(2)),
                            total.setScale(2).toPlainString()));
                    }
                } finally {
                    result.put("query_finished_at", now());
                }
                result.put("rows", rows);
                result.put("ok", EXPECTED.equals(rows));
                if (!EXPECTED.equals(rows)) fail(result, "Aggregate rows differ from the six-row fixture");
            }
        } catch (Exception error) {
            fail(result, error.getClass().getSimpleName() + ": " + error.getMessage());
        } finally {
            finished.countDown();
            if (connection != null) {
                // Keep the first completed session open while the other client is still executing.
                try {
                    if (!finished.await(QUERY_TIMEOUT_SECONDS + 100L, TimeUnit.SECONDS)) {
                        fail(result, "Other client did not finish within the bounded wait");
                    }
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    fail(result, "Interrupted while holding both sessions open");
                }
                try {
                    connection.close();
                    result.put("closed_at", now());
                } catch (Exception error) {
                    fail(result, "Connection close failed: " + error.getMessage());
                } finally {
                    if (counted) active.decrementAndGet();
                }
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            throw new IllegalArgumentException("Usage: ConcurrentClients JDBC_URL USER OUTPUT_JSON [TABLE]");
        }
        String database = remoteDatabase(args[0]);
        String table = args.length == 4 ? args[3] : "sales";
        if (!table.matches("[a-z][a-z0-9_]{0,39}")) throw new IllegalArgumentException("Invalid table identifier");
        String query = "SELECT dt, COUNT(*), SUM(amount) FROM " + database + "." + table + " GROUP BY dt ORDER BY dt";
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("started_at", now());
        report.put("jdbc_url", args[0]);
        report.put("query", query);
        report.put("query_timeout_seconds", QUERY_TIMEOUT_SECONDS);
        report.put("expected_rows", EXPECTED);
        report.put("session_measurement", "Independent JDBC connections successfully opened and held until both queries finish; client-side observation, not a server metric");
        report.put("execution_settings", Arrays.asList("hive.execution.engine=mr", "hive.fetch.task.conversion=none",
            "hive.compute.query.using.stats=false", "hive.query.results.cache.enabled=false", "hive.exec.mode.local.auto=false"));
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        DriverManager.setLoginTimeout(60);
        CyclicBarrier ready = new CyclicBarrier(2);
        CountDownLatch finished = new CountDownLatch(2);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<Map<String, Object>>> tasks = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            final String clientName = "client_" + i;
            tasks.add(pool.submit(() -> runClient(clientName, args[0], args[1], query, ready, finished, active, maximum)));
        }
        List<Map<String, Object>> clients = new ArrayList<>();
        try {
            for (Future<Map<String, Object>> task : tasks) clients.add(task.get(580, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        boolean ok = maximum.get() == 2;
        for (Map<String, Object> client : clients) ok &= Boolean.TRUE.equals(client.get("ok"));
        report.put("clients", clients);
        report.put("max_simultaneous_sessions", maximum.get());
        report.put("finished_at", now());
        report.put("ok", ok);
        Path output = Paths.get(args[2]);
        Files.write(output, (json(report) + "\n").getBytes(StandardCharsets.UTF_8));
        System.out.println(ok ? "PASS: two overlapping JDBC sessions returned the expected partition totals" : "FAIL: JDBC concurrency or data check failed");
        if (!ok) System.exit(1);
    }
}
