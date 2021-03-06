package de.kune.mysqlsync;

import de.kune.mysqlsync.anonymizer.FieldAnonymizer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MySQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static de.kune.mysqlsync.TestUtil.*;
import static de.kune.mysqlsync.anonymizer.FieldAnonymizer.DEFAULT_ANONYMIZERS;
import static org.assertj.core.api.Assertions.assertThat;

public class DataSourceSynchronizerIT {

    private static DataSourceSynchronizer synchronizer;
    private static DataSourceSynchronizer anonymizingSynchronzier;
    private static DataSourceSynchronizer synchronizerWithExclusions;
    private static final String SOURCE_SCHEMA = "test_source_schema";
    private static final String TARGET_SCHEMA = "test_target_schema";
    @ClassRule
    public static MySQLContainer sourceDatabase = new MySQLContainer().withUsername("test").withPassword("test").withDatabaseName(SOURCE_SCHEMA);
    @ClassRule
    public static MySQLContainer targetDatabase = new MySQLContainer().withUsername("test").withPassword("test").withDatabaseName(TARGET_SCHEMA);

    private final static Map<String, String> ONLY_PRIMARY_KEY = new LinkedHashMap<String, String>() {{
        put("customerNumber", "564232");
        put("emailAddress", "someone@somewhere.com");
    }};

    private final static Map<String, String> ONLY_PRIMARY_KEY_ANONYMIZED = new LinkedHashMap<String, String>(ONLY_PRIMARY_KEY) {{
        put("emailAddress", "1664503933874841391");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY = new LinkedHashMap<String, String>() {{
        put("creationDate", "2019-07-12 10:52:11");
        put("lastModifiedDate", null);
        put("customerNumber", "564232");
        put("emailAddress", "someone@somewhere.com");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY_ANONYMIZED = new LinkedHashMap<String, String>(NO_PRIMARY_KEY) {{
        put("emailAddress", "1664503933874841391");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY_REDUCED = new LinkedHashMap<String, String>(NO_PRIMARY_KEY) {{
        remove("customerNumber");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY_REDUCED_EXCLUDED_FIELDS = new LinkedHashMap<String, String>(NO_PRIMARY_KEY_REDUCED) {{
        put("emailAddress", "");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY_UPDATED = new LinkedHashMap<String, String>(NO_PRIMARY_KEY) {{
        put("lastModifiedDate", "2019-07-12 10:59:30");
        put("emailAddress", "someone@somewhereelse.com");
    }};

    private final static Map<String, String> NO_PRIMARY_KEY_UPDATED_ANONYMIZED = new LinkedHashMap<String, String>(NO_PRIMARY_KEY_ANONYMIZED) {{
        put("lastModifiedDate", "2019-07-12 10:59:30");
        put("emailAddress", "8229633782615946672");
    }};

    private final static Map<String, String> USER = new LinkedHashMap<String, String>() {{
        put("id", "1");
        put("creationDate", "2019-07-12 10:52:11");
        put("lastModifiedDate", null);
        put("username", "hans.hansen");
        put("customerId", "1");
    }};

    private final static Map<String, String> USER_ANONYMIZED = new LinkedHashMap<String, String>(USER) {{
        put("username", "user-1");
    }};

    private final static Map<String, String> CUSTOMER = new LinkedHashMap<String, String>() {{
        put("id", "1");
        put("creationDate", "2019-07-12 10:52:11");
        put("lastModifiedDate", null);
        put("customerNumber", "564232");
        put("emailAddress", "someone@somewhere.com");
        put("firstname", "Hans");
        put("lastname", "Hansen");
        put("title", "DR");
        put("gender", "MALE");
        put("uuid", "56b579e1-a482-11e9-aa9f-0242ac110004");
        put("newsletter", "0");
        put("version", "0");
    }};

    private final static Map<String, String> CUSTOMER_ANONYMIZED = new LinkedHashMap<String, String>(CUSTOMER) {{
        put("emailAddress", "1664503933874841391");
        put("firstname", "Shauna");
        put("lastname", "Olson");
    }};

    private final static Map<String, String> CUSTOMER_REDUCED = new LinkedHashMap<String, String>(CUSTOMER) {{
        remove("newsletter");
        remove("uuid");
    }};

    private final static Map<String, String> CUSTOMER_REDUCED_EXCLUDED_FIELDS = new LinkedHashMap<String, String>(CUSTOMER_REDUCED) {{
        put("emailAddress", "");
        put("gender", null);
    }};

    private final static Map<String, String> CUSTOMER_UPDATED = new LinkedHashMap<String, String>(CUSTOMER) {{
        put("lastModifiedDate", "2019-07-12 10:59:28");
        put("emailAddress", "someone@somewhereelse.com");
        put("newsletter", "1");
    }};

    private final static Map<String, String> CUSTOMER_UPDATED_ANONYMIZED = new LinkedHashMap<String, String>(CUSTOMER_ANONYMIZED) {{
        put("lastModifiedDate", "2019-07-12 10:59:28");
        put("emailAddress", "8229633782615946672");
        put("newsletter", "1");
    }};

    @BeforeClass
    public static void beforeClass() throws IOException {
        synchronizer = DataSourceSynchronizer.builder().source(dataSource(sourceDatabase)).target(dataSource(targetDatabase)).build();
        anonymizingSynchronzier = DataSourceSynchronizer.builder().source(dataSource(sourceDatabase)).target(dataSource(targetDatabase)).anonymizerMap(
                anonymizers()).build();
        synchronizerWithExclusions = DataSourceSynchronizer.builder().source(dataSource(sourceDatabase)).target(dataSource(targetDatabase))
                .exclude(".*\\.emailAddress")
                .exclude(".*\\.gender")
                .build();
    }

    private static Map<Pattern, FieldAnonymizer> anonymizers() {
        Map<Pattern, FieldAnonymizer> anonymizerMap = new HashMap<>(DEFAULT_ANONYMIZERS);
        anonymizerMap.put(Pattern.compile("^user\\.username$"), FieldAnonymizer.findByName("generic(user-${id})"));
        return anonymizerMap;
    }

    @Before
    public void before() throws SQLException {
        init(sourceDatabase, SOURCE_SCHEMA);
        init(targetDatabase, TARGET_SCHEMA);
    }

    @Test
    public void synchronize_empty_schema() throws IOException, SQLException {
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
    }

    @Test
    public void synchronize_empty_table() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).isEmpty();
    }

    @Test
    public void synchronize() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY);
    }

    @Test
    public void synchronizeAnonymized() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        anonymizingSynchronzier.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "user")).containsOnlyOnce(USER_ANONYMIZED);
    }

    @Test
    public void updateFullSync() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY);
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert", "update");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_UPDATED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_UPDATED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY);
    }

    @Test
    public void updateIncrementalSync() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY);
        run(sourceDatabase, SOURCE_SCHEMA, "update");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, true, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_UPDATED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_UPDATED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY);
    }

    @Test
    public void updateIncrementalSyncAnonymized() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_schema");
        anonymizingSynchronzier.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY_ANONYMIZED);
        run(sourceDatabase, SOURCE_SCHEMA, "update");
        anonymizingSynchronzier.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, true, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_UPDATED_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_UPDATED_ANONYMIZED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "only_primary_key")).containsOnlyOnce(ONLY_PRIMARY_KEY_ANONYMIZED);
    }

    @Test
    public void synchronize_reduced_fields() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_reduced_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(sourceDatabase, SOURCE_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_REDUCED);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_REDUCED);
    }

    @Test
    public void synchronize_empty_table_reduced_fields() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema");
        init(targetDatabase, TARGET_SCHEMA, "create_reduced_schema");
        synchronizer.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).isEmpty();
    }

    @Test
    public void synchronize_reduced_fields_with_exclusions() throws IOException, SQLException {
        init(sourceDatabase, SOURCE_SCHEMA, "create_schema", "insert");
        init(targetDatabase, TARGET_SCHEMA, "create_reduced_schema");
        synchronizerWithExclusions.sync(SOURCE_SCHEMA, TARGET_SCHEMA, null, false, false, false, false, false, false, 50);
        assertThat(queryAll(sourceDatabase, SOURCE_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "customer")).containsOnlyOnce(CUSTOMER_REDUCED_EXCLUDED_FIELDS);
        assertThat(queryAll(targetDatabase, TARGET_SCHEMA, "no_primary_key")).containsOnlyOnce(NO_PRIMARY_KEY_REDUCED_EXCLUDED_FIELDS);
    }

}
