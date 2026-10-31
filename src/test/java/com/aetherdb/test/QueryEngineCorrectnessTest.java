package com.aetherdb.test;

import com.aetherdb.analytics.*;
import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.engine.HybridStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class QueryEngineCorrectnessTest {

    private Path tempDir;
    private HybridStorageManager storage;
    private QueryEngine queryEngine;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_query_test_");
        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        config.validateAndCreateDirectories();

        storage = new HybridStorageManager(config);
        queryEngine = new QueryEngine(storage, 1024);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Independent aggregate state: multiple expressions in one query maintain isolated accumulators")
    void testIndependentAggregateAccumulators() throws Exception {
        List<Map<String, ColumnValue>> records = new ArrayList<>();
        records.add(Map.of("id", ColumnValue.ofLong(1), "val", ColumnValue.ofDouble(10.0)));
        records.add(Map.of("id", ColumnValue.ofLong(2), "val", ColumnValue.ofDouble(20.0)));
        records.add(Map.of("id", ColumnValue.ofLong(3), "val", ColumnValue.ofDouble(30.0)));

        Map<String, ColumnValue> nullRow = new LinkedHashMap<>();
        nullRow.put("id", ColumnValue.ofLong(4));
        nullRow.put("val", ColumnValue.ofNull(ColumnType.FLOAT64));
        records.add(nullRow);

        storage.ingestColumnar("metrics", records);

        QueryRequest request = new QueryRequest(
                "metrics",
                List.of("COUNT(*)", "COUNT(val)", "SUM(val)", "AVG(val)", "MIN(val)", "MAX(val)"),
                Collections.emptyList(),
                Collections.emptyList(),
                10
        );

        QueryResult result = queryEngine.execute(request);
        assertNotNull(result);
        assertEquals(1, result.getRows().size());

        List<Object> row = result.getRows().get(0);
        assertEquals(4L, row.get(0));
        assertEquals(3L, row.get(1));
        assertEquals(60.0, (Double) row.get(2), 0.001);
        assertEquals(20.0, (Double) row.get(3), 0.001);
        assertEquals(10.0, (Double) row.get(4), 0.001);
        assertEquals(30.0, (Double) row.get(5), 0.001);
    }

    @Test
    @DisplayName("Large integer precision: SUM preserves full 64-bit precision without double rounding")
    void testLargeIntegerSumPreservesPrecision() throws Exception {
        long large1 = 3_000_000_000_000_001L;
        long large2 = 4_000_000_000_000_002L;
        long expectedSum = 7_000_000_000_000_003L;

        List<Map<String, ColumnValue>> records = List.of(
                Map.of("big_num", ColumnValue.ofLong(large1)),
                Map.of("big_num", ColumnValue.ofLong(large2))
        );
        storage.ingestColumnar("large_ints", records);

        QueryResult res = queryEngine.execute(new QueryRequest(
                "large_ints",
                List.of("SUM(big_num)"),
                Collections.emptyList(),
                Collections.emptyList(),
                10
        ));

        assertEquals(1, res.getRows().size());
        Object sumVal = res.getRows().get(0).get(0);
        assertInstanceOf(Long.class, sumVal);
        assertEquals(expectedSum, sumVal);
    }

    @Test
    @DisplayName("Empty table aggregates: SQL-standard semantics (COUNT=0, SUM/AVG=null)")
    void testEmptyTableAggregates() {
        QueryRequest req = new QueryRequest(
                "non_existent_table",
                List.of("COUNT(*)", "SUM(amount)", "AVG(amount)", "MIN(amount)", "MAX(amount)"),
                Collections.emptyList(),
                Collections.emptyList(),
                10
        );

        QueryResult res = queryEngine.execute(req);
        assertEquals(1, res.getRows().size());
        List<Object> row = res.getRows().get(0);
        assertEquals(0L, row.get(0)); // COUNT(*)
        assertNull(row.get(1));       // SUM
        assertNull(row.get(2));       // AVG
        assertNull(row.get(3));       // MIN
        assertNull(row.get(4));       // MAX
    }

    @Test
    @DisplayName("Validation: non-aggregate expressions in GROUP BY queries are strictly rejected")
    void testInvalidGroupByExpressionRejected() throws Exception {
        storage.ingestColumnar("dept_data", List.of(
                Map.of("dept", ColumnValue.ofString("eng"), "salary", ColumnValue.ofLong(100), "name", ColumnValue.ofString("alice"))
        ));

        // 'name' is in SELECT but neither in GROUP BY nor in an aggregate function
        QueryRequest invalidReq = new QueryRequest(
                "dept_data",
                List.of("dept", "name", "AVG(salary)"),
                Collections.emptyList(),
                List.of("dept"),
                10
        );

        assertThrows(IllegalArgumentException.class, () -> {
            queryEngine.execute(invalidReq);
        });
    }

    @Test
    @DisplayName("Validation: unknown column in SELECT is rejected")
    void testUnknownColumnRejected() throws Exception {
        storage.ingestColumnar("items", List.of(Map.of("item_id", ColumnValue.ofLong(1))));

        QueryRequest badReq = new QueryRequest(
                "items",
                List.of("non_existent_col"),
                Collections.emptyList(),
                Collections.emptyList(),
                10
        );

        assertThrows(IllegalArgumentException.class, () -> {
            queryEngine.execute(badReq);
        });
    }

    @Test
    @DisplayName("Boundary comparisons: >= and <= include exact boundary values")
    void testBoundaryComparisonOperators() throws Exception {
        List<Map<String, ColumnValue>> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            records.add(Map.of("num", ColumnValue.ofInt(i)));
        }
        storage.ingestColumnar("numbers", records);

        // >= 5 must return 6 rows (5, 6, 7, 8, 9, 10)
        QueryResult gteResult = queryEngine.execute(new QueryRequest(
                "numbers",
                List.of("num"),
                List.of(new Predicate("num", FilterOperator.GREATER_THAN_OR_EQUALS, ColumnValue.ofInt(5))),
                Collections.emptyList(),
                100
        ));
        assertEquals(6, gteResult.getRows().size());

        // > 5 must return 5 rows (6, 7, 8, 9, 10)
        QueryResult gtResult = queryEngine.execute(new QueryRequest(
                "numbers",
                List.of("num"),
                List.of(new Predicate("num", FilterOperator.GREATER_THAN, ColumnValue.ofInt(5))),
                Collections.emptyList(),
                100
        ));
        assertEquals(5, gtResult.getRows().size());

        // <= 5 must return 5 rows (1, 2, 3, 4, 5)
        QueryResult lteResult = queryEngine.execute(new QueryRequest(
                "numbers",
                List.of("num"),
                List.of(new Predicate("num", FilterOperator.LESS_THAN_OR_EQUALS, ColumnValue.ofInt(5))),
                Collections.emptyList(),
                100
        ));
        assertEquals(5, lteResult.getRows().size());

        // != 5 must return 9 rows
        QueryResult neResult = queryEngine.execute(new QueryRequest(
                "numbers",
                List.of("num"),
                List.of(new Predicate("num", FilterOperator.NOT_EQUALS, ColumnValue.ofInt(5))),
                Collections.emptyList(),
                100
        ));
        assertEquals(9, neResult.getRows().size());
    }
}
