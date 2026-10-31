package com.aetherdb.test;

import com.aetherdb.analytics.*;
import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.columnar.ZoneMap;
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
        // Ingest dataset:
        // row 1: val=10
        // row 2: val=20
        // row 3: val=30
        // row 4: val=NULL
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
        // COUNT(*) = 4 (all rows)
        assertEquals(4L, row.get(0));
        // COUNT(val) = 3 (excluding NULL)
        assertEquals(3L, row.get(1));
        // SUM(val) = 60.0
        assertEquals(60.0, (Double) row.get(2), 0.001);
        // AVG(val) = 60.0 / 3 = 20.0 (dividing by non-null count!)
        assertEquals(20.0, (Double) row.get(3), 0.001);
        // MIN(val) = 10.0
        assertEquals(10.0, (Double) row.get(4), 0.001);
        // MAX(val) = 30.0
        assertEquals(30.0, (Double) row.get(5), 0.001);
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

    @Test
    @DisplayName("ZoneMap pruning logic correctly handles boundary values")
    void testZoneMapBoundaryPruning() {
        ZoneMap zm = new ZoneMap(ColumnType.INT32);
        zm.update(ColumnValue.ofInt(10));
        zm.update(ColumnValue.ofInt(20));

        // Chunk range is [10, 20]
        // Target = 20
        // > 20: maxVal <= 20 -> can skip! (no value strictly > 20)
        assertTrue(zm.canSkipGreaterThan(ColumnValue.ofInt(20)));

        // >= 20: maxVal < 20 is false (maxVal is 20) -> CANNOT skip! (20 matches)
        assertFalse(zm.canSkipGreaterThanOrEquals(ColumnValue.ofInt(20)));

        // Target = 10
        // < 10: minVal >= 10 -> can skip! (no value strictly < 10)
        assertTrue(zm.canSkipLessThan(ColumnValue.ofInt(10)));

        // <= 10: minVal > 10 is false (minVal is 10) -> CANNOT skip! (10 matches)
        assertFalse(zm.canSkipLessThanOrEquals(ColumnValue.ofInt(10)));
    }

    @Test
    @DisplayName("GROUP BY queries aggregate per group key accurately")
    void testGroupByAggregations() throws Exception {
        List<Map<String, ColumnValue>> records = List.of(
                Map.of("dept", ColumnValue.ofString("ENG"), "salary", ColumnValue.ofDouble(100.0)),
                Map.of("dept", ColumnValue.ofString("ENG"), "salary", ColumnValue.ofDouble(200.0)),
                Map.of("dept", ColumnValue.ofString("SALES"), "salary", ColumnValue.ofDouble(150.0)),
                Map.of("dept", ColumnValue.ofString("SALES"), "salary", ColumnValue.ofDouble(250.0)),
                Map.of("dept", ColumnValue.ofString("HR"), "salary", ColumnValue.ofDouble(80.0))
        );
        storage.ingestColumnar("employees", records);

        QueryRequest req = new QueryRequest(
                "employees",
                List.of("COUNT(*)", "SUM(salary)", "AVG(salary)"),
                Collections.emptyList(),
                List.of("dept"),
                10
        );

        QueryResult res = queryEngine.execute(req);
        assertEquals(3, res.getRows().size()); // 3 departments

        Map<String, Double> avgByDept = new HashMap<>();
        for (List<Object> row : res.getRows()) {
            String dept = (String) row.get(0);
            Double avgSal = (Double) row.get(3);
            avgByDept.put(dept, avgSal);
        }

        assertEquals(150.0, avgByDept.get("ENG"), 0.01);
        assertEquals(200.0, avgByDept.get("SALES"), 0.01);
        assertEquals(80.0, avgByDept.get("HR"), 0.01);
    }

    @Test
    @DisplayName("LIKE filter evaluation with % wildcards")
    void testLikeOperator() throws Exception {
        List<Map<String, ColumnValue>> records = List.of(
                Map.of("name", ColumnValue.ofString("Alice Smith")),
                Map.of("name", ColumnValue.ofString("Bob Jones")),
                Map.of("name", ColumnValue.ofString("Charlie Smith"))
        );
        storage.ingestColumnar("names", records);

        QueryResult res = queryEngine.execute(new QueryRequest(
                "names",
                List.of("name"),
                List.of(new Predicate("name", FilterOperator.LIKE, ColumnValue.ofString("%Smith"))),
                Collections.emptyList(),
                10
        ));

        assertEquals(2, res.getRows().size());
    }
}
