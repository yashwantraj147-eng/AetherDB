package com.aetherdb.test;

import com.aetherdb.analytics.FilterOperator;
import com.aetherdb.analytics.Predicate;
import com.aetherdb.analytics.QueryEngine;
import com.aetherdb.analytics.QueryRequest;
import com.aetherdb.analytics.QueryResult;
import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.BloomFilter;
import com.aetherdb.common.MurmurHash3;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.DataRecord;
import com.aetherdb.partition.ConsistentHashRouter;
import com.aetherdb.storage.columnar.ColumnDecoder;
import com.aetherdb.storage.columnar.ColumnEncoder;
import com.aetherdb.storage.columnar.ZoneMap;
import com.aetherdb.storage.engine.HybridStorageManager;

import java.io.File;
import java.util.*;

/**
 * Production validation test suite for AetherDB.
 * Requires zero external test dependencies; executes unit and integration tests.
 */
public class TestRunner {

    private static int testsRun = 0;
    private static int testsPassed = 0;

    public static void main(String[] args) {
        System.out.println("========================================================");
        System.out.println("  AetherDB Verification & Test Suite");
        System.out.println("========================================================\n");

        runTest("MurmurHash3 & BloomFilter Accuracy", TestRunner::testBloomFilter);
        runTest("Columnar RLE and Dictionary Encoders", TestRunner::testColumnarEncoders);
        runTest("ZoneMap Predicate Pushdown", TestRunner::testZoneMapSkipping);
        runTest("Consistent Hash Ring Routing", TestRunner::testConsistentHashRing);
        runTest("End-to-End LSM Storage Engine (WAL, MemTable, SSTables, Compaction)", TestRunner::testLsmStorageEngine);
        runTest("End-to-End Columnar Analytics & Vectorized Query Engine", TestRunner::testColumnarAnalyticsEngine);

        System.out.println("\n========================================================");
        System.out.println("  Test Results: " + testsPassed + " / " + testsRun + " Passed.");
        if (testsPassed == testsRun) {
            System.out.println("  STATUS: ALL TESTS PASSED! System verified.");
        } else {
            System.out.println("  STATUS: TEST FAILURES DETECTED.");
        }
        System.out.println("========================================================");

        if (testsPassed != testsRun) {
            System.exit(1);
        }
    }

    private static void runTest(String name, TestCase testCase) {
        testsRun++;
        System.out.print("[TEST] " + name + " ... ");
        try {
            testCase.execute();
            testsPassed++;
            System.out.println("PASSED");
        } catch (Throwable t) {
            System.out.println("FAILED: " + t.getMessage());
            t.printStackTrace(System.out);
        }
    }

    @FunctionalInterface
    interface TestCase {
        void execute() throws Throwable;
    }

    private static void assertEquals(Object expected, Object actual, String msg) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError(msg + " Expected: " + expected + ", Actual: " + actual);
        }
    }

    private static void assertTrue(boolean condition, String msg) {
        if (!condition) {
            throw new AssertionError("Assertion failed: " + msg);
        }
    }

    private static void testBloomFilter() {
        BloomFilter bf = new BloomFilter(1000, 0.01);
        for (int i = 0; i < 500; i++) {
            bf.add("test-key-" + i);
        }

        // Test positive lookups (must have 0 false negatives)
        for (int i = 0; i < 500; i++) {
            assertTrue(bf.mightContain("test-key-" + i), "Bloom filter missing key " + i);
        }

        // Test negative lookups (false positive rate should be <= 3%)
        int falsePositives = 0;
        for (int i = 500; i < 1500; i++) {
            if (bf.mightContain("non-existent-key-" + i)) {
                falsePositives++;
            }
        }
        double fpr = (double) falsePositives / 1000.0;
        assertTrue(fpr < 0.05, "False positive rate too high: " + fpr);
    }

    private static void testColumnarEncoders() {
        // Test RLE
        List<ColumnValue> rleList = new ArrayList<>();
        for (int i = 0; i < 20; i++) rleList.add(ColumnValue.ofString("status_ok"));
        for (int i = 0; i < 30; i++) rleList.add(ColumnValue.ofString("status_error"));

        ColumnEncoder rleEncoder = new ColumnEncoder(ColumnType.STRING, 0.7);
        byte[] rleEncoded = rleEncoder.encode(rleList);

        ColumnDecoder decoder = new ColumnDecoder(ColumnType.STRING);
        List<ColumnValue> rleDecoded = decoder.decode(rleEncoded);

        assertEquals(rleList.size(), rleDecoded.size(), "RLE decoded size mismatch");
        for (int i = 0; i < rleList.size(); i++) {
            assertEquals(rleList.get(i), rleDecoded.get(i), "RLE value mismatch at " + i);
        }

        // Test Dictionary
        List<ColumnValue> dictList = new ArrayList<>();
        String[] regions = {"us-east", "us-west", "eu-central", "ap-south"};
        for (int i = 0; i < 200; i++) {
            dictList.add(ColumnValue.ofString(regions[i % regions.length]));
        }

        ColumnEncoder dictEncoder = new ColumnEncoder(ColumnType.STRING, 0.7);
        byte[] dictEncoded = dictEncoder.encode(dictList);

        List<ColumnValue> dictDecoded = decoder.decode(dictEncoded);
        assertEquals(dictList.size(), dictDecoded.size(), "Dictionary decoded size mismatch");
        for (int i = 0; i < dictList.size(); i++) {
            assertEquals(dictList.get(i), dictDecoded.get(i), "Dictionary value mismatch at " + i);
        }
    }

    private static void testZoneMapSkipping() {
        ZoneMap zm = new ZoneMap(ColumnType.INT32);
        zm.update(ColumnValue.ofInt(10));
        zm.update(ColumnValue.ofInt(50));
        zm.update(ColumnValue.ofInt(100));

        Predicate eqSkip = new Predicate("val", FilterOperator.EQUALS, ColumnValue.ofInt(200));
        assertTrue(eqSkip.canSkipChunk(zm), "Should skip equals 200");

        Predicate gtSkip = new Predicate("val", FilterOperator.GREATER_THAN, ColumnValue.ofInt(150));
        assertTrue(gtSkip.canSkipChunk(zm), "Should skip greater than 150");

        Predicate ltSkip = new Predicate("val", FilterOperator.LESS_THAN, ColumnValue.ofInt(5));
        assertTrue(ltSkip.canSkipChunk(zm), "Should skip less than 5");

        Predicate noSkip = new Predicate("val", FilterOperator.EQUALS, ColumnValue.ofInt(50));
        assertTrue(!noSkip.canSkipChunk(zm), "Should not skip 50");
    }

    private static void testConsistentHashRing() {
        ConsistentHashRouter router = new ConsistentHashRouter(32);
        router.addNode("node-1");
        router.addNode("node-2");
        router.addNode("node-3");

        assertEquals(3, router.getPhysicalNodes().size(), "Node count mismatch");
        assertEquals(3 * 32, router.getRingSize(), "Virtual ring size mismatch");

        String nodeA = router.routeKey("customer_key_999");
        String nodeB = router.routeKey("customer_key_999");
        assertEquals(nodeA, nodeB, "Consistent hash routing must be deterministic");
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    private static void testLsmStorageEngine() throws Exception {
        deleteRecursively(new File("./data/test_lsm"));

        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", "./data/test_lsm/wal");
        config.setProperty("storage.sstable.dir", "./data/test_lsm/sstable");
        config.setProperty("storage.columnar.dir", "./data/test_lsm/columnar");
        config.setProperty("storage.data.dir", "./data/test_lsm/storage");
        config.setProperty("lsm.memtable.max.bytes", "4096"); // Small threshold to trigger SSTable flushes
        config.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(config);

        // Insert records
        for (int i = 0; i < 200; i++) {
            engine.put("user:" + i, "data_value_" + i);
        }

        // Verify point gets
        for (int i = 0; i < 200; i++) {
            DataRecord rec = engine.get("user:" + i);
            assertTrue(rec != null, "Key user:" + i + " must exist");
            assertEquals("data_value_" + i, rec.getValueAsString(), "Value mismatch");
        }

        // Delete some records and test tombstone
        engine.delete("user:42");
        assertTrue(engine.get("user:42") == null, "Deleted key user:42 must return null");

        // Flush and compact
        engine.flushMemTable();
        engine.compact();

        // Verify remaining records persist across compaction
        assertTrue(engine.get("user:42") == null, "Compacted tombstone must remain deleted");
        DataRecord remaining = engine.get("user:99");
        assertTrue(remaining != null, "Key user:99 must survive compaction");
        assertEquals("data_value_99", remaining.getValueAsString(), "Compacted value mismatch");

        engine.close();
    }

    private static void testColumnarAnalyticsEngine() throws Exception {
        deleteRecursively(new File("./data/test_col"));

        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", "./data/test_col/wal");
        config.setProperty("storage.sstable.dir", "./data/test_col/sstable");
        config.setProperty("storage.columnar.dir", "./data/test_col/columnar");
        config.setProperty("storage.data.dir", "./data/test_col/storage");
        config.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(config);

        // Ingest columnar records
        List<Map<String, ColumnValue>> batch = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            Map<String, ColumnValue> row = new LinkedHashMap<>();
            row.put("id", ColumnValue.ofLong(i));
            row.put("region", ColumnValue.ofString(i % 2 == 0 ? "us-east" : "us-west"));
            row.put("revenue", ColumnValue.ofDouble(10.0 * i));
            batch.add(row);
        }

        engine.ingestColumnar("sales", batch);

        QueryEngine qe = new QueryEngine(engine, 256);

        // Query 1: COUNT(*)
        QueryRequest countReq = new QueryRequest("sales", List.of("COUNT(*)"), Collections.emptyList(), Collections.emptyList(), 10);
        QueryResult countRes = qe.execute(countReq);
        assertEquals(1, countRes.getRows().size(), "Result row count mismatch");
        assertEquals(1000L, countRes.getRows().get(0).get(0), "COUNT(*) must be 1000");

        // Query 2: SUM(revenue)
        QueryRequest sumReq = new QueryRequest("sales", List.of("SUM(revenue)"), Collections.emptyList(), Collections.emptyList(), 10);
        QueryResult sumRes = qe.execute(sumReq);
        double expectedSum = 10.0 * (1000 * 1001 / 2.0); // 5005000.0
        assertEquals(expectedSum, (Double) sumRes.getRows().get(0).get(0), "SUM(revenue) mismatch");

        engine.close();
    }
}
