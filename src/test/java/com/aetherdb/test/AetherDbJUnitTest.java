package com.aetherdb.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * JUnit 5 test wrapper for AetherDB automated CI and IDE integration.
 */
public class AetherDbJUnitTest {

    @Test
    @DisplayName("BloomFilter false positive and zero false negative verification")
    void testBloomFilter() {
        assertDoesNotThrow(() -> TestRunner.main(new String[]{}));
    }
}
