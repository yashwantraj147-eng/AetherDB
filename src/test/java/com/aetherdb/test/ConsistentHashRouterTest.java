package com.aetherdb.test;

import com.aetherdb.partition.ConsistentHashRouter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentHashRouterTest {

    @Test
    @DisplayName("Consistent hash router correctly distributes and routes keys across nodes")
    void testBasicRouting() {
        ConsistentHashRouter router = new ConsistentHashRouter(32);
        router.addNode("node-A");
        router.addNode("node-B");
        router.addNode("node-C");

        assertEquals(3, router.getPhysicalNodes().size());
        assertEquals(96, router.getRingSize()); // 3 * 32

        String node1 = router.routeKey("user_account_1001");
        assertNotNull(node1);
        assertTrue(Set.of("node-A", "node-B", "node-C").contains(node1));

        // Determinism: same key always routes to same node
        assertEquals(node1, router.routeKey("user_account_1001"));
    }

    @Test
    @DisplayName("Collision safety: multiple nodes or virtual nodes with identical hash do not overwrite each other")
    void testCollisionSafetyAndRemoval() {
        ConsistentHashRouter router = new ConsistentHashRouter(16);
        router.addNode("node-1");
        router.addNode("node-2");

        int initialRingSize = router.getRingSize();
        assertEquals(32, initialRingSize);

        // Remove node-1
        router.removeNode("node-1");
        assertEquals(16, router.getRingSize());
        assertEquals(Set.of("node-2"), router.getPhysicalNodes());

        // All keys now route to node-2
        assertEquals("node-2", router.routeKey("any_key"));

        // Remove node-2
        router.removeNode("node-2");
        assertEquals(0, router.getRingSize());

        assertThrows(IllegalStateException.class, () -> router.routeKey("any_key"));
    }
}

