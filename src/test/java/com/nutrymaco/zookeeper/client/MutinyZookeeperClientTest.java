package com.nutrymaco.zookeeper.client;

import com.nutrymaco.mutiny.zookeeper.client.MutinyZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryForever;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Random;

import static org.junit.Assert.*;


public class MutinyZookeeperClientTest {

    private static GenericContainer<?> container;

    private static CuratorFramework curatorFramework;
    private static MutinyZookeeperClient mutinyZkClient;

    private final Random random = new Random();
    @BeforeClass
    public static void startZookeeper() throws InterruptedException {
        container = new GenericContainer<>("zookeeper");
        container.addExposedPorts(2181);
        container.start();
        int port = container.getMappedPort(2181);
        curatorFramework = CuratorFrameworkFactory.newClient("localhost:" + port, new RetryForever(100));
        curatorFramework.start();
        curatorFramework.blockUntilConnected();
        mutinyZkClient = new MutinyZookeeperClient(curatorFramework);
    }

    @AfterClass
    public static void stopZookeeper() {
        if (container != null) {
            container.stop();
        }
    }

    @Test
    public void testOneValue() {
        String path = "/test/path/" + random.nextInt();
        mutinyZkClient.setValue(path, "test-value")
                        .await().indefinitely();

        assertTrue(mutinyZkClient.isExist(path).await().indefinitely());
        assertEquals(mutinyZkClient.getValue(path).await().indefinitely(), "test-value");
    }

    @Test
    public void testSetCheckDelete() {
        String path = "/test/path/" + random.nextInt();
        mutinyZkClient.setValue(path, "test-value")
                .await().indefinitely();
        assertTrue(mutinyZkClient.isExist(path).await().indefinitely());

        mutinyZkClient.delete(path).await().indefinitely();
        assertFalse(mutinyZkClient.isExist(path).await().indefinitely());
    }

}
