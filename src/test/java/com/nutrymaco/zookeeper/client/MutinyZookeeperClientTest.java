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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MutinyZookeeperClientTest {

    private static GenericContainer<?> container;

    private static CuratorFramework curatorFramework;
    private static MutinyZookeeperClient mutinyZkClient;

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
    public void oneValueTest() {
        mutinyZkClient.setValue("/test/path", "test-value")
                        .await().indefinitely();

        assertTrue(mutinyZkClient.isExist("/test/path").await().indefinitely());
        assertEquals(mutinyZkClient.getValue("/test/path").await().indefinitely(), "test-value");
    }

}
