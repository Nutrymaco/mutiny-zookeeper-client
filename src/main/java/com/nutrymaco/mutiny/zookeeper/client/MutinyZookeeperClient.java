package com.nutrymaco.mutiny.zookeeper.client;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MutinyZookeeperClient {

    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final CuratorFramework curatorFramework;

    public MutinyZookeeperClient(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }

    public Uni<String> getValue(String path) {
        UnicastProcessor<String> processor = UnicastProcessor.create();
        executorService.submit(() -> {
            try {
                String value = new String(curatorFramework.getData().forPath(path));
                processor.onNext(value);
                processor.onComplete();
            } catch (Exception e) {
                processor.onError(e);
            }
        });
        return processor.toUni();
    }

    public Uni<Void> setValue(String path, String value) {
        // string not void because queue not accepting null as value
        UnicastProcessor<String> processor = UnicastProcessor.create();
        executorService.submit(() -> {
            try {
                curatorFramework.create().orSetData().creatingParentsIfNeeded().forPath(path, value.getBytes());
                processor.onNext("");
                processor.onComplete();
            } catch (Exception e) {
                processor.onError(e);
            }
        });
        return processor.toUni().replaceWithVoid();
    }

    public Uni<Boolean> isExist(String path) {
        UnicastProcessor<Boolean> processor = UnicastProcessor.create();
        executorService.submit(() -> {
            try {
                var isExist = curatorFramework.checkExists().forPath(path) != null;
                processor.onNext(isExist);
                processor.onComplete();
            } catch (Exception e) {
                processor.onError(e);
            }
        });
        return processor.toUni();
    }

    public Uni<Void> delete(String path) {
        return isExist(path)
                .flatMap(isExist -> {
                    if (isExist) {
                        UnicastProcessor<String> processor = UnicastProcessor.create();
                        executorService.submit(() -> {
                            try {
                                curatorFramework.delete().forPath(path);
                                processor.onNext("");
                            } catch (Exception e) {
                                processor.onError(e);
                            }
                        });
                        return processor.toUni().replaceWithVoid();
                    } else {
                        return Uni.createFrom().voidItem();
                    }
                });
    }

}
