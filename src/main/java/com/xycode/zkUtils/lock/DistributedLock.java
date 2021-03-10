package com.xycode.zkUtils.lock;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 乱序争抢的分布式锁
 *
 * @Author: xycode
 * @Date: 2020/4/21
 **/
public class DistributedLock {
    private enum State {
        master, backup
    }

    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);
    private State state;

    private final ZKCli zkCli;
    private final String lockPath;

    public DistributedLock(ZKCli zkCli, String lockPath) {
        this.zkCli = zkCli;
        this.lockPath = lockPath;
        this.state = State.master;
    }

    public boolean tryAcquire() {
        zkCli.unregisterDefaultListener();
        while (true) {
            try {
                zkCli.createEphemeral(lockPath, Thread.currentThread().getName());
            } catch (Exception e) {
                this.state = State.backup;
                logger.trace("{} fail to acquire DistributedLock, state: [{}->{}]", Thread.currentThread().getName(), State.master, State.backup);
            }
            if (this.state.equals(State.backup)) {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ZKListener zkListener = new SimpleEventListener(lockPath) {
                    @Override
                    public void NodeDeletedHandler(WatchedEvent event) {
                        logger.trace("{} detect DistributedLock is idle", Thread.currentThread().getName());
                        countDownLatch.countDown();
                    }
                };
                this.zkCli.registerListener(zkListener, lockPath);
                this.zkCli.triggerListener(lockPath);
                try {
                    logger.trace("{} is waiting for DistributedLock, state: [{}]", Thread.currentThread().getName(), State.backup);
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.state = State.master;
            } else {
                return true;
            }
            zkCli.unregisterListener(lockPath);
        }
    }

    public void lock() {
        if (tryAcquire()) {
            logger.debug("{} acquire distributedLock: {}", Thread.currentThread().getName(), lockPath);
        }
    }

    public boolean locked() {
        return zkCli.exists(lockPath);
    }

    public void unlock(String owner) {
        try {
            zkCli.delete(lockPath);
            logger.debug("{} released distributedLock: {}", owner, lockPath);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 20; ++i) {
            es.submit(() -> {
                ZKCli zkCli = new ZKCli("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
                DistributedLock distributedLock = new DistributedLock(zkCli, "/distributedLock");
                distributedLock.lock();
                try {
                    //可以做一些独占的事情...
                    TimeUnit.SECONDS.sleep(3);
                    logger.info("{} release distributedLock", Thread.currentThread().getName());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    distributedLock.unlock(Thread.currentThread().getName());
                }

            });
        }

    }

}
