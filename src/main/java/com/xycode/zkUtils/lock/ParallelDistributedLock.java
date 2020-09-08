package com.xycode.zkUtils.lock;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * 并行的分布式锁
 *
 * @Author: xycode
 * @Date: 2020/4/21
 **/
public class ParallelDistributedLock {
    private enum State {
        master, backup
    }

    private static final Logger logger = LoggerFactory.getLogger(ParallelDistributedLock.class);
    private ZKCli zkCli;
    private State state;

    public ParallelDistributedLock(ZKCli zkCli) {
        this.zkCli = zkCli;
        this.state = State.master;
    }

    public boolean tryAcquire(String lockPath, String owner) {
        zkCli.unregisterDefaultListener();
        while (true) {
            try {
                zkCli.createEphemeral(lockPath, owner);
            } catch (Exception e) {
                this.state = State.backup;
                logger.info("{} fail to acquire BinaryDistributedLock, state: [{}->{}]", owner, State.master, State.backup);
            }
            if (this.state.equals(State.backup)) {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ZKListener zkListener = new SimpleEventListener(lockPath) {
                    @Override
                    public void NodeDeletedHandler(WatchedEvent event) {
                        logger.info("{} detect BinaryDistributedLock is idle", owner);
                        countDownLatch.countDown();
                    }
                };
                this.zkCli.registerDefaultListener(zkListener);
                this.zkCli.triggerListener(lockPath);
                try {
                    logger.info("{} is waiting for BinaryDistributedLock, state: [{}]", owner, State.backup);
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.state = State.master;
            } else {
                logger.info("{} acquire RegisterLock", owner);
                return true;
            }
            zkCli.unregisterDefaultListener();
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKCli zkCli = new ZKCli("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        ParallelDistributedLock parallelDistributedLock = new ParallelDistributedLock(zkCli);
        if (parallelDistributedLock.tryAcquire("/binaryLock", "")) {//会阻塞,直到成功获得锁
            //可以做一些独占的事情...

        }
    }

}
