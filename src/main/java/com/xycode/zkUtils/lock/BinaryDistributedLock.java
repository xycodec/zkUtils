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
 * ClassName: BinaryLock
 *
 * @Author: xycode
 * @Date: 2020/4/21
 * @Description: this is description of the BinaryLock class
 **/
public class BinaryDistributedLock {
    private enum State{
        master,backup
    }
    private static final Logger logger= LoggerFactory.getLogger("myLogger");
    private ZKCli zkCli;
    private State state;

    public BinaryDistributedLock(ZKCli zkCli) {
        this.zkCli = zkCli;
        this.state=State.master;
    }

    public boolean tryAcquire(String lockPath) {
        while(true){
            try {
                zkCli.createEphemeral(lockPath,"");
            } catch (KeeperException | InterruptedException e) {
                this.state=State.backup;
            }
            if(this.state.equals(State.backup)){
                CountDownLatch countDownLatch=new CountDownLatch(1);
                ZKListener zkListener=new SimpleEventListener(lockPath){
                    @Override
                    public void NodeDeletedHandler(WatchedEvent event) {
                        countDownLatch.countDown();
                    }
                };
                this.zkCli.registerDefaultListener(zkListener);
                this.zkCli.triggerListener(lockPath);
                try {
                    logger.debug("Failed to acquire BinaryDistributedLock,state={},so that is waiting..",this.state);
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.state=State.master;
            }else{
                return true;
            }
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKCli zkCli=new ZKCli("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        BinaryDistributedLock binaryDistributedLock =new BinaryDistributedLock(zkCli);
        if(binaryDistributedLock.tryAcquire("/binaryLock")){//会阻塞,直到成功获得锁
            //可以做一些独占的事情...

        }
    }

}
