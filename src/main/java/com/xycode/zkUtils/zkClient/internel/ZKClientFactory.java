package com.xycode.zkUtils.zkClient.internel;

import com.xycode.zkUtils.listener.ZKListener;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZKClientFactory {
    private static final Logger logger= LoggerFactory.getLogger("myLogger");
    /**
     * @param zkAddr
     * @return Zookeeper Client, with a default watcher
     */
    public static ZooKeeper createDefaultZKClient(String zkAddr) throws TimeoutException {
        CountDownLatch countDownLatch=new CountDownLatch(1);
        ZooKeeper zk=null;
        try {
            //实际上是异步创建
            zk= new ZooKeeper(zkAddr, 10000,new Watcher() {
                public void process(WatchedEvent event) {
                    Event.KeeperState eventState=event.getState();
                    Event.EventType eventType=event.getType();
                    if(eventState.equals(Event.KeeperState.SyncConnected)&&eventType.equals(Event.EventType.None)){
                        logger.debug(Thread.currentThread().getName()+": connection established");
                        countDownLatch.countDown();//成功建立连接
                    }else if(eventState.equals(Event.KeeperState.Disconnected)) {
                        logger.debug(Thread.currentThread().getName()+": disconnected");
                    }else{
                        if(event.getPath()!=null)
                            logger.debug(Thread.currentThread().getName()+": (eventType: "+eventType+") at \""+event.getPath()+"\"");
                        else
                            logger.debug(Thread.currentThread().getName()+": (eventType: "+eventType+")");
                    }
                }
            });

//            //这个等待是必要的，不然在多线程场景下会报错KeeperErrorCode = ConnectionLoss
//            //不过这种等待方式不太优雅,考虑使用CountDownLatch
//            while(zk.getState()!= ZooKeeper.States.CONNECTED) {
//                Thread.sleep(20);
//                //System.out.println(zk.getState());
//            }

            //CountDownLatch方式等待
            countDownLatch.await(15, TimeUnit.SECONDS);
            if(!zk.getState().equals(ZooKeeper.States.CONNECTED)){
                throw new TimeoutException("create zookeeper client timeout");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk;
    }

    /**
     *
     * @param zkAddr
     * @return Zookeeper Client,with a empty watcher
     */
    public static ZooKeeper createZKClient(String zkAddr) {
        ZooKeeper zk=null;
        CountDownLatch countDownLatch=new CountDownLatch(1);
        try {
            zk= new ZooKeeper(zkAddr, 10000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    Event.KeeperState eventState=event.getState();
                    Event.EventType eventType=event.getType();
                    if(eventState.equals(Event.KeeperState.SyncConnected)&&eventType.equals(Event.EventType.None)) {
                        logger.debug("[WatchedEvent-" + Thread.currentThread().getId() + "]: connection established");
                        countDownLatch.countDown();//成功建立连接
                    }
                }
            });

//            while(zk.getState()!= ZooKeeper.States.CONNECTED) {//这个等待是必要的，不然在多线程场景下会报错KeeperErrorCode = ConnectionLoss
//                Thread.sleep(20);
//                //System.out.println(zk.getState());
//            }

            countDownLatch.await(15, TimeUnit.SECONDS);
            if(!zk.getState().equals(ZooKeeper.States.CONNECTED)){
                throw new TimeoutException("create zookeeper client timeout");
            }
        } catch (IOException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }
        return zk;
    }

    /**
     *
     * @param zkAddr
     * @param listener your own define listener
     * @return Zookeeper Client
     */
    public static ZooKeeper createZKClient(String zkAddr, ZKListener listener) throws TimeoutException {
        ZooKeeper zk=null;
        try {
            zk= new ZooKeeper(zkAddr, 10000,listener);
            //这个等待是必要的，不然在多线程场景下会报错KeeperErrorCode = ConnectionLoss
            int count=0;
            while(zk.getState()!= ZooKeeper.States.CONNECTED) {
                if(count>=70) throw new TimeoutException("create zookeeper client timeout");
                Thread.sleep(20);
                //System.out.println(zk.getState());
                ++count;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk;
    }
}
