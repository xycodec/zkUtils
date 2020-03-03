package com.xycode.zkUtils.lock;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import com.xycode.zkUtils.zkClient.ZKCliGroup;
import com.xycode.zkUtils.zkClient.ZKConnectionPool;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: ZKDistributedLock
 * 基于Zookeeper的顺序分布式锁
 * @Author: xycode
 * @Date: 2019/10/30
 * Description:
 **/
public class ZKDistributedLock {
    private static final Logger logger= LoggerFactory.getLogger("myLogger");

    private String lockPath;//顺序锁的父路径
    private String curID;//当前ZKCli占有的顺序ID
    private String prevID;//前一个顺序ID,当前ZKCLi要等待它

    private ZKCli zkCli;//竞争分布式锁的zkCli

    public ZKDistributedLock(String lockPath, ZKCli zkCli){
        this.lockPath=lockPath;
        this.zkCli=zkCli;
    }

    /**
     * 尝试获得锁的函数,会创建一个临时顺序节点,
     * 然后判断这个临时节点是否在等待队列的开头,若在开头,说明获锁成功,返回true
     * @return
     */
    public boolean tryLock(){
        try {
            //创建临时顺序节点,理论上不会出现竞争
            String[] tmp=zkCli.createEphemeralSeq(lockPath+"/1","").split("/");
            curID=tmp[tmp.length-1];//获得当前创建的顺序节点ID
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        //从最小的ID开始选,也就是最先创建顺序节点成功的那个ZKCli
        List<String> ids = zkCli.getChildren(lockPath);
        String chosenId = Collections.min(ids);
//        System.out.println("-->" + ids.size());
        if(curID.equals(chosenId)) {//获得锁了
            logger.debug("Agent-{} acquire lock",curID);
            return true;
        }else{
            return false;
        }
    }

    /**
     * 等待锁的函数,当tryLock()没能获得锁时调用这个函数
     * 具体操作就是注册一个监听器,用于监听前一个顺序ID对应的路径,监听其删除事件,在这之前一直会阻塞等待
     */
    public void waitLock(){
        final CountDownLatch[] countDownLatch={new CountDownLatch(1)};
        //创建监听器对象
        ZKListener listener=new SimpleEventListener() {
            @Override
            public void NodeDeletedHandler(WatchedEvent event) {
//                System.out.println("--delete--");
                if(countDownLatch[0]!=null)
                    countDownLatch[0].countDown();
            }

        };
        //找到prevID
        List<String> ids=zkCli.getChildren(lockPath);
        Collections.sort(ids);
        for(int i=0;i<ids.size();++i){
            if(ids.get(i).equals(curID)){
                if(i==0) return;//前面的节点delete了,并且curID节点位于最前头,这时return尝试tryLock
                prevID=ids.get(i-1);//由于可能chainBroken Exception,prevID与curID可能未必连续
                break;
            }
        }
        //指定监听prevID对应的路径
        final String prevPath=lockPath + "/" + prevID;
        zkCli.registerListener(listener, prevPath);
        if (zkCli.exists(prevPath)) {//这里判断一下prevPath是否存在,因为tryLock到waitLock可能会有延迟以及chainBroken Exception与
            logger.debug("{} waiting for {}", curID,prevID);
            try {
                countDownLatch[0].await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{//prevPath不存在,可能是prevID对应的节点突然挂了,那样的话就去下面判断curID是否是队头节点
            zkCli.unregisterListener(prevPath);//清理无用的监听器
        }
        //判断curID是否是队头节点
        ids=zkCli.getChildren(lockPath);//先前的子节点在等待过程中可能已经发生了改变,所以这里要及时刷新,以获得最新数据
        Collections.sort(ids);
        if(!curID.equals(ids.get(0))) {
            //warn: 监听到前一个节点的删除事件,但此时curID对应的节点却不是第一个节点,
            //      前一个节点是中间节点但却异常退出了,等待链条断裂,这时就不能执行业务代码
            //fix: 下面再次waitLock()的目的就是再次去找等待队列中的前一个"prevID",把等待链条续上
            //例如: 1 -> 2 -> 3(3意外挂掉了) -> curID -> ...,即变成了1 -> 2 ->   -> curID -> ...
            //这时curID是会监听到删除事件的,但是前面实际上还有ZKCli在等待,
            //再次waitLock()就是想变成这样1 -> 2 -> curID -> ...
            logger.warn("[Fix chainBroken Exception]: try waitLock again!");
            waitLock();
        }else{
            logger.debug("Agent-{} acquire lock",curID);
        }
    }

    /**
     * 加锁操作
     */
    public void lock(){
        if(!tryLock()){//每个尝试lock的线程,都会先tryLock一下,通过创建的顺序节点来确定是否获得lock
            waitLock();//lock失败了就wait,等待前一个节点delete,并且注意ChainBroken的异常情况
        }
    }

    /**
     * 解锁操作,就是删除当前占用的顺序节点
     */
    public void unlock(boolean close){
        final String curPath=lockPath+"/"+curID;
        try {
            if(zkCli.exists(curPath))
                zkCli.delete(curPath);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        logger.debug("Agent-{} release lock",curID);
        if(close) zkCli.close();
    }

    //默认关闭连接
    public void unlock(){
        unlock(true);
    }

    //test, notice: 貌似这里使用测试框架会出错...
    public static void main(String[] args) {
        final String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        //notice: 这里使用自实现的ZKCliGroup来测试,
        //        只使用5个连接构成的连接池,提供给20个线程来使用,显然达到了连接复用的目的
        //        暂时没发现什么问题
        ZKCliGroup zkCliGroup=new ZKCliGroup(ZKC_ADDRESS,5);
        ZKCli zkCli=zkCliGroup.getZKConnection();
        String path="/seqLockPath";
        if(!zkCli.exists(path)) {
            try {
                //先创建一个永久节点
                zkCli.createPersistent(path, "");
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        zkCliGroup.releaseZKConnection(zkCli);

        CountDownLatch countDownLatch=new CountDownLatch(20);
        Thread[] t=new Thread[20];
        for(int i=0;i<t.length;++i){
            t[i]= new Thread(() -> {
                ZKCli zkCli1 =zkCliGroup.getZKConnection();
                //tip: 用法
                ZKDistributedLock lock=new ZKDistributedLock(path, zkCli1);
                try{
                    lock.lock();
                    //zkCli此时获得了锁,可以做一些独占的事情
                    logger.info("Agent-{} working...",lock.curID);
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }finally {
                    lock.unlock(false);
                    zkCliGroup.releaseZKConnection(zkCli1);
                    countDownLatch.countDown();
                }
            });
        }
        for (Thread thread : t) {
            thread.start();
        }

        //清理资源
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            zkCliGroup.shutdown();
        }
    }
}
