package com.xycode.zkUtils.lock;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.Test;

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
    private String lockPath;//顺序锁的父路径
    private String curID;//当前ZKCli占有顺序ID
    private String prevID;//前一个顺序ID,当前ZKCLi要等待它

    private ZKCli zkCli;

    public ZKDistributedLock(String lockPath, ZKCli zkCli){
        this.lockPath=lockPath;
        this.zkCli=zkCli;
    }

    public boolean tryLock(){
        try {
            //创建顺序节点,理论上不会出现竞争
            String[] tmp=zkCli.createEphemeralSeq(lockPath+"/1","", ZooDefs.Ids.OPEN_ACL_UNSAFE).split("/");
            curID=tmp[tmp.length-1];//获得顺序ID
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<String> ids = zkCli.getChildren(lockPath);
        String chosenId = Collections.min(ids);
        System.out.println("-->" + ids.size());
        if(curID.equals(chosenId)) {
            System.out.println("[first]Agent-" + curID + " acquire lock");
            return true;
        }else{
            return false;
        }
    }

    public void waitLock(){
        final CountDownLatch[] countDownLatch=new CountDownLatch[1];
        ZKListener listener=new SimpleEventListener() {
            @Override
            public void NodeDeletedHandler(WatchedEvent event) {
//                System.out.println("--delete--");
                if(countDownLatch[0]!=null)
                    countDownLatch[0].countDown();
            }

        };
        List<String> ids=zkCli.getChildren(lockPath);
        Collections.sort(ids);
        for(int i=0;i<ids.size();++i){
            if(ids.get(i).equals(curID)){
                if(i==0) return;//前面的节点出现异常挂了,或者已经主动delete了,这是位于curID的ZKCli就获得了锁
                prevID=ids.get(i-1);//由于chainBroken Exception,prevID可能会被多次赋值
                break;
            }
        }
        countDownLatch[0] = new CountDownLatch(1);
        zkCli.registerListener(listener, lockPath + "/" + prevID);
        if (zkCli.exists(lockPath + "/" + prevID)) {
            System.out.println(curID + " waiting for " + prevID);
            try {
                countDownLatch[0].await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else{
            zkCli.unregisterListener(lockPath+"/"+prevID);//监听器被触发后,实际上就没了,所以这里可能会抛异常(除非锁没被触发)
        }
        ids=zkCli.getChildren(lockPath);//先前的子节点在等待过程中可能已经发生了改变,所以这里要及时刷新,以获得最新数据
        Collections.sort(ids);
        if(!curID.equals(ids.get(0))) {//说明等待链条有中间节点异常退出了,等待链条断裂,这时就不能执行业务代码
            waitLock();
            System.err.println("[Fix chainBroken Exception]: try waitLock again!");
        }else{
            System.out.println("Agent-" + curID + " acquire lock");
        }
    }
    public void lock(){
        if(!tryLock()){//每个尝试lock的线程,都会先tryLock一下,通过创建的顺序节点来确定是否获得lock
            waitLock();//lock失败了就wait,等待前一个节点delete,并且注意ChainBroken的异常情况
        }
    }

    public void unlock(){
        try {
            if(zkCli.exists(lockPath+"/"+curID))
                zkCli.delete(lockPath+"/"+curID);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Agent-" + curID + " release lock");
        zkCli.close();
    }

    @Test
    public void test() {
        String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        ZKCli zkCli=new ZKCli(ZKC_ADDRESS);
        String path="/seqLockPath";
        if(!zkCli.exists(path)) {
            try {
                //先创建一个永久节点
                zkCli.createPersistent(path, "", ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
        zkCli.close();
        Thread[] t=new Thread[10];
        for(int i=0;i<t.length;++i){
            t[i]= new Thread(() -> {
                ZKCli zkCli1 =new ZKCli(ZKC_ADDRESS);
                zkCli1.unregisterDefaultListener();
                ZKDistributedLock lock=new ZKDistributedLock(path, zkCli1);
                //tip: 用法
                try{
                    lock.lock();
                    //zkCli此时获得了锁,可以做一些独占的事情
                    System.out.println("Agent-" + lock.curID + " working...");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }finally {
                    lock.unlock();
                }
            });
        }
        for (Thread thread : t) {
            thread.start();
        }
    }
}
