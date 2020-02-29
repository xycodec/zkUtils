package com.xycode.zkUtils.listener;

import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 这仅是一个测试类,不建议使用,若有需求直接继承扩展AbstractEventListener/SimpleEventListener即可
 */
public class MyEventListener extends AbstractEventListener {
    private static String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    private static ZKCli zkCli=new ZKCli(ZKC_ADDRESS);
    @Override
    public void NodeCreatedHandler(WatchedEvent event) {
        System.out.println("[created]: "+path);
    }

    @Override
    public void NodeChildrenChangedHandler(WatchedEvent event) {
        int cnt=ThreadLocalRandom.current().nextInt(1000);
        System.out.println("[childrenChanged start]: "+path+", "+cnt);
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
//            zkCli.triggerChildListener(path);
            zkCli.delete(path+"/test2");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("[childrenChanged done]: "+path+", "+cnt);
    }

    @Override
    public void NodeDataChangedHandler(WatchedEvent event) {
        System.out.println("[dataChanged]: "+path);
    }

    @Override
    public void NodeDeletedHandler(WatchedEvent event) {
        System.out.println("[deleted start]: "+path);
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("[deleted done]: "+path);
    }

    public MyEventListener(String path) {
            super(path);
        }

    @Test
    public void test() {
        String path="/listener";
        ZKCli zkClient=new ZKCli(ZKC_ADDRESS,new MyEventListener(path));
        try {
            zkClient.waitConnected(5000);
            if(!zkClient.exists(path)) zkClient.createPersistent(path,"");
//                zkClient.writeStringData(path,"changed");
//                System.out.println(zkClient.readStringData(path));
//
//                zkClient.writeDoubleData(path,Double.MAX_VALUE);
//                System.out.println(zkClient.readDoubleData(path));
//
//                zkClient.writeIntData(path,Integer.MIN_VALUE);
//                System.out.println(zkClient.readIntData(path));
//
//                zkClient.writeLongData(path,Long.MAX_VALUE);
//                System.out.println(zkClient.readLongData(path));
            zkClient.createEphemeral(path+"/test1","");
            zkClient.createEphemeral(path+"/test2","");

            zkClient.triggerChildListener(path);
            zkClient.delete(path+"/test1");

            TimeUnit.SECONDS.sleep(10);
            zkCli.delete(path);
            zkClient.close();
        } catch (KeeperException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }

    }
}
