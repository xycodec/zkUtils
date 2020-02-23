package com.xycode.zkUtils.listener.multipleListener;

import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * ClassName: SimpleEventListener
 *
 * @Author: xycode
 * @Date: 2019/10/30
 * @Description: 默认的都是空的方法,可以根据需求重写,而不必重写所有方法
 **/
public class MultipleEventListener extends AbstractMultipleEventListener {
    public MultipleEventListener(List<String> path) {
        super(path);
    }

    public MultipleEventListener() {
        super();
    }

    @Override
    public void NodeDeletedHandler(WatchedEvent event) {
    }

    @Override
    public void NodeDataChangedHandler(WatchedEvent event) {
    }

    @Override
    public void NodeCreatedHandler(WatchedEvent event) {
    }

    @Override
    public void NodeChildrenChangedHandler(WatchedEvent event) {
    }

    @Test
    public void test() {
        try {
            MultipleEventListener listener=new MultipleEventListener(Arrays.asList("/a","/b","/c","/d")) {
                @Override
                public void NodeChildrenChangedHandler(WatchedEvent event) {
                    System.out.println(event.getPath()+"--children change--");
                }

                @Override
                public void NodeCreatedHandler(WatchedEvent event) {
                    System.out.println(event.getPath()+"--create--");
                }

                @Override
                public void NodeDeletedHandler(WatchedEvent event) {
                    System.out.println(event.getPath()+"--delete--");
                }

                @Override
                public void NodeDataChangedHandler(WatchedEvent event) {
                    System.out.println(event.getPath()+"--change--");
                }
            };

            //监听NodeChildrenChanged事件的方式,getChildren(path,true)
            String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
            ZKCli zkCli=new ZKCli(ZKC_ADDRESS);
            zkCli.registerDefaultListener(listener);
            zkCli.createEphemeral("/a","",true);
            zkCli.createEphemeral("/b","",true);
            zkCli.createEphemeral("/c","",true);
            System.out.println();
            zkCli.writeStringData("/a","changed",true);
            zkCli.writeStringData("/b","changed",true);
            zkCli.writeStringData("/c","changed",true);
            System.out.println();
            zkCli.delete("/a",true);
            zkCli.delete("/b",true);
            zkCli.delete("/c",true);

            System.out.println("-------------------");

            zkCli.createPersistent("/d","", ZooDefs.Ids.OPEN_ACL_UNSAFE,false);

            System.out.println("create /d/1");
            zkCli.triggerChildListener("/d");
            zkCli.createPersistent("/d/1","",ZooDefs.Ids.OPEN_ACL_UNSAFE,false);

//            System.out.println("create /d/1/2");
//            zkCli.addChildListener("/d");
//            zkCli.createPersistent("/d/1/2","",ZooDefs.Ids.OPEN_ACL_UNSAFE,false);//tip: 孙节点感知不到

            System.out.println("write /d/1");
            zkCli.triggerChildListener("/d");
            zkCli.writeStringData("/d/1","changed");//tip:子节点数据改变,父节点注册的NodeChildrenChangedHandler()感知不到,说明NodeChildrenChangedHandler()只针对子节点的创建,删除

//            System.out.println("delete /d/1/2");
//            zkCli.addChildListener("/d");
//            zkCli.delete("/d/1/2");//tip: 孙节点感知不到

            System.out.println("delete /d/1");
            zkCli.triggerChildListener("/d");
            zkCli.delete("/d/1");

            zkCli.delete("/d");

            zkCli.close();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
