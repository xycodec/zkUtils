package com.xycode.zkUtils.listener;

import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.testng.annotations.Test;

/**
 * ClassName: SimpleEventListener
 *
 * @Author: xycode
 * @Date: 2019/10/30
 * @Description: 默认的都是空的方法,可以根据需求重写,而不必重写所有方法
 **/
public class SimpleEventListener extends AbstractEventListener {
    public SimpleEventListener(String path) {
        super(path);
    }

    public SimpleEventListener() {
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
        String path="/seqLockPath";
        try {
            ZKListener listener=new SimpleEventListener(path) {
                @Override
                public void NodeChildrenChangedHandler(WatchedEvent event) {
                    System.out.println("--children change--");
                }

            };

            //监听NodeChildrenChanged事件的方式,getChildren(path,true)
            String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
            ZKCli zkCli=new ZKCli(ZKC_ADDRESS,listener);
            zkCli.triggerChildListener(path);
            zkCli.createEphemeral(path+"/1","",false);
            zkCli.triggerChildListener(path);
            zkCli.delete(path+"/1",false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
