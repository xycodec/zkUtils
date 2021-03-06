package com.xycode.zkUtils.listener;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

public interface ZKListener extends Watcher {

    void NodeDeletedHandler(WatchedEvent event);

    void NodeDataChangedHandler(WatchedEvent event);

    void NodeCreatedHandler(WatchedEvent event);

    void NodeChildrenChangedHandler(WatchedEvent event);

    /**
     * 监听指定path,依此回调process()
     * 该方法可以多次调用覆盖path
     * @param path
     */
    void listen(String path);
}
