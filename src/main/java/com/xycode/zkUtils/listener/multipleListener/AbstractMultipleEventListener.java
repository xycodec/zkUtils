package com.xycode.zkUtils.listener.multipleListener;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;

/**
 * ClassName: AbstractMultipleEventListener
 *
 * @Author: xycode
 * @Date: 2019/11/11
 * @Description: this is description of the AbstractMultipleEventListener class
 **/
public abstract class AbstractMultipleEventListener implements ZKMultipleListener{
    private List<String> paths=null;

    public AbstractMultipleEventListener(List<String> paths) {
        this.paths = paths;
    }

    public AbstractMultipleEventListener() {
    }

    @Override
    public void process(WatchedEvent event) {
        if(paths==null) return;
//        System.out.println(event.getType()+", "+event.getPath());
        for(String path:paths){
            if(path.equals(event.getPath())){
                if(event.getType().equals(Watcher.Event.EventType.NodeDeleted)){
                    NodeDeletedHandler(event);
                }else if(event.getType().equals(Watcher.Event.EventType.NodeDataChanged)){
                    NodeDataChangedHandler(event);
                }else if(event.getType().equals(Watcher.Event.EventType.NodeCreated)) {
                    NodeCreatedHandler(event);
                }else if(event.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)){
                    NodeChildrenChangedHandler(event);
                }
            }
        }

    }

    @Override
    public void listen(List<String> paths) {
        this.paths=paths;
    }

}
