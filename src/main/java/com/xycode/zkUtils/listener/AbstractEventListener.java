package com.xycode.zkUtils.listener;

import org.apache.zookeeper.WatchedEvent;

public abstract class AbstractEventListener implements ZKListener {
    protected volatile String path;

    public AbstractEventListener(String path) {
        this.path = path;
    }

    public AbstractEventListener(){
    }

    @Override
    public void listen(String path){
        this.path=path;
    }

    @Override
    public void process(WatchedEvent event) {
        if(path==null) return;
//        System.out.println(event.getType()+", "+event.getPath());
        if(path.equals(event.getPath())){//event.getPath()一开始可能是null,防止空指针异常,所以这样比较
            if(event.getType().equals(Event.EventType.NodeDeleted)){
                NodeDeletedHandler(event);
            }else if(event.getType().equals(Event.EventType.NodeDataChanged)){
                NodeDataChangedHandler(event);
            }else if(event.getType().equals(Event.EventType.NodeCreated)) {
                NodeCreatedHandler(event);
            }else if(event.getType().equals(Event.EventType.NodeChildrenChanged)){
                NodeChildrenChangedHandler(event);
            }
            //NodeChildrenChanged触发条件比较奇特
            //经试验,使用getChildren(xxx,true)才能添加Children的监听器,使用时要尤其注意.
            // 并且只能监听到子节点的create,dataChange监听不到,并且孙节点的事件也是监听不到的
        }

    }

    public static void main(String[] args) {

    }
}
