### zkUtils:
#### 提供一个Zookeeper的简易封装库,主要的功能有:
* Listener: 针对Zookeeper原生的Watcher的封装,这里使用了模板设计方法,
提供两个公共的监听器接口,ZKListener与multipleListener,分别对应单节点的监听器与多节点的监听器.
* ZKCli: 针对Zookeeper原生客户端的封装,并且基于已实现的ZKListener与multipleListener,提供了一套监听器操作接口.
* Lock: 基于已实现的zkCli的顺序分布式锁.

#### <u>实现该工具库的初衷是源于一个工程项目...</u>
    