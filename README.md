### zkUtils:
#### 提供一个Zookeeper的简易封装库,主要的功能有:
* Listener: 针对Zookeeper原生的Watcher的封装,这里使用了模板设计方法,提供两个公共的监听器接口,ZKListener与multipleListener,分别对应单节点的监听器与多节点的监听器.
* zkClient.ZKCli: 针对Zookeeper原生客户端的封装,并且基于已实现的ZKListener与multipleListener,提供了一套监听器操作接口.
* zkClient.ZKGroup: 基于ZKCli实现的连接池,考虑到扩展性,基于cglib动态代理实现了AOP机制,以达到访问控制.
* zkClient.ZKConnectionPool: 基于ZKCli实现的连接池,存在安全性与扩展性问题,已废弃
* lock.ZKDistributedLock: 基于已实现的zkCli的顺序分布式锁.
* server: 提供两个创建Zookeeper Server的便捷API,ZKServerAlone为单Server模式,ZKServerQuorum为server集群模式(默认1 Leader,2 Follower,1 Observer;如需改动,请修改resources中的配置文件即可).

#### <u>实现该工具库的初衷是源于一个工程项目...</u>
    