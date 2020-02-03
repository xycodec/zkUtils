package com.xycode.zkUtils.server;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

public class ZKServerAlone {
	int numConnections = 5000;//可接受的连接数量
	int tickTime = 2000;//心跳时间间隔
	File snapDir= new File(System.getProperty("java.io.tmpdir"), "zookeeper/snap").getAbsoluteFile();
	File logDir=new File(System.getProperty("java.io.tmpdir"), "zookeeper/log").getAbsoluteFile();
	NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();
	private static final Logger logger= LoggerFactory.getLogger("testLogger");
	public ZKServerAlone() {
		super();
	}

	public ZKServerAlone(File snapDir, File logDir, NIOServerCnxnFactory standaloneServerFactory) {
		super();
		this.snapDir = snapDir;
		this.logDir = logDir;
		this.standaloneServerFactory = standaloneServerFactory;
	}

	public void setupZKServer(InetSocketAddress addr)  {
		String dataDirectory = System.getProperty("java.io.tmpdir");
		logger.info("snapDir : "+snapDir);
		logger.info("snapDir : "+logDir);
		try {
			standaloneServerFactory = new NIOServerCnxnFactory();
			standaloneServerFactory.configure(addr, numConnections);
			standaloneServerFactory.startup(new ZooKeeperServer(snapDir, logDir, tickTime)); // start the server.
			logger.info("start the ZKServerAlone!");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
	
	public void shutdown() {
		standaloneServerFactory.shutdown();//stop the server.
		logger.info("stop the ZKServerAlone!");
	}
	
	public static void test() {
//		System.out.println(System.getProperty("java.io.tmpdir"));
		ZKServerAlone zka=new ZKServerAlone();
		zka.setupZKServer(new InetSocketAddress("127.0.0.1", 2184));
		Thread t=new Thread() {
			public void run() {
		        ZooKeeper zk;
				try {
					zk = new ZooKeeper("127.0.0.1:2100", 10000,new Watcher() {
					    public void process(WatchedEvent event) {
					        System.out.println("[WatchedEvent-"+Thread.currentThread().getId()+"]: "+event.getState() + "["+event.getType()+"],at "+event.getPath());
					    }
					});
					//因为创建Zookeeper client是异步的，所以在这里最好等待一下，否则可能会出错
					while(zk.getState()!=States.CONNECTED) {
						Thread.sleep(20);
					}
					
					//因为PERSISTENT类型的节点才能创建子节点，所以这里指定为PERSISTENT类型
			        zk.create("/com/xycode", "com/xycode".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			        
			        // 创建一个子目录节点
			        zk.exists("/com/xycode/test_1",true);
			        zk.create("/com/xycode/test_1", "test_1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			        System.out.println(new String(zk.getData("/com/xycode/test_1",true,null)));

			        zk.delete("/com/xycode/test_1", -1);
			        zk.exists("/com/xycode",true);
			        zk.delete("/com/xycode", -1);
			        
			        zk.close();
				} catch (IOException | InterruptedException | KeeperException e) {
					e.printStackTrace();
				}
			}
		};
		t.start();
	}
		 
	public static void main(String[] args) throws Exception {
//		ZKServerAlone zka=new ZKServerAlone();
//		zka.setupZKServer(new InetSocketAddress("127.0.0.1", 2184));
		test();
	}

}
