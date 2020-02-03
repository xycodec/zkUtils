package com.xycode.zkUtils.server;

import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZKServerQuorum implements Runnable{
	QuorumPeerMain main;
	QuorumPeerConfig config;
	String config_path;
	public static CountDownLatch latch;
	public static final Logger logger= LoggerFactory.getLogger("testLogger");
	public ZKServerQuorum(String config_path) {
		main=new QuorumPeerMain();
		config=new QuorumPeerConfig();
		this.config_path = config_path;
		try {
			this.config.parse(config_path);
		} catch (ConfigException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void run() {
		try {
			latch.countDown();
			this.main.runFromConfig(this.config);
		} catch (IOException | AdminServerException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void setupCluster(int server_num) {
		latch=new CountDownLatch(server_num);
		ExecutorService es=Executors.newFixedThreadPool(server_num);
		//System.out.println(System.getProperty("user.dir"));
		for(int i=0;i<server_num;++i) {
			String config_path="src/main/resources/z"+ (i + 1) +".cfg";
			logger.info("ZKServer-"+(i+1)+" started, configPath: "+config_path);
			es.execute(new ZKServerQuorum(config_path));
		}
		es.shutdown();
		
	}

	public static void formCluster(int server_num){
		//配置参数
		setupCluster(server_num);
		logger.info("waiting for forming cluster....");
		try {
			latch.await(30,TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("cluster has been formed.");
	}
	
	public static void main(String[] args) {
		//配置参数
		setupCluster(4);
		logger.info("waiting for forming cluster....");
		try {
			latch.await(30,TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("ZKServer cluster started!");
	}
}
