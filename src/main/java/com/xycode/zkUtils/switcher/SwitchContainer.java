package com.xycode.zkUtils.switcher;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/*
 *
 * @author: xycode
 * @date: 2020/9/12
 */
public class SwitchContainer {
    private static final Logger logger = LoggerFactory.getLogger("myLogger");

    private ZKCli coreCli;

    private String nameSpace;

    private Map<String, Object> container = new ConcurrentHashMap<>();

    public SwitchContainer(String zkAddress, String nameSpace) throws KeeperException, InterruptedException {
        if (nameSpace == null || nameSpace.trim().length() == 0) {
            throw new IllegalArgumentException("nameSpace can't be empty");
        }
        this.nameSpace = nameSpace;
        this.coreCli = new ZKCli(zkAddress);
        if (!this.coreCli.exists("/" + nameSpace)) {
            this.coreCli.createPersistent("/" + nameSpace);
        }
    }

    public <T extends Serializable> void registerSwitcher(String name, T value) throws KeeperException, InterruptedException, IOException {
        this.container.put(name, value);
        final String listenerPath = "/" + nameSpace + "/" + name;
        this.coreCli.createEphemeral(listenerPath);
        this.coreCli.writeObjectData(listenerPath, value);
        this.coreCli.registerListener(new SimpleEventListener() {
            @Override
            public void NodeDataChangedHandler(WatchedEvent event) {
                try {
                    logger.info("{}={} -> {}={}", name, value, name, coreCli.readObjectData(listenerPath));
                } catch (ClassNotFoundException | IOException e) {
                    e.printStackTrace();
                } finally {
                    coreCli.triggerListener(listenerPath);
                }
            }

            @Override
            public void NodeDeletedHandler(WatchedEvent event) {
                logger.info("{}={} is destroy", name, value);
            }

        }, listenerPath);

        this.coreCli.triggerListener(listenerPath);
    }

    public void registerStringSwitcher(String name, String value) throws KeeperException, InterruptedException {
        this.container.put(name, value);
        final String listenerPath = "/" + nameSpace + "/" + name;
        this.coreCli.createEphemeral(listenerPath, value);
        this.coreCli.registerListener(new SimpleEventListener() {
            String oldValue=value;
            @Override
            public void NodeDataChangedHandler(WatchedEvent event) {
                try {
                    logger.info("{}={} -> {}={}", name, oldValue, name, (oldValue=coreCli.readStringData(listenerPath)));
                } finally {
                    coreCli.registerListener(this, listenerPath);
                }
            }

            @Override
            public void NodeDeletedHandler(WatchedEvent event) {
                logger.info("{}={} is destroy", name, value);
                coreCli.unregisterListener(listenerPath);
            }

        }, listenerPath);
    }

    public Object getSwitcher(String name) throws IOException, ClassNotFoundException {
        return this.coreCli.readObjectData("/" + nameSpace + "/" + name);
    }

    public String getStringSwitcher(String name) throws IOException, ClassNotFoundException {
        return this.coreCli.readStringData("/" + nameSpace + "/" + name);
    }

    public void shutdown() {
        this.coreCli.close();
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        SwitchContainer switchContainer = new SwitchContainer("127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183,127.0.0.1:3184", "test001");

        switchContainer.registerStringSwitcher("strSwitch", "version-0");

        Scanner scanner = new Scanner(System.in);
        scanner.next();
        switchContainer.shutdown();
    }

}
