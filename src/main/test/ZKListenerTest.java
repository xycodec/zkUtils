import com.xycode.zkUtils.listener.AbstractEventListener;
import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.zkClient.ZKCli;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 * 测试ZKListener
 * @author: xycode
 * @date: 2019/11/22
 */
public class ZKListenerTest {
    @Test
    public void testEventListener() {
        String path = "/listener";
        MyZKListener myZKListener = new MyZKListener(path);
        ZKCli zkClient = new ZKCli("127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183", myZKListener);
        try {
            zkClient.waitConnected(5000);
            if (!zkClient.exists(path)) zkClient.createPersistent(path, "");
            zkClient.writeStringData(path, "changed");
            System.out.println(zkClient.readStringData(path));

            zkClient.writeDoubleData(path, Double.MAX_VALUE);
            System.out.println(zkClient.readDoubleData(path));

            zkClient.writeIntData(path, Integer.MIN_VALUE);
            System.out.println(zkClient.readIntData(path));

            zkClient.writeLongData(path, Long.MAX_VALUE);
            System.out.println(zkClient.readLongData(path));


            zkClient.createEphemeral(path + "/test1", "");
            zkClient.createEphemeral(path + "/test2", "");

            zkClient.triggerChildListener(path);
            zkClient.delete(path + "/test1");

            TimeUnit.SECONDS.sleep(10);
            myZKListener.zkCli.delete(path);
            zkClient.close();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    /**
     * 测试zk某个path是否支持多个监听器
     * <p>
     * 实际测试证明: 一个path可以支持多个监听器,包括全局的默认监听器
     */
    @Test
    public void testMultiEventListener() {
        String path = "/listener";
        //定义无状态的监听器,即没没有指定path
        SimpleEventListener listener1 = new SimpleEventListener() {
            @Override
            public void NodeDataChangedHandler(WatchedEvent event) {
                System.out.println("listener1: " + event.getPath() + "'data changed");
            }
        };
        SimpleEventListener listener2 = new SimpleEventListener() {
            @Override
            public void NodeDataChangedHandler(WatchedEvent event) {
                System.out.println("listener2: " + event.getPath() + "'data changed");
            }
        };


        try (ZKCli zkClient = new ZKCli("127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183", new SimpleEventListener(path) {
            @Override
            public void NodeDataChangedHandler(WatchedEvent event) {
                System.out.println("defaultListener: " + event.getPath() + "'data changed");
            }
        })) {
            zkClient.waitConnected(5000);
            if (!zkClient.exists(path)) zkClient.createPersistent(path, "");
            zkClient.registerListener(listener1, path);
            zkClient.registerListener(listener2, path);
            zkClient.triggerListener(path);
            zkClient.writeStringData(path, "changed");

            zkClient.delete(path);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    static class MyZKListener extends AbstractEventListener {
        private ZKCli zkCli = new ZKCli("127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183");

        @Override
        public void NodeCreatedHandler(WatchedEvent event) {
            System.out.println("[created]: " + path);
        }

        @Override
        public void NodeChildrenChangedHandler(WatchedEvent event) {
            int cnt = ThreadLocalRandom.current().nextInt(1000);
            System.out.println("[childrenChanged start]: " + path + ", " + cnt);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
//            zkCli.triggerChildListener(path);
                zkCli.delete(path + "/test2");
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("[childrenChanged done]: " + path + ", " + cnt);
        }

        @Override
        public void NodeDataChangedHandler(WatchedEvent event) {
            System.out.println("[dataChanged]: " + path);
        }

        @Override
        public void NodeDeletedHandler(WatchedEvent event) {
            System.out.println("[deleted start]: " + path);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("[deleted done]: " + path);
        }

        public MyZKListener(String path) {
            super(path);
        }
    }
}
