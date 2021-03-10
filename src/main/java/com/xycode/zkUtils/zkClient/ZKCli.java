package com.xycode.zkUtils.zkClient;

import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.internel.ZKClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * ZKCli的读写操作,默认都是不激活监听器的,
 * 若需要激活监听器,需要手动指定isWatch,或者调用triggerXXX()
 */
public class ZKCli implements AutoCloseable {
    private ZooKeeper zk;
    private String zkAddress;

    public ZKCli(String zkAddress) {
        this.zkAddress = zkAddress;
        try {
            this.zk = ZKClientFactory.createDefaultZKClient(zkAddress);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public ZKCli(String zkAddress, String zkVerification) {
        this.zkAddress = zkAddress;
        try {
            this.zk = ZKClientFactory.createDefaultZKClient(zkAddress);
            if (StringUtils.isNotEmpty(zkVerification)) {
                addAuth(zkVerification);
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public ZKCli(String zkAddress, ZKListener listener) {
        this.zkAddress = zkAddress;
        try {
            zk = ZKClientFactory.createZKClient(zkAddress, listener);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * 轮询式的等待连接成功
     *
     * @param timeout (ms)
     * @throws TimeoutException
     */
    public void waitConnected(long timeout) throws TimeoutException {
        long t = 0;
        long sep = 20;
        while (zk.getState() != ZooKeeper.States.CONNECTED) {
            try {
                Thread.sleep(sep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            t += sep;
            if (t >= timeout) throw new TimeoutException("waitConnectedTimeout!");
        }
    }

    public void reConnect() {
        if (this.zkAddress == null) return;
        else {
            try {
                this.zk = ZKClientFactory.createDefaultZKClient(this.zkAddress);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public void reConnect(ZKListener listener) {
        if (this.zkAddress == null) return;
        else {
            try {
                close();
                this.zk = ZKClientFactory.createZKClient(this.zkAddress, listener);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(zk, zkAddress);
    }

    /**
     * 相当于addAuth("digest",auth), 也就是采用 <u>用户名:密码</u> 的方式来认证
     *
     * @param auth username:password
     */
    public void addAuth(String auth) {
        addAuth("digest", auth);
    }

    /**
     * @param scheme <p>认证的方式,有以下几种:</p>
     *               <p>world：默认方式，相当于全世界都能访问,也就是默认没有认证</p>
     *               <p>auth：代表已经认证通过的用户(cli中可以通过addauth digest user:pwd 来添加当前上下文中的授权用户)</p>
     *               <p>digest：即用户名:密码这种方式认证，这也是业务系统中最常用的</p>
     *               <p>ip：使用Ip地址认证</p>
     * @param auth   认证时所用的信息
     */
    public void addAuth(String scheme, String auth) {
        zk.addAuthInfo(scheme, auth.getBytes());
    }

    public String createPersistent(String path, String data, ArrayList<ACL> ids, boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (data == null) data = "";
        return zk.create(path, data.getBytes(), ids, CreateMode.PERSISTENT);
    }

    public String createPersistent(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createPersistent(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isWatch);
    }

    public String createPersistent(String path, String data) throws KeeperException, InterruptedException {
        return createPersistent(path, data, false);
    }

    public String createPersistent(String path) throws KeeperException, InterruptedException {
        return createPersistent(path, "");
    }

    //创建顺序的永久节点
    public String createPersistentSeq(String path, String data, ArrayList<ACL> ids, boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (data == null) data = "";
        return zk.create(path, data.getBytes(), ids, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public String createPersistentSeq(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createPersistentSeq(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isWatch);
    }

    public String createPersistentSeq(String path, String data) throws KeeperException, InterruptedException {
        return createPersistentSeq(path, data, false);
    }

    public String createPersistentSeq(String path) throws KeeperException, InterruptedException {
        return createPersistentSeq(path, "");
    }

    /**
     * create a ephemeral node
     *
     * @param path
     * @param data
     * @param ids
     * @throws KeeperException
     * @throws InterruptedException
     */

    public String createEphemeral(String path, String data, ArrayList<ACL> ids, boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (data == null) data = "";
        return zk.create(path, data.getBytes(), ids, CreateMode.EPHEMERAL);
    }

    public String createEphemeral(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createEphemeral(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isWatch);
    }

    public String createEphemeral(String path, String data) throws KeeperException, InterruptedException {
        return createEphemeral(path, data, false);
    }

    public String createEphemeral(String path) throws KeeperException, InterruptedException {
        return createEphemeral(path, "");
    }

    //创建顺序的临时节点
    public String createEphemeralSeq(String path, String data, ArrayList<ACL> ids, boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (data == null) data = "";
        return zk.create(path, data.getBytes(), ids, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public String createEphemeralSeq(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, isWatch);
    }

    public String createEphemeralSeq(String path, String data) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path, data, false);
    }

    public String createEphemeralSeq(String path) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path, "");
    }


    /**
     * set data at a node
     *
     * @param path
     * @param data
     * @param isWatch
     */

    public void writeData(String path, byte[] data, boolean isWatch) {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
            zk.setData(path, data, -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeStringData(String path, String data) {
        writeStringData(path, data, false);
    }

    public void writeStringData(String path, String data, boolean isWatch) {
        try {
            if (isWatch) zk.exists(path, true);//先注册监听器
            if (data == null) data = "";
            zk.setData(path, data.getBytes(), -1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeIntData(String path, Integer data) {
        writeStringData(path, data.toString(), false);
    }

    public void writeLongData(String path, Long data) {
        writeStringData(path, data.toString(), false);
    }

    public void writeDoubleData(String path, Double data) {
        writeStringData(path, data.toString(), false);
    }

    @Deprecated
    public <T extends Serializable> void writeObjectData(String path, T t, boolean isWatch) throws IOException {
        byte[] bytes = null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(t);
            oos.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (bytes == null) throw new IOException("Serializable failure");
        writeData(path, bytes, isWatch);
    }

    /**
     * 写入一个对象到zk中
     *
     * @param path 路径
     * @param t    待写入到对象
     * @param <T>  对象的类型,需实现Serializable接口
     * @throws IOException 对象序列化失败时,抛出该异常
     * @deprecated 目前的序列化方式效率较低, 不太建议使用
     */
    @Deprecated
    public <T extends Serializable> void writeObjectData(String path, T t) throws IOException {
        writeObjectData(path, t, false);
    }

    /**
     * read data from a node
     *
     * @param path
     * @return
     */
    public byte[] readData(String path) {
        try {
            return zk.getData(path, false, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public String readStringData(String path) {
        return new String(readData(path), StandardCharsets.UTF_8);
    }

    public Integer readIntData(String path) {
        return Integer.valueOf(readStringData(path));
    }

    public Long readLongData(String path) {
        return Long.valueOf(readStringData(path));
    }

    public Double readDoubleData(String path) {
        return Double.valueOf(readStringData(path));
    }

    /**
     * 从zk中读取数据,并反序列化成一个对象
     *
     * @param path zk路径
     * @return 反序列化成的对象
     * @throws ClassNotFoundException 待反序列化的数据不合法时,抛出该异常
     * @throws IOException            数据读取失败时,抛出该异常
     * @deprecated 目前的反序列化方式效率较低, 不太建议使用
     */
    @Deprecated
    public Object readObjectData(String path) throws ClassNotFoundException, IOException {
        byte[] bytes = null;
        try {
            bytes = zk.getData(path, false, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (bytes == null) return null;
        ObjectInputStream ois = null;
        ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return ois.readObject();
    }

    public List<String> getChildren(String path, boolean isWatch) {
        List<String> result = null;
        try {
            result = zk.getChildren(path, isWatch);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获得path下的子节点名字(非递归方式,对于path下子节点还有子节点的,不会返回孙子节点的名字)
     *
     * @param path
     * @return 若返回null, 表明还没有这个节点
     */
    public List<String> getChildren(String path) {
        return getChildren(path, false);
    }

    public boolean exists(String path) {
        try {
            return zk.exists(path, false) != null;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void delete(String path, boolean isWatch) throws KeeperException, InterruptedException {
        if (isWatch) zk.exists(path, true);
        zk.delete(path, -1);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        delete(path, false);
    }

    public boolean alive() {//是否alive
        return zk != null && zk.getState().equals(ZooKeeper.States.CONNECTED);
    }

    /**
     * 支持try with resource
     */
    @Override
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void triggerListener(String path) {
        try {
            zk.exists(path, true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void triggerChildListener(String path) {
        try {
            zk.getChildren(path, true);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * register a default(global) listener
     *
     * @param listener
     */
    public void registerDefaultListener(ZKListener listener) {
        zk.register(listener);
    }

    /**
     * unregister default(global) listener
     */
    public void unregisterDefaultListener() {
        //注册一个空监听器,会覆盖掉之前的监听器 or 直接置为null
        zk.register(null);
    }

    /**
     * register a listener at a special path
     *
     * @param listener
     * @param path
     */
    public void registerListener(ZKListener listener, String path) {
        listener.listen(path);
        try {
            zk.exists(path, listener);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void registerChildListener(ZKListener listener, String path) {
        listener.listen(path);
        try {
            zk.getChildren(path, listener);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unregisterListener(String path, Watcher.WatcherType watcherType) {
        try {
            zk.removeAllWatches(path, watcherType, true);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void unregisterListener(String path) {
        unregisterListener(path, Watcher.WatcherType.Any);
    }

}
