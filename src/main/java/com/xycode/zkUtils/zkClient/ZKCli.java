package com.xycode.zkUtils.zkClient;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import com.xycode.zkUtils.zkClient.internel.ZKClientFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.testng.annotations.Test;

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
public class ZKCli {
    private ZooKeeper zk;
    private String zkAddress;

//    public ZKCli(ZKCli zkCli) {
//        this.zkAddress=zkCli.zkAddress;
//        this.zk=zkCli.zk;
//    }

    public ZKCli(String zkAddress) {
        this.zkAddress = zkAddress;
        try {
            this.zk= ZKClientFactory.createDefaultZKClient(zkAddress);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public ZKCli(String zkAddress, ZKListener listener) {
        this.zkAddress = zkAddress;
        try {
            zk= ZKClientFactory.createZKClient(zkAddress,listener);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param timeout (ms)
     * @throws TimeoutException
     */
    public void waitConnected(long timeout) throws TimeoutException {
        long t=0;
        long sep=20;
        while(zk.getState()!= ZooKeeper.States.CONNECTED) {
            try {
                Thread.sleep(sep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            t+=sep;
            if(t>=timeout) throw new TimeoutException("waitConnectedTimeout!");
        }
    }

    public void reConnect(){
        if(this.zkAddress==null) return;
        else {
            try {
                this.zk= ZKClientFactory.createDefaultZKClient(this.zkAddress);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    public void reConnect(ZKListener listener){
        if(this.zkAddress==null) return;
        else {
            try {
                close();
                this.zk= ZKClientFactory.createZKClient(this.zkAddress,listener);
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(zk,zkAddress);
    }

    /**
     * equivalent to addAuth("digest",auth)
     * @param auth username:password
     */
    public void addAuth(String auth){
        addAuth("digest",auth);
    }

    /**
     *
     * @param scheme scheme of certification
     *               world：默认方式，相当于全世界都能访问
     *               auth：代表已经认证通过的用户(cli中可以通过addauth digest user:pwd 来添加当前上下文中的授权用户)
     *               digest：即用户名:密码这种方式认证，这也是业务系统中最常用的
     *               ip：使用Ip地址认证
     * @param auth authorization information
     */
    public void addAuth(String scheme,String auth){
        zk.addAuthInfo(scheme,auth.getBytes());
    }

    public String createPersistent(String path, String data, ArrayList<ACL> ids,boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk.create(path,data.getBytes(),ids, CreateMode.PERSISTENT);
    }

    public String createPersistent(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createPersistent(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,isWatch);
    }

    /**
     * create a persistent node
     * @param path
     * @param data
     * @param ids ACL
     */
    public String createPersistent(String path, String data, ArrayList<ACL> ids) throws KeeperException, InterruptedException {
        return createPersistent(path,data,ids,false);
    }

    public String createPersistent(String path, String data) throws KeeperException, InterruptedException {
        return createPersistent(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    //创建顺序的永久节点
    public String createPersistentSeq(String path,String data,ArrayList<ACL> ids,boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk.create(path,data.getBytes(),ids,CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public String createPersistentSeq(String path,String data,ArrayList<ACL> ids) throws KeeperException, InterruptedException {
        return createPersistentSeq(path,data,ids,false);
    }

    public String createPersistentSeq(String path,String data,boolean isWatch) throws KeeperException, InterruptedException {
        return createPersistentSeq(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,isWatch);
    }


    /**
     * create a ephemeral node
     * @param path
     * @param data
     * @param ids
     * @throws KeeperException
     * @throws InterruptedException
     */

    public String createEphemeral(String path, String data, ArrayList<ACL> ids,boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk.create(path,data.getBytes(),ids, CreateMode.EPHEMERAL);
    }

    public String createEphemeral(String path, String data, boolean isWatch) throws KeeperException, InterruptedException {
        return createEphemeral(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,isWatch);
    }

    public String createEphemeral(String path, String data, ArrayList<ACL> ids) throws KeeperException, InterruptedException {
        return createEphemeral(path,data,ids,false);
    }

    public String createEphemeral(String path,String data) throws KeeperException, InterruptedException {
        return createEphemeral(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
    //创建顺序的临时节点
    public String createEphemeralSeq(String path,String data,ArrayList<ACL> ids,boolean isWatch) throws KeeperException, InterruptedException {
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return zk.create(path,data.getBytes(),ids,CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public String createEphemeralSeq(String path,String data,ArrayList<ACL> ids) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path,data,ids,false);
    }

    public String createEphemeralSeq(String path,String data) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,false);
    }

    public String createEphemeralSeq(String path,String data,boolean isWatch) throws KeeperException, InterruptedException {
        return createEphemeralSeq(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,isWatch);
    }



    /**
     *
     * set data at a node
     * @param path
     * @param data
     */

    public void writeData(String path,byte[] data,boolean isWatch){
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
            zk.setData(path,data,-1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeStringData(String path,String data){
        writeStringData(path,data,false);
    }

    public void writeStringData(String path,String data,boolean isWatch){
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
            zk.setData(path,data.getBytes(),-1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeIntData(String path,Integer data){
        writeIntData(path,data,false);
    }

    public void writeIntData(String path,Integer data,boolean isWatch){
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
            zk.setData(path,data.toString().getBytes(),-1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeLongData(String path,Long data){
        writeLongData(path,data,false);
    }

    public void writeLongData(String path,Long data,boolean isWatch){
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
            zk.setData(path,data.toString().getBytes(),-1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void writeDoubleData(String path,Double data){
        writeDoubleData(path,data,false);
    }

    public void writeDoubleData(String path,Double data,boolean isWatch){
        try {
            if(isWatch) zk.exists(path,true);//先注册监听器
            zk.setData(path,data.toString().getBytes(),-1);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    public <T extends Serializable> void writeObjectData(String path,T t,boolean isWatch) throws IOException {
        byte[] bytes = null;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos);)
        {
            oos.writeObject(t);
            oos.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(bytes==null) throw new IOException("Serializable failure");
        writeData(path,bytes,isWatch);
    }

    @Deprecated
    public <T extends Serializable> void writeObjectData(String path,T t) throws IOException {
        writeObjectData(path,t,false);
    }

    /**
     * read data from a node
     * @param path
     * @return
     */
    public byte[] readData(String path){
        try {
            return zk.getData(path,false,null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String readStringData(String path){
        return new String(readData(path), StandardCharsets.UTF_8);
    }

    public Integer readIntData(String path){
        return Integer.valueOf(readStringData(path));
    }

    public Long readLongData(String path){
        return Long.valueOf(readStringData(path));
    }

    public Double readDoubleData(String path){
        return Double.valueOf(readStringData(path));
    }

    @Deprecated
    public Object readObjectData(String path) throws ClassNotFoundException, IOException {
        byte[] bytes=null;
        try {
            bytes=zk.getData(path,false,null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        if(bytes==null) return null;
        ObjectInputStream ois = null;
        ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return ois.readObject();
    }

    public List<String> getChildren(String path,boolean isWatch){
        List<String> result=null;
        try {
            result=zk.getChildren(path,isWatch);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获得path下的子节点名字(非递归方式,对于path下子节点还有子节点的,不会返回孙子节点的名字)
     * @param path
     * @return 若返回null,表明还没有这个节点
     */
    public List<String> getChildren(String path){
        return getChildren(path,false);
    }

    public boolean exists(String path){
        try {
            return zk.exists(path, false) != null;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void delete(String path,boolean isWatch) throws KeeperException, InterruptedException {
        if(isWatch) zk.exists(path,true);
        zk.delete(path,-1);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        delete(path,false);
    }

    public boolean alive(){//是否alive
        return zk!=null&&zk.getState().equals(ZooKeeper.States.CONNECTED);
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void triggerListener(String path){
        try {
            zk.exists(path,true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void triggerChildListener(String path){
        try {
            zk.getChildren(path,true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * register a default(global) listener,通常和triggerListener/triggerChildListener结合起来使用
     * @param listener
     */
    public void registerDefaultListener(ZKListener listener){
        zk.register(listener);
    }

    /**
     * unregister default(global) listener
     */
    public void unregisterDefaultListener(){
        //注册一个空监听器,会覆盖掉之前的监听器 or 直接置为null
        zk.register(null);
    }

    /**
     * register a listener at a special path
     * @param listener
     * @param path
     */
    public void registerListener(ZKListener listener, String path){
        listener.listen(path);
        try {
            zk.exists(path,listener);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 注册并触发指定路径上的监听器
     * @param listener
     * @param path
     */
    public void registerChildListener(ZKListener listener, String path){
        listener.listen(path);
        try {
            zk.getChildren(path,listener);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unregisterListener(String path,Watcher.WatcherType watcherType){
        try {
            zk.removeAllWatches(path, watcherType,true);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void unregisterListener(String path){
        unregisterListener(path, Watcher.WatcherType.Any);
    }

    @Test
    public void test() {
        //test
        String path="/listener";
        String ZKC_ADDRESS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

        ZKCli zkCli=new ZKCli(ZKC_ADDRESS);
        try {
            if(!zkCli.exists(path))
                zkCli.createPersistent(path,"");
            zkCli.createEphemeralSeq(path+"/1","");
            String s=zkCli.createEphemeralSeq(path+"/1","");
            System.out.println(s);
            zkCli.registerChildListener(new SimpleEventListener() {
                @Override
                public void NodeDeletedHandler(WatchedEvent event) {
                    System.out.println("[delete]: "+s);
                }
            },path);

            //tip: 等效的写法
//            zkCli.registerDefaultListener(new SimpleEventListener() {
//                @Override
//                public void NodeDeletedHandler(WatchedEvent event) {
//                    System.out.println("[delete]: "+s);
//                }
//            });
//            zkCli.triggerChildListener(path);

            //打印path的子节点
            List<String> l=zkCli.getChildren(path);
            for(String data:l){
                System.out.println(data);
            }
            zkCli.delete(s);
            zkCli.close();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }


    }

}
