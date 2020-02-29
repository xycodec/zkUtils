package com.xycode.zkUtils.zkClient;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static java.lang.Thread.currentThread;

/**
 * ClassName: ZKConnectionGroup
 *
 * @Author: xycode
 * @Date: 2019/11/7
 * @Description: this is description of the ZKCliGroup class
 **/
public class ZKConnectionPool {
    /**
     * TODO: 一个优化性能的想法,即使用一组ZKCli,这一组ZKCli中只有一个关联lockPath(看成boss),
     *      其余ZKCli是worker,服务于boss;这样就可以粗化CountDownLatch的粒度,这样的话就是锁一组ZKCli;
     *      问题: 当boss挂掉时,worker的任何操作都将非法了
     *      方案1: 设置一个supervisor角色,用于监听lockPath,若监听到lockPath的delete事件,就close所有的worker
     *      但是这样又引入了一个问题,即supervisor挂掉了怎么办?
     *      方案2: 增加supervisor角色数量...(有点挫...)
     *
     * tip: 尚未实现...
     * fix: 退而求其次,实现一个通用的连接池好了...
     */
    private static final Logger logger= LoggerFactory.getLogger("myLogger");
    private static final int MAX_CONNECTIONS=16;

    private String zkAddr;
    private int size;
    private ZKConnection[] connections;
    //连接状态数组,使用原子类型的array,0代表空闲,1代表繁忙
    private AtomicIntegerArray states;

    //线程池状态
    private boolean isClosed;

    public ZKConnectionPool(String zkAddr, int size) {
        assert size>0&&size<=MAX_CONNECTIONS;
        this.zkAddr=zkAddr;
        this.size = size;
        this.connections =new ZKConnection[size];
        this.states=new AtomicIntegerArray(new int[size]);
        this.isClosed=false;
        for(int i=0;i<size;++i){
            connections[i]=new ZKConnection(zkAddr,new SimpleEventListener());
        }
    }

    /**
     * 这里的getZKConnection()返回值与releaseConnection()的参数都是ZKCli,这里是为了扩展性,
     * 并且为了防止一些非法操作,内部对ZKCli进行了封装: ZKConnection,使用的连接都是ZKConnection类型的,
     * ZKConnection是一个(私有)静态内部类,为了安全不对外公开,所以这里其实也只能用ZKCli作为对外的交互参数类型.
     * @return
     */
    public ZKCli getZKConnection(){
        assert !isClosed;
        while(true){
            for(int i=0;i<size;++i){
                if(states.get(i)==0&&connections[i].alive()){//空闲状态
                    if(states.compareAndSet(i,0,1)){//CAS成功,说明成功竞争到连接
                        logger.debug("{} get zkConnection: {}", currentThread().getName(),connections[i]);
                        return connections[i];
                    }
                }
            }
            //若没找到或竞争空闲连接失败,則当前线程进入等待(等待releaseConnection()的notifyAll())
            synchronized (this){
                try {
                    logger.debug("{} is waiting for idle zkConnection", currentThread().getName());
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 使用反射方式关闭连接
     * @param connection 封装后的ZKCli
     */
    private static void closeConnection(ZKConnection connection){
        assert connection!=null;
        try {
            Field field=ZKConnection.class.getSuperclass().getDeclaredField("zk");
            field.setAccessible(true);
            ZooKeeper zk=(ZooKeeper)field.get(connection);
            zk.close();
//            System.out.println(connection.alive());// ->false
        } catch (NoSuchFieldException | IllegalAccessException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放一个连接,还回到连接池中
     * @param connection
     */
    public void releaseConnection(ZKCli connection){
        assert !isClosed;
        assert connection instanceof ZKConnection;
        ZKConnection zkConnection= (ZKConnection) connection;

        for(int i=0;i<size;++i){
            if(!connections[i].alive()){//检测到一个连接已失效,就重建一个连接
                logger.warn("Detect ZKCliGroup's connection failure, try to create a new connection");
                //先close,即释放一些资源
                closeConnection(zkConnection);
                connections[i]=new ZKConnection(zkAddr,new SimpleEventListener());
            }
            if(connections[i]==zkConnection){//若连接是待释放连接
                states.set(i,0);//因为连接只能被一个线程持有,所以这里是线程安全的,不用CAS

                //有连接空出来了,通知所有处于等待的线程
                logger.debug("{} release zkConnection: {}", currentThread().getName(),zkConnection);
                synchronized (this){
                    this.notifyAll();
                }
            }
        }
    }

    //关闭连接池
    public void shutdown(){
        for(int i=0;i<size;++i){
            if(connections[i]!=null){
                closeConnection(connections[i]);
            }
        }
        isClosed=true;
        //help GC
        states=null;
        connections=null;
        zkAddr=null;
    }
    //对ZKCli进行封装,主要是防止外部对ZKCliGroup内的ZKCli进行一些非法操作
    private static class ZKConnection extends ZKCli{
        public ZKConnection(String zkAddress) {
            super(zkAddress);
        }

        public ZKConnection(String zkAddress, ZKListener listener) {
            super(zkAddress, listener);
        }

        @Override
        public void close() {
            logger.error("Illegal Operation: Close ZKCliGroup's connection externally");
        }

        @Override
        public void reConnect() {
            logger.error("Illegal Operation: ReConnect ZKCliGroup's connection externally");
        }

        @Override
        public void reConnect(ZKListener listener) {
            logger.error("Illegal Operation: ReConnect ZKCliGroup's connection externally");
        }

        @Override
        public void addAuth(String auth) {
            logger.error("Illegal Operation: Add auth to ZKCliGroup's connection externally");
        }

        @Override
        public void addAuth(String scheme, String auth) {
            logger.error("Illegal Operation: Add auth to ZKCliGroup's connection externally");
        }

    }

    public static void main(String[] args) {
        ZKConnectionPool zkConnectionPool =new ZKConnectionPool("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",5);
        ZKCli zkCli= zkConnectionPool.getZKConnection();
        zkCli.close();//warn: 非法操作
        zkConnectionPool.releaseConnection(zkCli);
        zkConnectionPool.shutdown();

        //测试反射方式关闭连接
//        ZKCli zkCli=new ZKConnection("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
//        System.out.println(zkCli.alive());// -> true
//        try {
//            Class<?> clazz=ZKConnection.class.getSuperclass();//虽然声明是ZKCli类型,但是JVM能够识别出来是ZKConnection类型,所以这里仍需要getSuperclass()
//            Field field=clazz.getDeclaredField("zk");
//            field.setAccessible(true);
//            ZooKeeper zk=(ZooKeeper)field.get(zkCli);
//            zk.close();
//            System.out.println(zkCli.alive());// -> false
//        } catch (NoSuchFieldException | IllegalAccessException | InterruptedException e) {
//            e.printStackTrace();
//        }

    }
}
