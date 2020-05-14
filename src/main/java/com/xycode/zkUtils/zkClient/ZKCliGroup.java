package com.xycode.zkUtils.zkClient;

import com.xycode.zkUtils.listener.SimpleEventListener;
import com.xycode.zkUtils.listener.ZKListener;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.currentThread;

/**
 * ClassName: ZKConnectionGroup
 *
 * @Author: xycode
 * @Date: 2019/11/7
 * @Description: this is description of the ZKCliGroup class
 **/
public class ZKCliGroup {
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

    private ProxyZKCli[] proxyZKClis;

    //线程池状态
    private boolean closed;

    public ZKCliGroup(String zkAddr, int size) {
        assert size>0;
        this.zkAddr=zkAddr;
        if(size>MAX_CONNECTIONS) this.size = MAX_CONNECTIONS;
        else this.size=size;
        this.proxyZKClis=new ProxyZKCli[size];
        this.closed =false;
        for(int i=0;i<size;++i){
            proxyZKClis[i]=new ProxyZKCli(zkAddr,new SimpleEventListener());
        }
    }

    /**
     * 这里的getZKConnection()返回值与releaseConnection()的参数都是ZKCli,这里是为了扩展性,
     * 并且为了防止一些非法操作,内部对ZKCli进行了封装: ZKConnection,使用的连接都是ZKConnection类型的,
     * ZKConnection是一个(私有)静态内部类,为了安全不对外公开,所以这里其实也只能用ZKCli作为对外的交互参数类型.
     * @return
     */
    public synchronized ZKCli getZKConnection(){
        if(closed) return null;
        while(true){
            for(int i=0;i<size;++i){
                if(!proxyZKClis[i].state.equals(State.externalAccessible)&&proxyZKClis[i].aliveProxyConnection()){//空闲状态
//                    proxyZKClis[i].access=true;//warn: 当access为false时,貌似使用proxyZKClis[i].zkCli会出错,所以这句代码放到上面
                    proxyZKClis[i].state=State.externalAccessible;
                    logger.debug("{} get zkConnection: {}", currentThread().getName(),proxyZKClis[i].zkCli);
                    return proxyZKClis[i].zkCli;
                }
            }
            //若没找到或竞争空闲连接失败,則当前线程进入等待(等待releaseConnection()的notifyAll())
            try {
                logger.debug("{} is waiting for idle zkConnection", currentThread().getName());
                this.wait();
                logger.debug("{} is trying to scramble for idle zkConnection", currentThread().getName());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 带超时参数的getZKConnection, 若超过了指定的时间,則返回null
     * @param mills  超时时间单位 ms
     * @return
     */
    public synchronized ZKCli getZKConnection(long mills){
        if(closed) return null;
        long future=System.currentTimeMillis()+mills;//未来发生超时的时间点
        while(true){
            for(int i=0;i<size;++i){
                if(!proxyZKClis[i].state.equals(State.externalAccessible)&&proxyZKClis[i].aliveProxyConnection()){//空闲状态
//                    proxyZKClis[i].access=true;//warn: 当access为false时,貌似使用proxyZKClis[i].zkCli会出错,所以放到上面
                    proxyZKClis[i].state=State.externalAccessible;
                    logger.debug("{} get zkConnection: {}", currentThread().getName(),proxyZKClis[i].zkCli);
                    return proxyZKClis[i].zkCli;
                }
            }
            //若没找到或竞争空闲连接失败,則当前线程进入等待(等待releaseConnection()的notifyAll())
            if(System.currentTimeMillis()>=future) {//超时
                logger.warn("{} is waiting timeout", currentThread().getName());
                return null;
            }
            try {
                logger.debug("{} is waiting for idle zkConnection", currentThread().getName());
                this.wait(mills);
                if(System.currentTimeMillis()>=future){//超时
                    logger.warn("{} is waiting timeout", currentThread().getName());
                    return null;
                }else{//被其它线程唤醒(notify),并且还没到超时时间,这时就去争抢连接
                    logger.debug("{} is trying to scramble for idle zkConnection", currentThread().getName());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 释放一个连接,还回到连接池中
     * @param connection
     */
    public synchronized void releaseZKConnection(ZKCli connection){
        if(closed) return;
        for(int i=0;i<size;++i){
            if(!proxyZKClis[i].aliveProxyConnection()){//检测到一个连接已失效,就重建一个连接
                logger.warn("Detect ZKCliGroup's connection failure, try to create a new connection");
                //先close,即释放一些资源
                proxyZKClis[i].closeProxyConnection();
                proxyZKClis[i]=new ProxyZKCli(zkAddr,new SimpleEventListener());
            }
            if(proxyZKClis[i].zkCli ==connection){//若连接是待释放连接
                //有连接空出来了,通知所有处于等待的线程
                logger.debug("{} release zkConnection: {}", currentThread().getName(),proxyZKClis[i].zkCli);
//                proxyZKClis[i].access=false;//warn: 当access为false时,貌似使用proxyZKClis[i].zkCli会出错,所以放到下面
                proxyZKClis[i].state=State.externalInaccessible;
                this.notifyAll();
            }
        }
    }

    public int size(){
        return this.size;
    }

    public boolean isClosed(){
        return this.closed;
    }

    //关闭连接池
    public void shutdown(){
        closed =true;
        for(int i=0;i<size;++i){
            if(proxyZKClis[i]!=null){
                proxyZKClis[i].closeProxyConnection();
                proxyZKClis[i]=null;//help GC
            }
        }
        //help GC
        zkAddr=null;
    }

    //状态
    enum State{
        internalAccessible,
        externalAccessible,
        externalInaccessible
    }
    private static class ProxyZKCli{
        private static final List<String> BLACK_LIST=new ArrayList<String>(){//外部不能访问的方法列表
            {
                add("close");
                add("addAuth");
                add("reConnect");
            }
        };

        private volatile State state=State.internalAccessible;
        private volatile ZKCli zkCli;

        private String zkAddress;
        private ZKListener zkListener;

        public ProxyZKCli(String zkAddress, ZKListener zkListener) {
            this.zkAddress = zkAddress;
            this.zkListener = zkListener;
            this.zkCli = getProxyZKCli();
        }

        public ProxyZKCli(ZKCli zkCli) {
            this.zkCli = zkCli;
        }

        private boolean checkState(Method method){
            if(state.equals(State.internalAccessible)) return true;
            else if(state.equals(State.externalAccessible)){
                if(BLACK_LIST.contains(method.getName())){
                    logger.warn("Illegal Operation: inaccessible operation");
                    return false;
                }else return true;
            }else{
                logger.warn("Illegal Operation: Released Connection is inaccessible");
                return false;
            }
        }

        private ZKCli getProxyZKCli(){
            Enhancer enhancer=new Enhancer();
            enhancer.setSuperclass(ZKCli.class);
            enhancer.setCallback(new MethodInterceptor() {
                @Override
                public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
                    if(!checkState(method)){
                        throw new IllegalStateException("Illegal Operation");
                    }
                    Object result=proxy.invokeSuper(obj,args);
                    return result;
                }
            });
            return (ZKCli) enhancer.create(new Class<?>[]{String.class,ZKListener.class},new Object[]{zkAddress,zkListener});
        }

        private void closeProxyConnection() {
            if(zkCli ==null) return;
            boolean flag=false;
            State tmpState=state;
            try {
                if(!state.equals(State.internalAccessible)){
                    state=State.internalAccessible;
                    flag=true;
                }
                zkCli.close();
                zkCli =null;//help GC
            }finally {
                if(flag) state=tmpState;
            }
        }

        private boolean aliveProxyConnection(){
            if(zkCli ==null) return false;
            boolean flag=false;
            State tmpState=state;
            try {
                if(!state.equals(State.internalAccessible)){
                    state=State.internalAccessible;
                    flag=true;
                }
                //如果一切正常的话,finally实际会在return之前执行,
                //但是若return的结果需要一个函数来计算,那么这个函数的计算过程实际上是先于finally的,
                //所以这里不会出现finally把access修改后再return最终结果,
                //实际上的流程是先计算出zkCli.alive(),然后把计算结果存在当前函数的栈中,然后在真正return这个结果之前会执行finally语句块
                //所以说access肯定是可访问的,zkCli.alive()可以正常执行
                return zkCli.alive();
            }finally {
                if(flag) state=tmpState;
            }
        }

    }

    public static void main(String[] args) {
        ZKCliGroup zkCliGroup=new ZKCliGroup("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",5);

        //tip: test1
        try {
            ZKCli zkCli=zkCliGroup.getZKConnection();
            System.out.println(zkCli.alive());
            zkCli.close();//非法操作
            zkCliGroup.releaseZKConnection(zkCli);
            System.out.println(zkCli.alive());//非法操作
        }finally {
            zkCliGroup.shutdown();
        }

        //tip: test2
//        ZKCli[] zkClis=new ZKCli[5];
//        for(int i=0;i<5;++i){
//            zkClis[i]=zkCliGroup.getZKConnection();
//            System.out.println(zkClis[i].alive());
//        }
//        for(int i=0;i<5;++i){
//            zkCliGroup.releaseZKConnection(zkClis[i]);
////            System.out.println(zkClis[i].alive());
//        }
//        System.out.println("------------------------------------");
//        for(int i=0;i<5;++i){
//            zkClis[i]=zkCliGroup.getZKConnection();
//            System.out.println(zkClis[i].alive());
//        }
//        for(int i=0;i<5;++i){
//            zkCliGroup.releaseZKConnection(zkClis[i]);
//        }
//        zkCliGroup.shutdown();


        //tip: test3,测试多线程并发的情况,如果getZKConnection()与releaseZKConnection()不是同步方法的话会有线程安全的问题
//        CountDownLatch countDownLatch=new CountDownLatch(20);
//        for(int i=0;i<20;++i){
//            new Thread(()->{
//               ZKCli zkCli=zkCliGroup.getZKConnection();
//               zkCli.alive();
//                try {
//                    TimeUnit.SECONDS.sleep(2);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                zkCliGroup.releaseZKConnection(zkCli);
//                countDownLatch.countDown();
//            }).start();
//        }
//
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        zkCliGroup.shutdown();
    }
}
