package com.xycode.zkUtils.zkClient;

import java.util.concurrent.BlockingQueue;

/**
 * ClassName: ZKCliGroup
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
     */
    private int workerNum;
    private BlockingQueue<ZKCli> workerGroup;
    private BlockingQueue<ZKCli> idleWorkerGroup;


}
