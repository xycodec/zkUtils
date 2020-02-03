package com.xycode.zkUtils.listener.multipleListener;

import com.xycode.zkUtils.listener.ZKListener;

import java.util.List;

/**
 * InterfaceName: ZKMultipleListener
 *
 * @Author: xycode
 * @Date: 2019/11/11
 * @Description: this is description of the interface
 **/
public interface ZKMultipleListener extends ZKListener {

    /**
     * 监听指定paths,依此回调process()
     * 该方法可以多次调用,以指定paths
     * @param paths
     */
    void listen(List<String> paths);

    @Override
    @Deprecated default void listen(String path){//对于ZKMultipleListener来说,禁止使用该方法
        throw new IllegalArgumentException("Argument should be a list");
    }
}
