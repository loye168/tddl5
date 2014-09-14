package com.alibaba.cobar.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;

/**
 * @author xianmao.hexm
 */
public interface NIOConnection {

    /**
     * 注册网络事件
     */
    void register(Selector selector) throws IOException;

    /**
     * 从目标端读取数据
     */
    void read() throws IOException;

    /**
     * 向目标端写出一块缓存数据
     */
    void write(ByteBuffer buffer);

    /**
     * 基于处理器队列方式的数据写出
     */
    void writeByQueue() throws IOException;

    /**
     * 基于Selector事件方式的数据写出
     */
    void writeByEvent() throws IOException;

    /**
     * 处理数据
     */
    void handleData(byte[] data);

    /**
     * 处理错误
     */
    void handleError(int errCode, Throwable t);

    /**
     * 关闭连接
     */
    boolean close();

}
