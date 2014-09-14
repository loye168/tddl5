package com.alibaba.cobar.net;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.cobar.net.buffer.BufferPool;
import com.alibaba.cobar.net.handler.CommandCount;
import com.alibaba.cobar.net.util.ExecutorUtil;
import com.alibaba.cobar.net.util.NameableExecutor;

/**
 * @author xianmao.hexm
 */
public final class NIOProcessor {

    private static final int                              DEFAULT_BUFFER_SIZE       = 1024 * 1024 * 16;
    private static final int                              DEFAULT_BUFFER_CHUNK_SIZE = 4096;
    private static final int                              AVAILABLE_PROCESSORS      = Runtime.getRuntime()
                                                                                        .availableProcessors();

    private final String                                  name;
    private final NIOReactor                              reactor;
    private final BufferPool                              bufferPool;
    private final NameableExecutor                        handler;
    // 做kill操作的线程池
    private final NameableExecutor                        killExecutor;
    private final ConcurrentMap<Long, FrontendConnection> frontends;
    private final ConcurrentMap<Long, BackendConnection>  backends;
    private final CommandCount                            commands;
    private long                                          netInBytes;
    private long                                          netOutBytes;

    public NIOProcessor(String name) throws IOException{
        this(name, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE, AVAILABLE_PROCESSORS, AVAILABLE_PROCESSORS);
    }

    public NIOProcessor(String name, int handler, int killExecutor) throws IOException{
        this(name, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE, handler, killExecutor);
    }

    public NIOProcessor(String name, int buffer, int chunk, int handler, int killExecutor) throws IOException{
        this.name = name;
        this.reactor = new NIOReactor(name);
        this.bufferPool = new BufferPool(buffer, chunk);
        this.handler = (handler > 0) ? ExecutorUtil.create(name + "-H", handler) : null;
        this.killExecutor = (killExecutor > 0) ? ExecutorUtil.create(name + "-K", killExecutor) : null;
        this.frontends = new ConcurrentHashMap<Long, FrontendConnection>();
        this.backends = new ConcurrentHashMap<Long, BackendConnection>();
        this.commands = new CommandCount();
    }

    public String getName() {
        return name;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public int getRegisterQueueSize() {
        return reactor.getRegisterQueue().size();
    }

    public int getWriteQueueSize() {
        return reactor.getWriteQueue().size();
    }

    public NameableExecutor getHandler() {
        return handler;
    }

    public NameableExecutor getKillExecutor() {
        return killExecutor;
    }

    public void startup() {
        reactor.startup();
    }

    public void postRegister(NIOConnection c) {
        reactor.postRegister(c);
    }

    public void postWrite(NIOConnection c) {
        reactor.postWrite(c);
    }

    public CommandCount getCommands() {
        return commands;
    }

    public long getNetInBytes() {
        return netInBytes;
    }

    public void addNetInBytes(long bytes) {
        netInBytes += bytes;
    }

    public long getNetOutBytes() {
        return netOutBytes;
    }

    public void addNetOutBytes(long bytes) {
        netOutBytes += bytes;
    }

    public long getReactCount() {
        return reactor.getReactCount();
    }

    public void addFrontend(FrontendConnection c) {
        frontends.put(c.getId(), c);
    }

    public ConcurrentMap<Long, FrontendConnection> getFrontends() {
        return frontends;
    }

    public void addBackend(BackendConnection c) {
        backends.put(c.getId(), c);
    }

    public ConcurrentMap<Long, BackendConnection> getBackends() {
        return backends;
    }

    /**
     * 定时执行该方法，回收部分资源。
     */
    public void check() {
        frontendCheck();
        backendCheck();
    }

    // 前端连接检查
    private void frontendCheck() {
        Iterator<Entry<Long, FrontendConnection>> it = frontends.entrySet().iterator();
        while (it.hasNext()) {
            FrontendConnection c = it.next().getValue();

            // 删除空连接
            if (c == null) {
                it.remove();
                continue;
            }

            // 清理已关闭连接，否则空闲检查。
            if (c.isClosed()) {
                it.remove();
                c.cleanup();
            } else {
                c.idleCheck();
            }
        }
    }

    // 后端连接检查
    private void backendCheck() {
        Iterator<Entry<Long, BackendConnection>> it = backends.entrySet().iterator();
        while (it.hasNext()) {
            BackendConnection c = it.next().getValue();

            // 删除空连接
            if (c == null) {
                it.remove();
                continue;
            }

            // 清理已关闭连接，否则空闲检查。
            if (c.isClosed()) {
                it.remove();
                c.cleanup();
            } else {
                c.idleCheck();
            }
        }
    }

}
