package com.alibaba.cobar.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.cobar.ErrorCode;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public final class NIOConnector extends Thread {

    private static final Logger                    logger       = LoggerFactory.getLogger(NIOConnector.class);
    private static final ConnectIdGenerator        ID_GENERATOR = new ConnectIdGenerator();

    private final String                           name;
    private final Selector                         selector;
    private final BlockingQueue<BackendConnection> connectQueue;
    private NIOProcessor[]                         processors;
    private int                                    nextProcessor;
    private long                                   connectCount;

    public NIOConnector(String name) throws IOException{
        super.setName(name);
        this.name = name;
        this.selector = Selector.open();
        this.connectQueue = new LinkedBlockingQueue<BackendConnection>();
    }

    public long getConnectCount() {
        return connectCount;
    }

    public void setProcessors(NIOProcessor[] processors) {
        this.processors = processors;
    }

    public void postConnect(BackendConnection c) {
        connectQueue.offer(c);
        selector.wakeup();
    }

    @Override
    public void run() {
        final Selector selector = this.selector;
        for (;;) {
            ++connectCount;
            try {
                selector.select(1000L);
                connect(selector);
                Set<SelectionKey> keys = selector.selectedKeys();
                try {
                    for (SelectionKey key : keys) {
                        Object att = key.attachment();
                        if (att != null && key.isValid() && key.isConnectable()) {
                            finishConnect(key, att);
                        } else {
                            key.cancel();
                        }
                    }
                } finally {
                    keys.clear();
                }
            } catch (Throwable e) {
                logger.warn(name, e);
            }
        }
    }

    private void connect(Selector selector) {
        BackendConnection c = null;
        while ((c = connectQueue.poll()) != null) {
            try {
                c.connect(selector);
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_CONNECT_SOCKET, e);
            }
        }
    }

    private void finishConnect(SelectionKey key, Object att) {
        BackendConnection c = (BackendConnection) att;
        try {
            if (c.finishConnect()) {
                clearSelectionKey(key);
                c.setId(ID_GENERATOR.getId());
                NIOProcessor processor = nextProcessor();
                c.setProcessor(processor);
                processor.postRegister(c);
            }
        } catch (Throwable e) {
            clearSelectionKey(key);
            c.handleError(ErrorCode.ERR_FINISH_CONNECT, e);
        }
    }

    private void clearSelectionKey(SelectionKey key) {
        if (key.isValid()) {
            key.attach(null);
            key.cancel();
        }
    }

    private NIOProcessor nextProcessor() {
        if (++nextProcessor == processors.length) {
            nextProcessor = 0;
        }
        return processors[nextProcessor];
    }

    /**
     * 后端连接ID生成器
     * 
     * @author xianmao.hexm
     */
    private static class ConnectIdGenerator {

        private static final long MAX_VALUE = Long.MAX_VALUE;

        private long              connectId = 0L;
        private final Object      lock      = new Object();

        private long getId() {
            synchronized (lock) {
                if (connectId >= MAX_VALUE) {
                    connectId = 0L;
                }
                return ++connectId;
            }
        }
    }

}
