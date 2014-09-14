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
 * 网络事件反应器
 * 
 * @author xianmao.hexm
 */
public final class NIOReactor {

    private static final Logger logger = LoggerFactory.getLogger(NIOReactor.class);
    private final String        name;
    private final R             reactorR;
    private final W             reactorW;

    public NIOReactor(String name) throws IOException{
        this.name = name;
        this.reactorR = new R();
        this.reactorW = new W();
    }

    final void startup() {
        new Thread(reactorR, name + "-R").start();
        new Thread(reactorW, name + "-W").start();
    }

    final void postRegister(NIOConnection c) {
        reactorR.registerQueue.offer(c);
        reactorR.selector.wakeup();
    }

    final BlockingQueue<NIOConnection> getRegisterQueue() {
        return reactorR.registerQueue;
    }

    final long getReactCount() {
        return reactorR.reactCount;
    }

    final void postWrite(NIOConnection c) {
        reactorW.writeQueue.offer(c);
    }

    final BlockingQueue<NIOConnection> getWriteQueue() {
        return reactorW.writeQueue;
    }

    private final class R implements Runnable {

        private final Selector                     selector;
        private final BlockingQueue<NIOConnection> registerQueue;
        private long                               reactCount;

        private R() throws IOException{
            this.selector = Selector.open();
            this.registerQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        @Override
        public void run() {
            final Selector selector = this.selector;
            for (;;) {
                ++reactCount;
                try {
                    selector.select(1000L);
                    register(selector);
                    Set<SelectionKey> keys = selector.selectedKeys();
                    try {
                        for (SelectionKey key : keys) {
                            Object att = key.attachment();
                            if (att != null && key.isValid()) {
                                int readyOps = key.readyOps();
                                if ((readyOps & SelectionKey.OP_READ) != 0) {
                                    read((NIOConnection) att);
                                } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                                    write((NIOConnection) att);
                                } else {
                                    key.cancel();
                                }
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

        private void register(Selector selector) {
            NIOConnection c = null;
            while ((c = registerQueue.poll()) != null) {
                try {
                    c.register(selector);
                } catch (Throwable e) {
                    c.handleError(ErrorCode.ERR_REGISTER, e);
                }
            }
        }

        private void read(NIOConnection c) {
            try {
                c.read();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_READ, e);
            }
        }

        private void write(NIOConnection c) {
            try {
                c.writeByEvent();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_WRITE_BY_EVENT, e);
            }
        }
    }

    private final class W implements Runnable {

        private final BlockingQueue<NIOConnection> writeQueue;

        private W(){
            this.writeQueue = new LinkedBlockingQueue<NIOConnection>();
        }

        @Override
        public void run() {
            NIOConnection c = null;
            for (;;) {
                try {
                    if ((c = writeQueue.take()) != null) {
                        write(c);
                    }
                } catch (Throwable e) {
                    logger.warn(name, e);
                }
            }
        }

        private void write(NIOConnection c) {
            try {
                c.writeByQueue();
            } catch (Throwable e) {
                c.handleError(ErrorCode.ERR_WRITE_BY_QUEUE, e);
            }
        }
    }

}
