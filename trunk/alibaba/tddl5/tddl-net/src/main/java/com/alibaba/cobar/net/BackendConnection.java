package com.alibaba.cobar.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.handler.NIOHandler;
import com.alibaba.cobar.net.util.TimeUtil;

/**
 * @author xianmao.hexm
 */
public abstract class BackendConnection extends AbstractConnection {

    protected long         id;
    protected String       host;
    protected int          port;
    protected int          localPort;
    protected long         idleTimeout;
    protected NIOConnector connector;
    protected NIOHandler   handler;
    protected boolean      isFinishConnect;

    public BackendConnection(SocketChannel channel){
        super(channel);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public boolean isIdleTimeout() {
        return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
    }

    public void setConnector(NIOConnector connector) {
        this.connector = connector;
    }

    public void connect(Selector selector) throws IOException {
        channel.register(selector, SelectionKey.OP_CONNECT, this);
        channel.connect(new InetSocketAddress(host, port));
    }

    public boolean finishConnect() throws IOException {
        if (channel.isConnectionPending()) {
            channel.finishConnect();
            localPort = channel.socket().getLocalPort();
            isFinishConnect = true;
            return true;
        } else {
            return false;
        }
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
        this.readBuffer = processor.getBufferPool().allocate();
        processor.addBackend(this);
    }

    public void setHandler(NIOHandler handler) {
        this.handler = handler;
    }

    @Override
    public void handleData(byte[] data) {
        try {
            handler.handle(data);
        } catch (Throwable e) {
            handleError(ErrorCode.ERR_HANDLE_DATA, e);
        }
    }

    @Override
    protected void idleCheck() {
        // nothing
    }

}
