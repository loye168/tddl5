package com.alibaba.cobar.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;

import com.alibaba.cobar.Capabilities;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.Versions;
import com.alibaba.cobar.net.handler.FrontendAuthenticator;
import com.alibaba.cobar.net.handler.NIOHandler;
import com.alibaba.cobar.net.handler.Privileges;
import com.alibaba.cobar.net.handler.QueryHandler;
import com.alibaba.cobar.net.packet.ErrorPacket;
import com.alibaba.cobar.net.packet.HandshakePacket;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.net.util.CharsetUtil;
import com.alibaba.cobar.net.util.ExecutorUtil;
import com.alibaba.cobar.net.util.MySQLMessage;
import com.alibaba.cobar.net.util.NameableExecutor;
import com.alibaba.cobar.net.util.RandomUtil;
import com.alibaba.cobar.net.util.TimeUtil;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.common.utils.logger.MDC;

/**
 * @author xianmao.hexm
 */
public abstract class FrontendConnection extends AbstractConnection {

    private static final Logger logger            = LoggerFactory.getLogger(FrontendConnection.class);
    private static String       serverVersion     = Versions.SERVER_VERSION;

    protected long              id;
    protected String            host;
    protected int               port;
    protected int               localPort;
    protected long              idleTimeout;
    protected String            charset;
    protected int               charsetIndex;
    protected byte[]            seed;
    protected String            user;
    protected String            schema;
    protected NIOHandler        handler;
    protected Privileges        privileges;
    protected QueryHandler      queryHandler;
    protected boolean           isAccepted;
    protected boolean           isAuthenticated;
    protected NameableExecutor  writeFileToInputStreamExecutor;
    protected boolean           loadFile;
    protected String            loadDataSql;
    protected InputStream       fileInputStream;
    protected final int         loadFileQueueSize = 2;

    public void setLoadFile(boolean b) {
        this.loadFile = b;

    }

    public void setLoadDataSql(String sql) {
        this.loadDataSql = sql;

    }

    public byte getNewPacketId() {
        return ++packetId;
    }

    public void setPacketId(byte packetId) {
        this.packetId = packetId;
    }

    public static String getServerVersion() {
        return serverVersion;
    }

    public static void setServerVersion(String serverVersion) {
        FrontendConnection.serverVersion = serverVersion;
    }

    public FrontendConnection(SocketChannel channel){
        super(channel);
        Socket socket = channel.socket();
        this.host = socket.getInetAddress().getHostAddress();
        this.port = socket.getPort();
        this.localPort = socket.getLocalPort();
        this.handler = new FrontendAuthenticator(this);
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

    public void setAccepted(boolean isAccepted) {
        this.isAccepted = isAccepted;
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
        this.readBuffer = processor.getBufferPool().allocate();
        processor.addFrontend(this);
    }

    public void setHandler(NIOHandler handler) {
        this.handler = handler;
    }

    public void setQueryHandler(QueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public void setAuthenticated(boolean isAuthenticated) {
        this.isAuthenticated = isAuthenticated;
    }

    public Privileges getPrivileges() {
        return privileges;
    }

    public void setPrivileges(Privileges privileges) {
        this.privileges = privileges;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public byte[] getSeed() {
        return seed;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.charset = charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public String getCharset() {
        return charset;
    }

    public boolean setCharset(String charset) {
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.charset = charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public void writeErrMessage(int errno, String msg) {
        writeErrMessage(this.getNewPacketId(), errno, msg);
    }

    public void writeErrMessage(byte id, int errno, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = id;
        err.errno = errno;
        err.message = encodeString(msg, charset);
        err.write(this);
    }

    // commands --------------------------------------------------------------
    public void initDB(byte[] data) {
        buildMDC();
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String db = mm.readString();

        // 检查schema是否已经设置
        if (schema != null) {
            if (schema.equals(db)) {
                write(writeToBuffer(OkPacket.OK, allocate()));
            } else {
                writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, "Not allowed to change the database!");
            }
            return;
        }

        // 检查schema的有效性
        if (db == null || !privileges.schemaExists(db)) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }
        if (!privileges.userExists(user, host)) {
            writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "'");
            return;
        }
        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(db)) {
            this.schema = db;
            write(writeToBuffer(OkPacket.OK, allocate()));
        } else {
            String s = "Access denied for user '" + user + "' to database '" + db + "'";
            writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
        }
    }

    public void query(byte[] data) {
        buildMDC();
        // 取得查询语句
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String sql = null;
        try {
            sql = mm.readString(CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            return;
        }
        if (sql == null || sql.length() == 0) {
            writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }

        // 执行查询
        if (queryHandler != null) {
            queryHandler.query(sql);
        } else {
            writeErrMessage(ErrorCode.ER_YES, "Empty QueryHandler");
        }
    }

    public void ping() {
        write(writeToBuffer(OkPacket.OK, allocate()));
    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtPrepare(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtExecute(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtClose(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void unknown(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    @Override
    protected void idleCheck() {
        if (isIdleTimeout()) {
            logger.warn("idle timeout");
            close();
        }
    }

    @Override
    public void register(Selector selector) throws IOException {
        super.register(selector);
        if (!isClosed.get()) {
            // 生成认证数据
            byte[] rand1 = RandomUtil.randomBytes(8);
            byte[] rand2 = RandomUtil.randomBytes(12);

            // 保存认证数据
            byte[] seed = new byte[rand1.length + rand2.length];
            System.arraycopy(rand1, 0, seed, 0, rand1.length);
            System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
            this.seed = seed;

            // 发送握手数据包
            HandshakePacket hs = new HandshakePacket();
            hs.packetId = 0;
            hs.protocolVersion = Versions.PROTOCOL_VERSION;
            hs.serverVersion = serverVersion.getBytes();
            hs.threadId = id;
            hs.seed = rand1;
            hs.serverCapabilities = getServerCapabilities();
            hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
            hs.serverStatus = 2;
            hs.restOfScrambleBuff = rand2;
            hs.write(this);
        }
    }

    public void handleFile(final byte[] data) {
        if (this.fileInputStream == null) {

            this.outPutPipeStream = new PipedOutputStream();
            try {
                this.fileInputStream = new PipedInputStream(outPutPipeStream);
            } catch (IOException e) {
                logger.error(" this.fileInputStream = new PipedInputStream(outPutPipeStream) error", e);
            }

            prepareLoadInfile();
        }

        if (this.writeFileToInputStreamExecutor == null) {
            this.writeFileToInputStreamExecutor = ExecutorUtil.create("LOCAL_INFILE_THREAD", 1, true);
        }

        /**
         * 对队列的大小做下限制，防止爆内存
         */
        if (writeFileToInputStreamExecutor.getQueue().size() > 2) {
            canReadNewPacket = false;
            logger.warn("start load data flow control, queue size is "
                        + writeFileToInputStreamExecutor.getQueue().size());
        }

        this.writeFileToInputStreamExecutor.execute(new Runnable() {

            @Override
            public void run() {
                try {

                    // empty packet
                    if (data.length == 4) {
                        outPutPipeStream.close();
                        logger.warn("end load data");

                        return;
                    }
                    outPutPipeStream.write(data, 4, data.length - 4);
                } catch (Throwable e) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, e);
                } finally {
                    if (writeFileToInputStreamExecutor.getQueue().size() <= 2) {
                        canReadNewPacket = true;
                        logger.warn("end load data flow control, queue size is "
                                    + writeFileToInputStreamExecutor.getQueue().size());

                    }
                }
            }
        });
        return;
    }

    @Override
    public void handleData(final byte[] data) {
        if (data.length < 4) {
            throw new IllegalAccessError("impossible packet length, packet:" + data);
        }

        this.setPacketId(data[3]);

        if (this.loadFile) {
            handleFile(data);
            return;

        }

        processor.getHandler().execute(new Runnable() {

            @Override
            public void run() {
                try {
                    handler.handle(data);
                } catch (Throwable e) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, e);
                }
            }
        });
    }

    protected abstract void prepareLoadInfile();

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        return flag;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("[host=")
            .append(host)
            .append(",port=")
            .append(port)
            .append(",schema=")
            .append(schema)
            .append(']')
            .toString();
    }

    protected void buildMDC() {
        StringBuilder builder = new StringBuilder();
        builder.append("host=").append(host).append(",port=").append(port).append(",schema=").append(schema);
        if (schema != null) {
            MDC.put("app", schema); // 设置schema上下文
        }
        MDC.put("CONNECTION", builder.toString());
    }

    private final static byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }

        charset = CharsetUtil.getJavaCharset(charset);
        if (charset == null) {
            return src.getBytes();
        }

        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    protected boolean isConnectionReset(Throwable t) {
        if (t instanceof IOException) {
            String msg = t.getMessage();
            return (msg != null && msg.contains("Connection reset by peer"));
        }
        return false;
    }

}
