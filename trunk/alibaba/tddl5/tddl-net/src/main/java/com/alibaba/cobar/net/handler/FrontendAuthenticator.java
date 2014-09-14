package com.alibaba.cobar.net.handler;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import com.alibaba.cobar.Commands;
import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.packet.AuthPacket;
import com.alibaba.cobar.net.packet.QuitPacket;
import com.alibaba.cobar.net.util.SecurityUtil;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 前端认证处理器
 * 
 * @author xianmao.hexm
 */
public class FrontendAuthenticator implements NIOHandler {

    private static final Logger        logger  = LoggerFactory.getLogger(FrontendAuthenticator.class);
    private static final byte[]        AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

    protected final FrontendConnection source;

    public FrontendAuthenticator(FrontendConnection source){
        this.source = source;
    }

    @Override
    public void handle(byte[] data) {
        // check quit packet
        if (data.length == QuitPacket.QUIT.length && data[4] == Commands.COM_QUIT) {
            source.close();
            return;
        }

        AuthPacket auth = new AuthPacket();
        auth.read(data);

        // check user
        if (!checkUser(auth.user, source.getHost())) {
            failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'");
            return;
        }

        // check if host is in trusted ip, otherwise check password
        if (!IsTrustedIp(source.getHost())) {
            // check password
            if (!checkPassword(auth.password, auth.user)) {
                failure(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'");
                return;
            }
        }

        // set schema added by leiwen.zh
        // JDBC and non-java driver may send some sqls after authentication
        // succeed, for non-java driver, this action may cause failure because
        // of null schema
        setSchema(auth);

        // check schema
        switch (checkSchema(auth.database, auth.user)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                failure(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + auth.database + "'");
                break;
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                String s = "Access denied for user '" + auth.user + "' to database '" + auth.database + "'";
                failure(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                break;
            default:
                success(auth);
        }
    }

    private void setSchema(AuthPacket auth) {
        String user = auth.user;
        if (user != null) {
            Set<String> schemas = source.getPrivileges().getUserSchemas(user);
            if (schemas != null && schemas.size() == 1 && auth.database == null) {
                auth.database = schemas.iterator().next();
            }
        }
    }

    protected boolean checkUser(String user, String host) {
        return source.getPrivileges().userExists(user, host);
    }

    protected boolean IsTrustedIp(String host) {
        return source.getPrivileges().IsTrustedIp(host);
    }

    protected boolean checkPassword(byte[] password, String user) {
        String pass = source.getPrivileges().getPassword(user);

        // check null
        if (pass == null || pass.length() == 0) {
            if (password == null || password.length == 0) {
                return true;
            } else {
                return false;
            }
        }
        if (password == null || password.length == 0) {
            return false;
        }

        // encrypt
        byte[] encryptPass = null;
        try {
            encryptPass = SecurityUtil.scramble411(pass.getBytes(), source.getSeed());
        } catch (NoSuchAlgorithmException e) {
            logger.warn(e);
            return false;
        }
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
        } else {
            return false;
        }

        return true;
    }

    protected int checkSchema(String schema, String user) {
        if (schema == null) {
            return 0;
        }
        Privileges privileges = source.getPrivileges();
        if (!privileges.schemaExists(schema)) {
            return ErrorCode.ER_BAD_DB_ERROR;
        }
        Set<String> schemas = privileges.getUserSchemas(user);
        if (schemas == null || schemas.size() == 0 || schemas.contains(schema)) {
            return 0;
        } else {
            return ErrorCode.ER_DBACCESS_DENIED_ERROR;
        }
    }

    protected void success(AuthPacket auth) {
        source.setAuthenticated(true);
        source.setUser(auth.user);
        source.setSchema(auth.database);
        source.setCharsetIndex(auth.charsetIndex);
        source.setHandler(new FrontendCommandHandler(source));
        if (logger.isInfoEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(source).append('\'').append(auth.user).append("' login success");
            byte[] extra = auth.extra;
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            logger.info(s.toString());
        }
        ByteBuffer buffer = source.allocate();
        source.write(source.writeToBuffer(AUTH_OK, buffer));
    }

    protected void failure(int errno, String info) {
        logger.error(info);
        source.writeErrMessage((byte) 2, errno, info);
    }

}
