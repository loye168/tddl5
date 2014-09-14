package com.alibaba.cobar.server.handler;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.Isolations;
import com.alibaba.cobar.net.packet.OkPacket;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.parser.ServerParse;
import com.alibaba.cobar.server.parser.ServerParseSet;
import com.alibaba.cobar.server.response.CharacterSet;
import com.alibaba.cobar.server.response.SqlMode;
import com.taobao.tddl.common.jdbc.ITransactionPolicy;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

import static com.alibaba.cobar.server.parser.ServerParseSet.*;

/**
 * SET 语句处理
 * 
 * @author xianmao.hexm
 */
public final class SetHandler {

    private static final Logger logger = LoggerFactory.getLogger(SetHandler.class);
    private static final byte[] AC_OFF = new byte[] { 7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0 };

    public static void handle(String stmt, ServerConnection c, int offset) {
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
            case AUTOCOMMIT_ON:
                if (!c.isAutocommit()) {
                    c.setAutocommit(true);
                }

                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case AUTOCOMMIT_OFF: {
                if (c.isAutocommit()) {
                    c.setAutocommit(false);
                }
                c.write(c.writeToBuffer(AC_OFF, c.allocate()));
                break;
            }
            case TX_READ_UNCOMMITTED: {
                c.setTxIsolation(Isolations.READ_UNCOMMITTED.getCode());
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_READ_COMMITTED: {
                c.setTxIsolation(Isolations.READ_COMMITTED.getCode());
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_REPEATED_READ: {
                c.setTxIsolation(Isolations.REPEATED_READ.getCode());
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case TX_SERIALIZABLE: {
                c.setTxIsolation(Isolations.SERIALIZABLE.getCode());
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            }
            case NAMES:
                String charset = stmt.substring(rs >>> 8).trim();
                if (c.setCharset(charset)) {
                    c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                } else {
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                }
                break;
            case CHARACTER_SET_CLIENT:
            case CHARACTER_SET_CONNECTION:
            case CHARACTER_SET_RESULTS:
                CharacterSet.response(stmt, c, rs);
                break;
            case SQL_MODE:
                SqlMode.response(stmt, c, rs);
                break;
            case TX_POLICY_1:
                c.setTrxPolicy(ITransactionPolicy.TDDL);
                break;
            // case TX_POLICY_2:
            // c.setTrxPolicy(new Cobar());
            // c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            // break;
            case TX_POLICY_3:
                c.setTrxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
                break;
            // case TX_POLICY_4:
            // c.setTrxPolicy(new Corona());
            // c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            // break;
            case TX_POLICY_5:
                c.setTrxPolicy(ITransactionPolicy.FREE);
                break;
            case AT_VAR:
                c.execute(stmt, ServerParse.SET);
            default:
                StringBuilder s = new StringBuilder();
                logger.warn(s.append(stmt).append(" is not executed").toString());
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        }
    }
}
