package com.alibaba.cobar.manager;

import com.alibaba.cobar.ErrorCode;
import com.alibaba.cobar.manager.handler.ClearHandler;
import com.alibaba.cobar.manager.handler.KillQueryHandler;
import com.alibaba.cobar.manager.handler.SelectHandler;
import com.alibaba.cobar.manager.handler.ShowHandler;
import com.alibaba.cobar.manager.parser.ManagerParse;
import com.alibaba.cobar.manager.response.KillConnection;
import com.alibaba.cobar.manager.response.Offline;
import com.alibaba.cobar.manager.response.Online;
import com.alibaba.cobar.net.handler.QueryHandler;
import com.alibaba.cobar.net.packet.OkPacket;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public class ManagerQueryHandler implements QueryHandler {

    private static final Logger     logger = LoggerFactory.getLogger(ManagerQueryHandler.class);
    private final ManagerConnection source;

    public ManagerQueryHandler(ManagerConnection source){
        this.source = source;
    }

    @Override
    public void query(String sql) {
        ManagerConnection c = this.source;
        if (logger.isDebugEnabled()) {
            logger.debug(new StringBuilder().append(sql).toString());
        }
        int rs = ManagerParse.parse(sql);
        switch (rs & 0xff) {
            case ManagerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.SET:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ManagerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.KILL_CONN:
                KillConnection.response(sql, rs >>> 8, c);
                break;
            case ManagerParse.OFFLINE:
                Offline.execute(sql, c);
                break;
            case ManagerParse.ONLINE:
                Online.execute(sql, c);
                break;
            case ManagerParse.CLEAR:
                ClearHandler.handle(sql, c, rs >>> 8);
                break;
            case ManagerParse.KILL_QUERY:
                KillQueryHandler.handle(sql, rs >>> 8, c);
                break;
            default:
                c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
        }
    }
}
