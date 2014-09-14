package com.alibaba.cobar.server;

import com.alibaba.cobar.net.handler.QueryHandler;
import com.alibaba.cobar.server.handler.BeginHandler;
import com.alibaba.cobar.server.handler.LoadHandler;
import com.alibaba.cobar.server.handler.SavepointHandler;
import com.alibaba.cobar.server.handler.SelectHandler;
import com.alibaba.cobar.server.handler.SetHandler;
import com.alibaba.cobar.server.handler.ShowHandler;
import com.alibaba.cobar.server.handler.StartHandler;
import com.alibaba.cobar.server.handler.UseHandler;
import com.alibaba.cobar.server.parser.ServerParse;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public class ServerQueryHandler implements QueryHandler {

    private static final Logger    logger = LoggerFactory.getLogger(ServerQueryHandler.class);
    private final ServerConnection source;

    public ServerQueryHandler(ServerConnection source){
        this.source = source;
    }

    @Override
    public void query(String sql) {
        ServerConnection c = this.source;
        if (logger.isInfoEnabled()) {
            logger.info(sql);
        }
        int rs = ServerParse.parse(sql);
        switch (rs & 0xff) {
            case ServerParse.SET:
                SetHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.START:
                StartHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.BEGIN:
                BeginHandler.handle(sql, c);
                break;
            case ServerParse.LOAD:
                LoadHandler.handle(sql, c);
                break;
            case ServerParse.SAVEPOINT:
                SavepointHandler.handle(sql, c);
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.COMMIT:
                c.commit();
                break;
            case ServerParse.ROLLBACK:
                c.rollback();
                break;
            default:
                c.execute(sql, rs);
        }
    }

}
