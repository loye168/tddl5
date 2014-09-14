package com.alibaba.cobar.server;

import java.nio.channels.SocketChannel;

import com.alibaba.cobar.CobarPrivileges;
import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.config.SystemConfig;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.factory.FrontendConnectionFactory;
import com.alibaba.cobar.server.session.ServerSession;

/**
 * @author xianmao.hexm
 */
public class ServerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        SystemConfig sys = CobarServer.getInstance().getConfig().getSystem();
        ServerConnection c = new ServerConnection(channel);
        c.setPrivileges(new CobarPrivileges());
        c.setQueryHandler(new ServerQueryHandler(c));
        c.setTxIsolation(sys.getTxIsolation());
        c.setSession(new ServerSession(c));
        return c;
    }

}
