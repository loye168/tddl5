package com.alibaba.cobar.manager;

import java.nio.channels.SocketChannel;

import com.alibaba.cobar.CobarPrivileges;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.factory.FrontendConnectionFactory;

/**
 * @author xianmao.hexm
 */
public class ManagerConnectionFactory extends FrontendConnectionFactory {

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        ManagerConnection c = new ManagerConnection(channel);
        c.setPrivileges(new CobarPrivileges());
        c.setQueryHandler(new ManagerQueryHandler(c));
        return c;
    }

}
