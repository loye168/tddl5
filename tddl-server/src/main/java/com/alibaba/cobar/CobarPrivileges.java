package com.alibaba.cobar;

import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.config.UserConfig;
import com.alibaba.cobar.net.handler.Privileges;

import com.alibaba.cobar.config.IUserHostDefination;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author xianmao.hexm
 */
public class CobarPrivileges implements Privileges {

    private static final Logger alarm = LoggerFactory.getLogger("alarm");

    @Override
    public boolean schemaExists(String schema) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        return conf.getSchemas().containsKey(schema);
    }

    @Override
    public boolean userExists(String user, String host) {
        /**
         * Always check cluster level blacklist firstly.
         */
        CobarConfig conf = CobarServer.getInstance().getConfig();

        if (conf.getClusterQuarantine() != null &&
                conf.getClusterQuarantine().getClusterBlacklist() != null) {
            if (conf.getClusterQuarantine().getClusterBlacklist().containsOf(host)) {
                return false;
            }
        }

        /**
         * No cluster level black list then check app level whitelist.
         */
        Map<String, IUserHostDefination> quarantineHosts = conf.getQuarantine().getHosts();
        /* Unlike original pattern, use appName/user as key to do search */
        if (quarantineHosts.containsKey(user)) {
            IUserHostDefination hosts = quarantineHosts.get(user);
            if (!hosts.isEmpty())
            {
                boolean rs = hosts.containsOf(host);
                if (!rs) {
                    alarm.error(new StringBuilder().append(CobarAlarms.QUARANTINE_ATTACK)
                            .append("[host=")
                            .append(host)
                            .append(",user=")
                            .append(user)
                            .append(']')
                            .toString());
                }
                return rs;
            }
            /* here should take care of if hosts is empty then to do global search as well */
        }

        /* here quarantineHosts no such user or user related hosts is empty, then search global */
        return conf.getUsers().containsKey(user);
    }

    @Override
    public String getPassword(String user) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getPassword();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getSchemas();
        } else {
            return null;
        }
    }

    @Override
    public boolean IsTrustedIp(String host) {
        CobarConfig conf = CobarServer.getInstance().getConfig();

        if (conf.getClusterQuarantine() != null &&
                conf.getClusterQuarantine().getTrustedIps() != null) {
            if (conf.getClusterQuarantine().getTrustedIps().containsOf(host)) {
                return true;
            }
        }

        return false;
    }

}
