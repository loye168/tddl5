package com.alibaba.cobar.manager.qa.sysadmin;

import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.util.ConstantDefine;

/**
 * @author xiaowen.guoxw
 * @version ???????2011-6-27 ????10:46:05
 */

public class DOFactory {

    public static ClusterDO getCluster() {
        ClusterDO cluster = new ClusterDO();
        cluster.setDeployContact("deployContact");
        cluster.setDeployDesc("deployDesc");
        cluster.setMaintContact("13456789123");
        cluster.setName("name");
        cluster.setOnlineTime("OnlineTime");
        cluster.setEnv("test");
        return cluster;
    }

    public static CobarDO getCobar() {
        CobarDO cobar = new CobarDO();
        cobar.setClusterId(1);
        cobar.setHost("1.1.1.1");
        cobar.setName("gxw");
        cobar.setPassword("gxw");
        cobar.setStatus(ConstantDefine.ACTIVE);
        cobar.setPort(9090);
        cobar.setTime_diff("time_diff");
        cobar.setUser("gxw");
        return cobar;
    }

    public static UserDO getUser() {
        UserDO user = new UserDO();
        user.setPassword("gxw");
        user.setRealname("gxw");
        user.setStatus(ConstantDefine.ACTIVE);
        user.setUser_role(ConstantDefine.SYSTEM_ADMIN);
        user.setUsername("wenjun");
        return user;
    }

}
