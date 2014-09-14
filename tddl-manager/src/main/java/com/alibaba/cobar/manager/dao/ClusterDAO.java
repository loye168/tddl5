package com.alibaba.cobar.manager.dao;

import java.util.List;

import com.alibaba.cobar.manager.dataobject.xml.AppDO;
import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;

/**
 * @author haiqing.zhuhq 2011-6-14
 */
public interface ClusterDAO {

    public List<ClusterDO> listAllCluster();

    public List<AppDO> listAllApp(ClusterDO cluster);

    public ClusterDO getClusterById(long id);

    public boolean modifyCluster(ClusterDO cluster);

    public boolean checkName(String name);

    public boolean checkName(String name, long id);

    public boolean addCluster(ClusterDO cluster);

    public String addApp(AppDO app);
}
