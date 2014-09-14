package com.alibaba.cobar.manager.dao;

import java.util.List;

import com.alibaba.cobar.manager.dataobject.xml.CobarDO;

/**
 * @author haiqing.zhuhq 2011-6-14
 */
public interface CobarDAO {

    public CobarDO getCobarById(long id);

    // public int getCobarCountByStatus(long clusterId, String status);

    public boolean checkName(String name, long clusterId);

    public boolean addCobar(CobarDO cobar);

    public List<CobarDO> getCobarList(long clusterId);

    public List<CobarDO> listCobarById(long[] cobarIds);

    public List<CobarDO> listAllCobar();

    public List<CobarDO> getCobarList(long clusterId, String status);

    public boolean modifyCobar(CobarDO cobar);

    public boolean checkName(String name, long clusterId, long cobarId);
}
