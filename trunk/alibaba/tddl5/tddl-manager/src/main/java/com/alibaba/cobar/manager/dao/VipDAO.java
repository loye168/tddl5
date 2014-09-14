/**
 * (created at 2011-11-18)
 */
package com.alibaba.cobar.manager.dao;

import java.util.List;

import com.alibaba.cobar.manager.dataobject.xml.VipDO;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public interface VipDAO {

    /**
     * @return null for not contain
     */
    VipDO getVipBySid(String sid);

    /**
     * @return null for not contain
     */
    VipDO getVipById(long id);

    /**
     * @param vip never null
     * @return
     */
    boolean addVip(VipDO vip);

    /**
     * check sid is repeat
     * 
     * @param sid
     * @return true for not repeat, else for contained
     */
    boolean checkSid(String sid);

    /**
     * @param sid never null
     * @return false if sid is not contained
     */
    boolean deleteVip(String sid);

    /**
     * @param id never null
     * @return false if sid is not contained
     */
    boolean deleteVip(long id);

    List<VipDO> listAllVipDO();
}
