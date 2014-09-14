package com.alibaba.cobar.manager.service;

import org.springframework.beans.factory.InitializingBean;

import com.alibaba.cobar.manager.dao.VipDAO;
import com.alibaba.cobar.manager.dao.xml.ClusterDAOImple;
import com.alibaba.cobar.manager.dao.xml.CobarDAOImple;
import com.alibaba.cobar.manager.dao.xml.PropertyDAOImple;
import com.alibaba.cobar.manager.dao.xml.UserDAOImple;

/**
 * @author haiqing.zhuhq 2011-6-15
 */
public class XmlAccesser implements InitializingBean {

    private ClusterDAOImple  clusterDAO;
    private CobarDAOImple    cobarDAO;
    private UserDAOImple     userDAO;
    private PropertyDAOImple propertyDAO;
    private VipDAO           vipMapDAO;

    public VipDAO getVipMapDAO() {
        return vipMapDAO;
    }

    public void setVipMapDAO(VipDAO vipMapDAO) {
        this.vipMapDAO = vipMapDAO;
    }

    public ClusterDAOImple getClusterDAO() {
        return clusterDAO;
    }

    public void setClusterDAO(ClusterDAOImple clusterDAO) {
        this.clusterDAO = clusterDAO;
    }

    public CobarDAOImple getCobarDAO() {
        return cobarDAO;
    }

    public void setCobarDAO(CobarDAOImple cobarDAO) {
        this.cobarDAO = cobarDAO;
    }

    public UserDAOImple getUserDAO() {
        return userDAO;
    }

    public void setUserDAO(UserDAOImple userDAO) {
        this.userDAO = userDAO;
    }

    public PropertyDAOImple getPropertyDAO() {
        return propertyDAO;
    }

    public void setPropertyDAO(PropertyDAOImple propertyDAO) {
        this.propertyDAO = propertyDAO;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (clusterDAO == null) {
            throw new IllegalArgumentException("property 'clusterDAO' is not set!");
        }
        if (cobarDAO == null) {
            throw new IllegalArgumentException("property 'cobarDAO' is not set!");
        }
        if (userDAO == null) {
            throw new IllegalArgumentException("property 'userDAO' is not set!");
        }
        if (propertyDAO == null) {
            throw new IllegalArgumentException("property 'propertyDAO' is not set!");
        }
        if (vipMapDAO == null) {
            throw new IllegalArgumentException("property 'vipMapDAO' is not set!");
        }
    }

}
