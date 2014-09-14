package com.alibaba.cobar.manager.service;

import org.springframework.beans.factory.InitializingBean;

import com.alibaba.cobar.manager.dao.CobarAdapterDAO;
import com.alibaba.cobar.manager.dao.delegate.AdapterDelegate;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author haiqing.zhuhq 2011-6-15
 */
public class CobarAccesser implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(CobarAccesser.class);
    private AdapterDelegate     cobarAdapterDelegate;
    private XmlAccesser         xmlAccesser;

    public AdapterDelegate getCobarAdapterDelegate() {
        return cobarAdapterDelegate;
    }

    public void setCobarAdapterDelegate(AdapterDelegate cobarAdapterDelegate) {
        this.cobarAdapterDelegate = cobarAdapterDelegate;
    }

    public XmlAccesser getXmlAccesser() {
        return xmlAccesser;
    }

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (cobarAdapterDelegate == null) {
            throw new IllegalArgumentException("property 'cobarAdapterDelegate' is null!");
        }
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    public CobarAdapterDAO getAccesser(long cobarId) {
        final CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(cobarId);
        if (cobar == null) {
            logger.error(new StringBuilder("Fail to get cobar information which id = ").append(cobarId).toString());
        }
        CobarAdapterDAO accesser = cobarAdapterDelegate.getCobarNodeAccesser(cobar.getHost(),
            cobar.getPort(),
            cobar.getUser(),
            cobar.getPassword());
        return accesser;
    }

    public CobarAdapterDAO getAccesser(CobarDO cobar) {
        CobarAdapterDAO accesser = cobarAdapterDelegate.getCobarNodeAccesser(cobar.getHost(),
            cobar.getPort(),
            cobar.getUser(),
            cobar.getPassword());
        return accesser;
    }
}
