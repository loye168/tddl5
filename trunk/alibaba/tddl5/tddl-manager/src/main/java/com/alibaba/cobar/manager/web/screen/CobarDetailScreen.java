package com.alibaba.cobar.manager.web.screen;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.beanutils.PropertyUtilsBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-9-1
 */
public class CobarDetailScreen extends AbstractController implements InitializingBean {

    private XmlAccesser   xmlAccesser;
    private CobarAccesser cobarAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    public void setCobarAccesser(CobarAccesser cobarAccesser) {
        this.cobarAccesser = cobarAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null == xmlAccesser) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
        if (null == cobarAccesser) {
            throw new IllegalArgumentException("property 'cobarAccesser' is null!");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response) {
        UserDO user = (UserDO) request.getSession().getAttribute("user");
        long nodeId = 0;
        try {
            nodeId = Long.parseLong(request.getParameter("nodeId").trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("parameter 'nodeId' is invalid: " + request.getParameter("nodeId"));
        }
        CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(nodeId);
        if (null == cobar) {
            throw new IllegalArgumentException("no cobar exsit for id : " + nodeId);
        }

        PropertyUtilsBean util = new PropertyUtilsBean();
        Map<String, Object> cobarMap = null;
        try {
            cobarMap = util.describe(cobar);
        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }

        cobarMap.remove("class");
        cobarMap.remove("name");
        cobarMap.put("name", CobarStringUtil.htmlEscapedString(cobar.getName()));

        ClusterDO cluster = xmlAccesser.getClusterDAO().getClusterById(cobar.getClusterId());
        Map<String, Object> clusterMap = new HashMap<String, Object>();
        clusterMap.put("id", cluster.getId());
        clusterMap.put("name", CobarStringUtil.htmlEscapedString(cluster.getName()));

        /*
         * CobarAdapterDAO perfAccesser = cobarAccesser.getAccesser(nodeId);
         * String startTime = null; String version = "UNKNOWN"; String status =
         * "RUNNING"; if (perfAccesser.checkConnection()) { status =
         * perfAccesser.getServerStatus().getStatus(); startTime =
         * perfAccesser.getStartUpTime().getFormatTime(); version =
         * FormatUtil.formatVersion(perfAccesser.getVersion()); } else { status
         * = "ERROR"; }
         */
        return new ModelAndView("v_cobarDetail", new FluenceHashMap<String, Object>().putKeyValue("user", user)
            .putKeyValue("cluster", clusterMap)
            .putKeyValue("cobarNode", cobarMap));
    }
}
