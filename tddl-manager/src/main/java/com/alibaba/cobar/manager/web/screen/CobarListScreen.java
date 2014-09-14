package com.alibaba.cobar.manager.web.screen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import com.alibaba.cobar.manager.util.ConstantDefine;
import com.alibaba.cobar.manager.util.FluenceHashMap;
import com.alibaba.cobar.manager.util.ListSortUtil;

/**
 * @author haiqing.zhuhq 2011-9-1
 */
public class CobarListScreen extends AbstractController implements InitializingBean {

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

    @SuppressWarnings({ "unchecked" })
    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        UserDO user = (UserDO) request.getSession().getAttribute("user");
        long clusterId = Long.parseLong(request.getParameter("clusterId"));
        ClusterDO cluster = xmlAccesser.getClusterDAO().getClusterById(clusterId);
        List<CobarDO> cobarList = xmlAccesser.getCobarDAO().getCobarList(clusterId);

        ListSortUtil.sortCobarByName(cobarList);

        // int aCount =
        // xmlAccesser.getCobarDAO().getCobarCountByStatus(clusterId,
        // ConstantDefine.ACTIVE);
        // int iCount =
        // xmlAccesser.getCobarDAO().getCobarCountByStatus(clusterId,
        // ConstantDefine.IN_ACTIVE);
        int aCount = xmlAccesser.getCobarDAO().getCobarList(clusterId, ConstantDefine.ACTIVE).size();
        int iCount = xmlAccesser.getCobarDAO().getCobarList(clusterId, ConstantDefine.IN_ACTIVE).size();
        Map<String, Integer> count = new HashMap<String, Integer>();
        count.put("aCount", aCount);
        count.put("iCount", iCount);
        count.put("tCount", (aCount + iCount));

        PropertyUtilsBean util = new PropertyUtilsBean();
        Map<String, Object> clusterMap;
        try {
            clusterMap = util.describe(cluster);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        clusterMap.remove("class");
        clusterMap.remove("name");
        clusterMap.remove("deployDesc");

        clusterMap.put("name", CobarStringUtil.htmlEscapedString(cluster.getName()));
        clusterMap.put("deployDesc", CobarStringUtil.htmlEscapedString(cluster.getDeployDesc()));

        List<Map<String, Object>> cobarListMap = new ArrayList<Map<String, Object>>();

        for (CobarDO c : cobarList) {
            Map<String, Object> cobarMap;
            try {
                cobarMap = util.describe(c);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            cobarMap.remove("class");
            cobarMap.remove("name");
            cobarMap.put("name", CobarStringUtil.htmlEscapedString(c.getName()));

            /*
             * if (c.getStatus().equals(ConstantDefine.ACTIVE)) {
             * CobarAdapterDAO perf = cobarAccesser.getAccesser(c); if
             * (!perf.checkConnection()) { cobarMap.remove("status");
             * cobarMap.put("status", ConstantDefine.ERROR); } }
             */
            cobarListMap.add(cobarMap);
        }

        return new ModelAndView("v_cobarList", new FluenceHashMap<String, Object>().putKeyValue("cluster", clusterMap)
            .putKeyValue("cobarList", cobarListMap)
            .putKeyValue("count", count)
            .putKeyValue("user", user));
    }

}
