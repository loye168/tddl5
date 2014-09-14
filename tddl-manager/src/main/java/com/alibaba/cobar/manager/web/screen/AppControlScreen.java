package com.alibaba.cobar.manager.web.screen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dataobject.xml.AppDO;
import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;
import com.alibaba.cobar.manager.util.ListSortUtil;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AppControlScreen extends AbstractController implements InitializingBean {

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
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
        if (null == cobarAccesser) {
            throw new IllegalArgumentException("property 'cobarAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        UserDO user = (UserDO) request.getSession().getAttribute("user");
        String id = request.getParameter("clusterId");
        long clusterId = -1;
        if (null != id) {
            clusterId = Long.parseLong(id);
        }

        List<ClusterDO> cList = xmlAccesser.getClusterDAO().listAllCluster();
        List<Map<String, Object>> clusterList = new ArrayList<Map<String, Object>>();

        ListSortUtil.sortClusterByName(cList);
        for (ClusterDO e : cList) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("id", e.getId());
            map.put("name", CobarStringUtil.htmlEscapedString(e.getName()));
            clusterList.add(map);
        }
        List<AppDO> appList = null;
        if (clusterId > 0) {
            ClusterDO cluster = xmlAccesser.getClusterDAO().getClusterById(clusterId);

            if (cluster != null) {
                appList = xmlAccesser.getClusterDAO().listAllApp(cluster);
            }
        }

        return new ModelAndView("c_app", new FluenceHashMap<String, Object>().putKeyValue("cList", clusterList)
            .putKeyValue("clusterId", clusterId)
            .putKeyValue("user", user)
            .putKeyValue("apps", appList));

    }
}
