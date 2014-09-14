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

import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;
import com.alibaba.cobar.manager.util.ListSortUtil;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AddCobarPage extends AbstractController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        UserDO user = (UserDO) request.getSession().getAttribute("user");
        long clusterId = Long.parseLong(request.getParameter("clusterId"));
        List<ClusterDO> cList;
        if (-1 == clusterId) {
            cList = this.xmlAccesser.getClusterDAO().listAllCluster();
        } else {
            ClusterDO cluster = xmlAccesser.getClusterDAO().getClusterById(clusterId);
            if (null == cluster) {
                logger.error("cluster doesn't exsit by id : " + clusterId);
            }
            cList = new ArrayList<ClusterDO>();
            cList.add(cluster);
        }

        ListSortUtil.sortClusterByName(cList);

        List<Map<String, Object>> clusterList = new ArrayList<Map<String, Object>>();
        for (ClusterDO e : cList) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("id", e.getId());
            map.put("name", CobarStringUtil.htmlEscapedString(e.getName()));
            clusterList.add(map);
        }

        return new ModelAndView("m_addCobar", new FluenceHashMap<String, Object>().putKeyValue("cList", clusterList)
            .putKeyValue("user", user));
    }
}
