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
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;
import com.alibaba.cobar.manager.util.ListSortUtil;

/**
 * @author haiqing.zhuhq 2011-12-12
 */
public class Index extends AbstractController implements InitializingBean {

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
        List<ClusterDO> list = xmlAccesser.getClusterDAO().listAllCluster();
        List<Map<String, Object>> clusterList = new ArrayList<Map<String, Object>>();
        ListSortUtil.sortClusterBySortId(list);
        for (ClusterDO e : list) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("id", e.getId());
            map.put("name", CobarStringUtil.htmlEscapedString(e.getName()));
            map.put("maintContact", e.getMaintContact());
            map.put("onlineTime", e.getOnlineTime());

            clusterList.add(map);
        }

        String result = null;
        try {
            result = request.getParameter("result").trim();
        } catch (NullPointerException e) {
            result = "null";
        }
        if (result == null) {
            result = "null";
        }

        // remove attributes for login
        if (null != request.getSession(false)) {
            request.getSession().removeAttribute("click");
            request.getSession().removeAttribute("lastRequest");
        }

        return new ModelAndView("index", new FluenceHashMap<String, Object>().putKeyValue("clusterList", clusterList)
            .putKeyValue("result", result));
    }

}
