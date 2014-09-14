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
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;

public class EditCobarPage extends AbstractController implements InitializingBean {

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

    @SuppressWarnings("unchecked")
    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        long cobarId = 0;
        try {
            cobarId = Long.parseLong(request.getParameter("cobarId").trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("parameter 'cobarId' is invalid:" + request.getParameter("cobarId"));
        }
        CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(cobarId);
        Map<String, Object> cobarMap = new PropertyUtilsBean().describe(cobar);
        cobarMap.remove("class");
        cobarMap.remove("name");
        cobarMap.put("name", CobarStringUtil.htmlEscapedString(cobar.getName()));

        List<ClusterDO> cList = xmlAccesser.getClusterDAO().listAllCluster();

        List<Map<String, Object>> clusterList = new ArrayList<Map<String, Object>>();
        for (ClusterDO e : cList) {
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("id", e.getId());
            map.put("name", CobarStringUtil.htmlEscapedString(e.getName()));
            clusterList.add(map);
        }

        return new ModelAndView("m_editCobar", new FluenceHashMap<String, Object>().putKeyValue("cluList", clusterList)
            .putKeyValue("cobar", cobarMap));
    }

}
