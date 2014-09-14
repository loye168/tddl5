package com.alibaba.cobar.manager.web.screen;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.beanutils.PropertyUtilsBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.dataobject.xml.VipDO;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class MVipListScreen extends AbstractController implements InitializingBean {

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
        UserDO user = (UserDO) request.getSession().getAttribute("user");

        String id = request.getParameter("vipId");
        long vipId = -1;
        if (null != id) {
            vipId = Long.parseLong(id);
        }

        List<VipDO> list = xmlAccesser.getVipMapDAO().listAllVipDO();
        PropertyUtilsBean util = new PropertyUtilsBean();
        List<Map<String, Object>> vipList = new ArrayList<Map<String, Object>>();
        List<Map<String, Object>> cobarList = new ArrayList<Map<String, Object>>();
        if (null != list && list.size() > 0) {
            if (-1 == vipId) {
                vipId = list.get(0).getId();
            }

            for (VipDO vip : list) {
                Map<String, Object> map;
                try {
                    map = util.describe(vip);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                map.remove("class");
                map.remove("cobarIds");
                vipList.add(map);
            }

            VipDO vip = xmlAccesser.getVipMapDAO().getVipById(vipId);
            long[] cobarId = vip.getCobarIds();

            for (int i = 0; i < cobarId.length; i++) {
                CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(cobarId[i]);
                Map<String, Object> map;
                try {
                    map = util.describe(cobar);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                map.remove("class");
                map.remove("buId");
                map.remove("user");
                map.remove("password");
                map.remove("time_diff");

                String clusterName = xmlAccesser.getClusterDAO().getClusterById(cobar.getClusterId()).getName();
                map.put("clusterName", clusterName);
                cobarList.add(map);
            }
        }

        return new ModelAndView("m_vipList", new FluenceHashMap<String, Object>().putKeyValue("user", user)
            .putKeyValue("vipList", vipList)
            .putKeyValue("cobarList", cobarList)
            .putKeyValue("vipId", vipId));

    }

}
