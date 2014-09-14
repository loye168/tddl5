package com.alibaba.cobar.manager.web.screen;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class MVipCobarManager extends AbstractController implements InitializingBean {

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
        long vipId = Long.parseLong(request.getParameter("vipIdK"));

        VipDO vip = xmlAccesser.getVipMapDAO().getVipById(vipId);
        PropertyUtilsBean util = new PropertyUtilsBean();

        Set<Long> set = new HashSet<Long>();

        long cobarIds[] = vip.getCobarIds();
        for (int i = 0; i < cobarIds.length; i++) {
            set.add(cobarIds[i]);
        }

        List<CobarDO> cList = xmlAccesser.getCobarDAO().listAllCobar();
        List<Map<String, Object>> cobarList = new ArrayList<Map<String, Object>>();
        for (CobarDO c : cList) {
            Map<String, Object> map;
            try {
                map = util.describe(c);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            map.remove("class");
            map.remove("buId");
            map.remove("user");
            map.remove("password");
            map.remove("time_diff");
            map.remove("name");
            if (set.contains(c.getId())) {
                map.put("choose", true);
            } else {
                map.put("choose", false);
            }
            map.put("name", CobarStringUtil.htmlEscapedString(c.getName()));

            String clusterName = xmlAccesser.getClusterDAO().getClusterById(c.getClusterId()).getName();
            map.put("clusterName", CobarStringUtil.htmlEscapedString(clusterName));
            cobarList.add(map);
        }

        return new ModelAndView("m_vipCobarList", new FluenceHashMap<String, Object>().putKeyValue("user", user)
            .putKeyValue("vip", vip)
            .putKeyValue("cobarList", cobarList));
    }

}
