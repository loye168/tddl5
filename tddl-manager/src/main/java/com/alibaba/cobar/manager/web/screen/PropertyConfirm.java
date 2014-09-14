package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.util.FluenceHashMap;

public class PropertyConfirm extends AbstractController implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        String type = null;
        String info = null;
        try {
            type = request.getParameter("type").trim();
        } catch (NullPointerException e) {
            type = null;
        }
        if ("configReload".equals(type)) {
            info = "确认进行配置重载";
        } else if ("configRollback".equals(type)) {
            info = "确认进行配置回滚";
        }
        return new ModelAndView("c_propertyConfirm", new FluenceHashMap<String, Object>().putKeyValue("type", type)
            .putKeyValue("info", info));
    }

}
