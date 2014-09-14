package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2012-1-12
 */
public class LogIn extends AbstractController implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {

        String result = null;
        try {
            result = request.getParameter("result").trim();
        } catch (NullPointerException e) {
            result = "login";
        }
        if (result == null) {
            result = "login";
        }

        return new ModelAndView("login", new FluenceHashMap<String, Object>().putKeyValue("result", result));
    }

}
