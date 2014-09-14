package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author wenfeng.cenwf 2011-4-7
 */
public class LogInPage extends AbstractController implements InitializingBean {

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
            result = "null";
        }
        if (result == null) {
            result = "null";
        }

        return new ModelAndView("logInPage", new FluenceHashMap<String, Object>().putKeyValue("result", result));
    }

}
