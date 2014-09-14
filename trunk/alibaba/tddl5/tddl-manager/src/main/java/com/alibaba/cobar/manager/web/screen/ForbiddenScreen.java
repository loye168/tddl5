package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author wenfeng.cenwf 2011-4-2
 */
public class ForbiddenScreen extends AbstractController {

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        UserDO user = (UserDO) request.getSession().getAttribute("user");

        String lastUrl = request.getHeader("Referer");

        return new ModelAndView("forbidden", new FluenceHashMap<String, Object>().putKeyValue("user", user)
            .putKeyValue("lastUrl", lastUrl));
    }

}
