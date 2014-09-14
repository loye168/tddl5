package com.alibaba.cobar.manager.web;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;

/**
 * (created at 2010-7-20)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-4-2
 * @author haiqing.zhuhq 2011-6-20
 */
public class AuthenticationInterceptor extends HandlerInterceptorAdapter implements InitializingBean {

    private Set<String> nonMatchURISet = new HashSet<String>();
    private Set<String> switchURISet   = new HashSet<String>();

    public void setNonMatchURISet(Set<String> nonMatchURISet) {
        this.nonMatchURISet = nonMatchURISet;
    }

    public void setSwitchURISet(Set<String> switchURISet) {
        this.switchURISet = switchURISet;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (nonMatchURISet == null) throw new IllegalArgumentException("property 'nonMatchURISet' is null!");
        if (switchURISet == null) throw new IllegalArgumentException("property 'switchURISet' is null!");
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        boolean click = false;
        try {
            click = Boolean.parseBoolean(request.getParameter("click").trim());
        } catch (NullPointerException e) {
            click = false;
        }
        request.getSession().setAttribute("click", click);

        if (!authenticated(request)) {
            // TODO redirect
            if ((Boolean) request.getSession().getAttribute("click")) {
                response.sendRedirect(URLBroker.redirectIndexPage("login"));
                return false;
            }
            response.sendRedirect(URLBroker.redirectLogInPage("login"));
            return false;
        }

        return super.preHandle(request, response, handler);
    }

    private boolean authenticated(HttpServletRequest request) {

        UserDO o = (UserDO) request.getSession().getAttribute("user");
        if (o != null) {
            return true;
        }
        String url = request.getServletPath().trim();

        if (nonMatchURISet.contains(url)) {
            return true;
        } else if (switchURISet.contains(url)) {
            String qString = request.getQueryString();

            StringBuilder sb = new StringBuilder(url.substring(1));
            if (null != qString) {
                sb.append("?").append(qString);
            }
            request.getSession().setAttribute("lastRequest", sb.toString());
        }
        return false;

    }
}
