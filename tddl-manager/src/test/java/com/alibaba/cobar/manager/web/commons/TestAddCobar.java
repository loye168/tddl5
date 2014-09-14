package com.alibaba.cobar.manager.web.commons;

import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.AbstractDependencyInjectionSpringContextTests;
import org.springframework.web.servlet.ModelAndView;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.util.ConstantDefine;
import com.alibaba.cobar.manager.web.action.AddCobar;

public class TestAddCobar extends AbstractDependencyInjectionSpringContextTests {

    private AddCobar addcobar;

    public void setAddcobar(AddCobar addcobar) {
        this.addcobar = addcobar;
    }

    public TestAddCobar(){
        super();
    }

    @Override
    protected String[] getConfigPaths() {
        return new String[] { "/WEB-INF/cobarManager-servlet.xml" };
    }

    public void testAddCobar() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("POST");
        MockHttpSession session = new MockHttpSession();
        UserDO user = new UserDO();
        user.setStatus(ConstantDefine.NORMAL);
        user.setUser_role(ConstantDefine.CLUSTER_ADMIN);
        session.setAttribute("user", user);
        request.setSession(session);
        request.addParameter("clusterId", "1");
        request.addParameter("host", "1.2.4.3");
        request.addParameter("cobarName", "test");
        request.addParameter("port", "8066");
        request.addParameter("userName", "test");
        request.addParameter("password", "TTT");
        request.addParameter("status", "ACTIVE");

        ModelAndView mav = addcobar.handleRequest(request, new MockHttpServletResponse());
        Assert.assertEquals("add cobar success", String.valueOf(mav.getModel().get("info")));
    }
}
