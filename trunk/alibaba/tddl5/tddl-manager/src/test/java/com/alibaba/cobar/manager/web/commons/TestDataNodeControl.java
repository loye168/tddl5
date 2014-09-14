package com.alibaba.cobar.manager.web.commons;

import java.util.List;

import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.AbstractDependencyInjectionSpringContextTests;
import org.springframework.web.servlet.ModelAndView;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.util.ConstantDefine;
import com.alibaba.cobar.manager.web.screen.DatanodesControlScreen;

public class TestDataNodeControl extends AbstractDependencyInjectionSpringContextTests {

    private DatanodesControlScreen datanodeControl;

    public void setDatanodeControl(DatanodesControlScreen datanodeControl) {
        this.datanodeControl = datanodeControl;
    }

    public TestDataNodeControl(){
        super();
    }

    @Override
    protected String[] getConfigPaths() {
        return new String[] { "/WEB-INF/cobarManager-servlet.xml" };
    }

    @SuppressWarnings("rawtypes")
    public void testDatanodeControl() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setMethod("POST");
        MockHttpSession session = new MockHttpSession();
        UserDO user = new UserDO();
        user.setStatus(ConstantDefine.NORMAL);
        user.setUser_role(ConstantDefine.CLUSTER_ADMIN);
        session.setAttribute("user", user);
        request.setSession(session);
        request.addParameter("clusterId", "1");
        try {
            ModelAndView mav = datanodeControl.handleRequest(request, new MockHttpServletResponse());
            Assert.assertEquals("1", String.valueOf(mav.getModel().get("clusterId")));
            Assert.assertEquals("true", String.valueOf(mav.getModel().get("uniform")));
            Assert.assertEquals("true", String.valueOf(mav.getModel().get("connecitonFlag")));
            Assert.assertEquals(1, ((List) mav.getModel().get("cList")).size());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail();
        }
    }
}
