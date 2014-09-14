package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.util.FluenceHashMap;

public class MModifyPwdConfirm extends AbstractController implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        String info = "确认修改密码?";
        return new ModelAndView("m_modifyPwdConfirm", new FluenceHashMap<String, Object>().putKeyValue("info", info));
    }

}
