package com.alibaba.cobar.manager.web.screen;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.EncryptUtil;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class EditUserPage extends AbstractController implements InitializingBean {

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

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        long userId = 0;
        try {
            userId = Long.parseLong(request.getParameter("userId").trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("parameter 'userId' is invalid:" + request.getParameter("cobarId"));
        }
        UserDO editUser = xmlAccesser.getUserDAO().getUserById(userId);
        UserDO u = new UserDO();
        u.setId(editUser.getId());
        u.setPassword(EncryptUtil.decrypt(editUser.getPassword()));
        u.setRealname(editUser.getRealname());
        u.setStatus(editUser.getStatus());
        u.setUser_role(editUser.getUser_role());
        u.setUsername(editUser.getUsername());
        return new ModelAndView("m_editUser", new FluenceHashMap<String, Object>().putKeyValue("editUser", u));
    }

}
