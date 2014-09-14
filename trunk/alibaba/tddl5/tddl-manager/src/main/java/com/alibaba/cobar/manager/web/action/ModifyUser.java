package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.validation.BindException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.EncryptUtil;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class ModifyUser extends SimpleFormController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @SuppressWarnings("unused")
    private static class UserForm {

        private long   userId;
        private String realname;
        private String username;
        private String password;
        private String user_role;
        private String status;

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getRealname() {
            return realname;
        }

        public void setRealname(String realname) {
            this.realname = realname;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getUser_role() {
            return user_role;
        }

        public void setUser_role(String user_role) {
            this.user_role = user_role;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(UserForm.class);
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        UserForm form = (UserForm) command;
        UserDO user = new UserDO();
        user.setId(form.getUserId());
        user.setPassword(EncryptUtil.encrypt(form.getPassword().trim()));
        user.setRealname(form.getRealname().trim());
        user.setUser_role(form.getUser_role().trim());
        user.setUsername(form.getUsername().trim());
        user.setStatus(form.getStatus().trim());

        boolean flag = xmlAccesser.getUserDAO().modifyUser(user);

        if (flag) {
            return new ModelAndView("m_success", "info", "用户信息修改成功");
        } else {
            String reason = form.getUsername() + "已经存在";
            return new ModelAndView("failure", "reason", reason);
        }
    }

}
