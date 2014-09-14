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
public class ModifyPassword extends SimpleFormController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @SuppressWarnings("unused")
    private static class PasswordForm {

        private String oldPassword;
        private String newPassword;
        private String newPasswordr;

        public String getOldPassword() {
            return oldPassword;
        }

        public void setOldPassword(String oldPassword) {
            this.oldPassword = oldPassword;
        }

        public String getNewPassword() {
            return newPassword;
        }

        public void setNewPassword(String newPassword) {
            this.newPassword = newPassword;
        }

        public String getNewPasswordr() {
            return newPasswordr;
        }

        public void setNewPasswordr(String newPasswordr) {
            this.newPasswordr = newPasswordr;
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(PasswordForm.class);
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        PasswordForm form = (PasswordForm) command;
        UserDO user = (UserDO) request.getSession().getAttribute("user");

        user.setPassword(EncryptUtil.encrypt(form.getNewPassword()));

        boolean flag = xmlAccesser.getUserDAO().modifyUser(user);

        if (flag) {
            return new ModelAndView("m_success", "info", "密码修改成功");
        } else {
            return new ModelAndView("failure", "reason", "写文件失败，请联系管理员");
        }
    }
}
