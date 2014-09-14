package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.validation.BindException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.alibaba.cobar.manager.dao.CobarAdapterDAO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.ConstantDefine;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AddCobar extends SimpleFormController implements InitializingBean {

    private XmlAccesser   xmlAccesser;
    private CobarAccesser cobarAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    public void setCobarAccesser(CobarAccesser cobarAccesser) {
        this.cobarAccesser = cobarAccesser;
    }

    @SuppressWarnings("unused")
    private static class CobarForm {

        private Long   clusterId;
        private String cobarName;
        private String host;
        private int    port;
        private int    serverPort;
        private String userName;
        private String password;
        private String status;

        public Long getClusterId() {
            return clusterId;
        }

        public void setClusterId(Long clusterId) {
            this.clusterId = clusterId;
        }

        public String getCobarName() {
            return cobarName;
        }

        public void setCobarName(String cobarName) {
            this.cobarName = cobarName;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public int getServerPort() {
            return serverPort;
        }

        public void setServerPort(int serverPort) {
            this.serverPort = serverPort;
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(CobarForm.class);
        if (null == xmlAccesser) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
        if (null == cobarAccesser) {
            throw new IllegalArgumentException("property 'cobarAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        CobarForm form = (CobarForm) command;
        CobarDO cobar = new CobarDO();
        cobar.setClusterId(form.getClusterId());
        cobar.setHost(form.getHost().trim());
        cobar.setName(form.getCobarName().trim());
        cobar.setPassword(form.getPassword().trim());
        cobar.setServerPort(form.getServerPort());
        cobar.setPort(form.getPort());
        cobar.setUser(form.getUserName().trim());
        cobar.setStatus(form.getStatus().trim());
        cobar.setTime_diff("0");

        CobarAdapterDAO perf = cobarAccesser.getAccesser(cobar);
        boolean flag = false;
        String reason = null;
        if (ConstantDefine.ACTIVE.equals(cobar.getStatus())) {
            if (perf.checkConnection()) {
                flag = this.xmlAccesser.getCobarDAO().addCobar(cobar);
                if (!flag) {
                    reason = form.getCobarName() + "已经存在";
                }
            } else {
                reason = "cobar连接失败，添加信息不成功，请检查相关参数！";
            }
        } else {
            flag = this.xmlAccesser.getCobarDAO().addCobar(cobar);
            if (!flag) {
                reason = form.getCobarName() + "已经存在";
            }
        }

        if (flag) {
            return new ModelAndView("m_success", "info", "Cobar信息添加成功");
        } else {
            return new ModelAndView("failure", "reason", reason);
        }
    }

}
