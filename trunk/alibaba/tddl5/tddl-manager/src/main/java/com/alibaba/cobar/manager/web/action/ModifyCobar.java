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
public class ModifyCobar extends SimpleFormController implements InitializingBean {

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

        private long   cobarId;
        private Long   clusterId;
        private String cobarName;
        private String host;
        private int    serverPort;
        private int    port;
        private String userName;
        private String password;
        private String status;

        public long getCobarId() {
            return cobarId;
        }

        public void setCobarId(long cobarId) {
            this.cobarId = cobarId;
        }

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
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        CobarForm form = (CobarForm) command;
        CobarDO cobar = new CobarDO();
        cobar.setId(form.getCobarId());
        cobar.setClusterId(form.getClusterId());
        cobar.setHost(form.getHost().trim());
        cobar.setName(form.getCobarName().trim());
        cobar.setPassword(form.getPassword().trim());
        cobar.setServerPort(form.getServerPort());
        cobar.setPort(form.getPort());
        cobar.setUser(form.getUserName().trim());
        cobar.setStatus(form.getStatus().trim());
        cobar.setTime_diff("0");

        CobarAdapterDAO perf = null;
        try {
            perf = cobarAccesser.getAccesser(cobar);
        } catch (Exception e) {
        }
        boolean flag = false;
        String reason = null;

        if (ConstantDefine.ACTIVE.equals(cobar.getStatus())) {
            if (perf != null && perf.checkConnection()) {
                // CobarDO cobarOld =
                // xmlAccesser.getCobarDAO().getCobarById(cobar.getId());
                flag = this.xmlAccesser.getCobarDAO().modifyCobar(cobar);
                if (!flag) {
                    reason = form.getCobarName() + "已经存在";
                }
                /*
                 * else if (!(flag = perf.setCobarStatus(true))) {
                 * this.xmlAccesser.getCobarDAO().modifyCobar(cobarOld); reason
                 * = form.getCobarName() + "更新状态失败"; }
                 */
            } else {
                reason = "cobar连接失败，修改信息不成功，请检查相关参数！";
            }
        } else {
            flag = this.xmlAccesser.getCobarDAO().modifyCobar(cobar);
            if (!flag) {
                reason = form.getCobarName() + "已经存在";
            }
            /*
             * else { if (perf != null) perf.setCobarStatus(false); }
             */
        }

        if (flag) {
            return new ModelAndView("m_success", "info", "Cobar信息修改成功");
        } else {
            return new ModelAndView("failure", "reason", reason);
        }
    }
}
