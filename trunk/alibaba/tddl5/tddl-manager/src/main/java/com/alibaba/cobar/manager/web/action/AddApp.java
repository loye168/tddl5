package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.validation.BindException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.alibaba.cobar.manager.dataobject.xml.AppDO;
import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.service.XmlAccesser;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AddApp extends SimpleFormController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @SuppressWarnings("unused")
    private static class AppForm {

        private String clusterId;
        private String appName;
        private String password;

        public String getClusterId() {
            return clusterId;
        }

        public void setClusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(AppForm.class);
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        AppForm form = (AppForm) command;
        AppDO app = new AppDO();
        app.setAppName(form.getAppName());
        app.setPassword(form.getPassword());

        long clusterId = -1;
        if (form.getClusterId() != null) {
            clusterId = Long.valueOf(form.getClusterId());
        }

        ClusterDO cluster = this.xmlAccesser.getClusterDAO().getClusterById(clusterId);

        if (cluster == null) {
            String reason = "cluster:" + form.getClusterId() + "不存在";
            return new ModelAndView("failure", "reason", reason);
        }

        app.setClusterId(cluster.getId());

        String error = this.xmlAccesser.getClusterDAO().addApp(app);

        if (error == null) {
            return new ModelAndView("m_success", "info", "App添加成功");
        } else {
            return new ModelAndView("failure", "reason", error);
        }
    }
}
