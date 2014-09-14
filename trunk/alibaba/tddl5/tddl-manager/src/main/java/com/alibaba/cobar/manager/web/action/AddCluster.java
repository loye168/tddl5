package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.validation.BindException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.service.XmlAccesser;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AddCluster extends SimpleFormController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @SuppressWarnings("unused")
    private static class ClusterForm {

        private String clusterName;
        private String deployName;
        private String maintName;
        private String deployDesc;
        private String onlineTime;
        private int    sortId;
        private String env;

        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public String getDeployName() {
            return deployName;
        }

        public void setDeployName(String deployName) {
            this.deployName = deployName;
        }

        public String getMaintName() {
            return maintName;
        }

        public void setMaintName(String maintName) {
            this.maintName = maintName;
        }

        public String getDeployDesc() {
            return deployDesc;
        }

        public void setDeployDesc(String deployDesc) {
            this.deployDesc = deployDesc;
        }

        public String getOnlineTime() {
            return onlineTime;
        }

        public void setOnlineTime(String onlineTime) {
            this.onlineTime = onlineTime;
        }

        public int getSortId() {
            return sortId;
        }

        public void setSortId(int sortId) {
            this.sortId = sortId;
        }

        public String getEnv() {
            return env;
        }

        public void setEnv(String env) {
            this.env = env;
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(ClusterForm.class);
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {
        ClusterForm form = (ClusterForm) command;
        ClusterDO cluster = new ClusterDO();
        cluster.setDeployContact(form.getDeployName().trim());
        cluster.setDeployDesc(form.getDeployDesc().trim());
        cluster.setMaintContact(form.getMaintName().trim());
        cluster.setName(form.getClusterName().trim());
        cluster.setOnlineTime(form.getOnlineTime().trim());
        cluster.setSortId(form.getSortId());
        cluster.setEnv(form.getEnv());
        boolean flag = this.xmlAccesser.getClusterDAO().addCluster(cluster);

        if (flag) {
            return new ModelAndView("m_success", "info", "集群信息添加成功");
        } else {
            String reason = form.getClusterName() + "已经存在";
            return new ModelAndView("failure", "reason", reason);
        }
    }

}
