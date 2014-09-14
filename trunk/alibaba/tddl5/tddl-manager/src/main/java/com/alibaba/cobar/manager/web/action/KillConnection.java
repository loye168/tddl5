package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dao.CobarAdapterDAO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.ConstantDefine;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class KillConnection extends AbstractController implements InitializingBean {

    private XmlAccesser   xmlAccesser;
    private CobarAccesser cobarAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    public void setCobarAccesser(CobarAccesser cobarAccesser) {
        this.cobarAccesser = cobarAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
        if (null == cobarAccesser) {
            throw new IllegalArgumentException("property 'cobarAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        UserDO user = (UserDO) request.getSession().getAttribute("user");

        long cobarId = Long.parseLong(request.getParameter("cobarId"));
        long connecId = Long.parseLong(request.getParameter("connectionId"));
        CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(cobarId);
        if (logger.isWarnEnabled()) {
            logger.warn(new StringBuilder().append(user.getUsername())
                .append(" | kill connection | cobar: ")
                .append(cobar.getName())
                .append(" | connection_id:")
                .append(connecId)
                .toString());
        }
        if (!cobar.getStatus().equals(ConstantDefine.ACTIVE)) {
            return new ModelAndView("c_failure", "reason", "cobar已被禁用，请仔细核对相关信息!");
        }
        CobarAdapterDAO control = cobarAccesser.getAccesser(cobarId);
        if (control.checkConnection()) {
            control.killConnection(connecId);
            return new ModelAndView("c_success", "info", "连接关闭成功!");
        }
        return new ModelAndView("c_failure", "reason", "cobar连接异常，请仔细核对相关信息!");
    }

}
