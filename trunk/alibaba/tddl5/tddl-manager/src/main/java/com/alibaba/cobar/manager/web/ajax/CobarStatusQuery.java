/**
 * (created at 2011-11-18)
 */
package com.alibaba.cobar.manager.web.ajax;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.HttpRequestHandler;

import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.VipDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.ConstantDefine;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class CobarStatusQuery implements HttpRequestHandler, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(CobarStatusQuery.class);
    private XmlAccesser         xmlAccesser;
    private CobarAccesser       cobarAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    public void setCobarAccesser(CobarAccesser cobarAccesser) {
        this.cobarAccesser = cobarAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (null == xmlAccesser) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
        if (null == cobarAccesser) {
            throw new IllegalArgumentException("property 'cobarAccesser' is null!");
        }
    }

    @Override
    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException,
                                                                                       IOException {
        try {
            handleRequestInternal(request, response);
        } catch (IllegalArgumentException e) {
            logger.error("### state invalid", e);
            throw e;
        }
    }

    private void handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                throws ServletException,
                                                                                                IOException {
        String sid = request.getParameter("sid");
        if (sid == null) {
            throw new IllegalArgumentException("parameter 'sid' is required!");
        }

        VipDO vip = xmlAccesser.getVipMapDAO().getVipBySid(sid);

        if (vip == null) {
            throw new IllegalArgumentException("vip with name of " + sid + " is not exist!");
        }

        long[] cobarIdList = vip.getCobarIds();
        if (cobarIdList == null || cobarIdList.length == 0) {
            throw new IllegalArgumentException("vip with name of " + sid + " is not exist!");
        }
        int[] weight = vip.getWeights();
        if (weight == null || weight.length <= 0) {
            weight = new int[cobarIdList.length];
            for (int i = 0; i < weight.length; ++i) {
                weight[i] = 1;
            }
        }

        List<CobarDO> list = xmlAccesser.getCobarDAO().listCobarById(cobarIdList);
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("vip with name of " + sid + " contains no Cobar node!");
        }

        StringBuilder json = new StringBuilder("{\r\n\"cobarList\": [");
        boolean isFst = true;
        for (int i = 0; isFst && i < 3; ++i) {
            for (Iterator<CobarDO> iter = list.iterator(); iter.hasNext();) {
                CobarDO cobar = iter.next();
                if (checkCobarDO(cobar) && ConstantDefine.ACTIVE.equals(cobar.getStatus())
                    && cobarAccesser.getAccesser(cobar).checkConnection()) {
                    if (isFst) isFst = false;
                    else json.append(", ");
                    appendCobar(json, vip, weight, cobar);
                    iter.remove();
                }
            }
        }
        if (isFst) {
            throw new IllegalArgumentException("vip with name of " + sid + " contains no valid Cobar node: " + list);
        }
        json.append("\r\n]\r\n}");

        response.setHeader("Content-Type", "text/json; charset=UTF8");
        response.setHeader("Connection", "close");
        OutputStream out = response.getOutputStream();
        out.write(json.toString().getBytes("utf-8"));
        // out.flush();
    }

    private void appendCobar(StringBuilder json, VipDO vip, int[] weight, CobarDO cobar) {
        long[] ids = vip.getCobarIds();
        long id = cobar.getId();
        int w = -1;
        for (int i = 0; i < ids.length; ++i) {
            if (ids[i] == id) {
                w = weight[i];
                break;
            }
        }
        if (w < 0) {
            logger.error("### vip contains a cobar node with id of " + id + " not included in vip's defination");
            w = 1;
        }
        json.append("\r\n\t{");
        json.append("\r\n\t\"ip\": \"").append(cobar.getHost()).append("\",");
        json.append("\r\n\t\"port\": ").append(cobar.getServerPort()).append(',');
        json.append("\r\n\t\"schema\": \"").append(vip.getSchema()).append("\",");
        json.append("\r\n\t\"weight\": ").append(w);
        json.append("\r\n\t}");
    }

    private boolean checkCobarDO(CobarDO cobar) {
        if (cobar == null) return false;
        String host = cobar.getHost();
        int port = cobar.getServerPort();
        return host != null && host.length() > 0 && port > 0;
    }

}
