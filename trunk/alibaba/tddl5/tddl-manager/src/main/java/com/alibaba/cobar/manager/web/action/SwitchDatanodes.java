package com.alibaba.cobar.manager.web.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.dao.CobarAdapterDAO;
import com.alibaba.cobar.manager.dataobject.xml.ClusterDO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.CobarStringUtil;
import com.alibaba.cobar.manager.util.ConstantDefine;

/**
 * @author haiqing.zhuhq 2011-8-17
 */
public class SwitchDatanodes extends AbstractController implements InitializingBean {

    private XmlAccesser                xmlAccesser;
    private CobarAccesser              cobarAccesser;
    private static final ReentrantLock lock = new ReentrantLock();

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
        String datanodes = request.getParameter("datanodes");
        int index = Integer.parseInt(request.getParameter("index"));
        long clusterId = Long.parseLong(request.getParameter("clusterIdK"));

        UserDO user = (UserDO) request.getSession().getAttribute("user");
        ClusterDO cluster = xmlAccesser.getClusterDAO().getClusterById(clusterId);

        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();

        List<CobarDO> cobarList = xmlAccesser.getCobarDAO().getCobarList(clusterId, ConstantDefine.ACTIVE);
        if (logger.isWarnEnabled()) {
            StringBuilder log = new StringBuilder();
            log.append(user.getUsername()).append(" | switch datanodes | cluster:");
            log.append(cluster.getName()).append(" | ");
            log.append(datanodes).append(" | index:");
            log.append(index);

            logger.warn(log.toString());
        }

        lock.lock();
        try {
            for (CobarDO c : cobarList) {
                CobarAdapterDAO control = cobarAccesser.getAccesser(c.getId());
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("name", CobarStringUtil.htmlEscapedString(c.getName()));
                if (control.checkConnection()) {
                    int num = control.switchDataNode(datanodes, index);
                    map.put("result", num + " rows");
                } else {
                    map.put("result", "connction error!");
                }
                resultList.add(map);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Map<String, Object> map = new HashMap<String, Object>();
            map.put("name", "UNKNOWN ERROR");
            map.put("result", "操作时发生未知异常，请仔细检查cobar状态！");
            resultList.clear();
            resultList.add(map);
        } finally {
            lock.unlock();
        }

        return new ModelAndView("c_result", "resultList", resultList);
    }
}
