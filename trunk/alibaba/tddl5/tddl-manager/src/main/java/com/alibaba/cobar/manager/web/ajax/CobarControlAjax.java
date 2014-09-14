package com.alibaba.cobar.manager.web.ajax;

import static com.alibaba.cobar.manager.util.ConstantDefine.COBAR_LIST;
import static com.alibaba.cobar.manager.util.ConstantDefine.KILL_CONNECTION;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.HttpRequestHandler;

import com.alibaba.cobar.manager.dao.CobarAdapterDAO;
import com.alibaba.cobar.manager.dataobject.xml.CobarDO;
import com.alibaba.cobar.manager.service.CobarAccesser;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.ConstantDefine;
import com.alibaba.cobar.manager.util.ListSortUtil;
import com.alibaba.cobar.manager.util.Pair;
import com.alibaba.fastjson.JSON;

/**
 * @author haiqing.zhuhq 2011-8-11
 */
public class CobarControlAjax implements HttpRequestHandler, InitializingBean {

    private XmlAccesser   xmlAccesser;
    private CobarAccesser cobarAccesser;

    public void setCobarAccesser(CobarAccesser cobarAccesser) {
        this.cobarAccesser = cobarAccesser;
    }

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
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

    private boolean killConnections(AjaxParams params) {
        long cobarId = params.getCobarNodeId();
        long connecId = params.getConnectionId();
        CobarDO cobar = xmlAccesser.getCobarDAO().getCobarById(cobarId);
        if (!cobar.getStatus().equals(ConstantDefine.ACTIVE)) {
            return false;
        }
        CobarAdapterDAO control = cobarAccesser.getAccesser(cobarId);
        if (control.checkConnection()) {
            control.killConnection(connecId);
            return true;
        }
        return false;
    }

    private List<Map<String, Object>> getCobarList(AjaxParams params) {
        long clusterId = params.getClusterId();
        List<CobarDO> cobarList = xmlAccesser.getCobarDAO().getCobarList(clusterId, ConstantDefine.ACTIVE);
        ListSortUtil.sortCobarByName(cobarList);
        List<Map<String, Object>> cobarViewList = new ArrayList<Map<String, Object>>();
        for (CobarDO c : cobarList) {
            CobarAdapterDAO perf = cobarAccesser.getAccesser(c.getId());
            if (perf.checkConnection()) {
                Map<String, Object> cobarMap = new HashMap<String, Object>();
                cobarMap.put("id", c.getId());
                cobarMap.put("name", c.getName());
                cobarViewList.add(cobarMap);
            }
        }
        return cobarViewList;
    }

    @Override
    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException,
                                                                                       IOException {
        AjaxParams params = new AjaxParams(request);
        String jsonRst = null;
        String st = params.getValueType();
        if (null == st || st.equals("")) {
            throw new IllegalArgumentException("parameter 'cobarControlValueType' is unknown: " + st);
        }
        int type = typeMap.get(st);
        switch (type) {
            case COBAR_LIST:
                List<Map<String, Object>> cobarList = getCobarList(params);
                jsonRst = JSON.toJSONString(cobarList);
                break;
            case KILL_CONNECTION:
                Pair<String, Boolean> kill = new Pair<String, Boolean>("result", killConnections(params));
                jsonRst = JSON.toJSONString(kill);
                break;
            default:
                throw new IllegalArgumentException("parameter 'cobarControlValueType' is unknown: "
                                                   + params.getValueType());
        }

        response.setHeader("Content-Type", "text/json; charset=utf-8");
        OutputStream out = response.getOutputStream();
        out.write(jsonRst.getBytes("utf-8"));
        out.flush();
    }

    private static final Map<String, Integer> typeMap = new HashMap<String, Integer>();
    static {
        typeMap.put("cobarList", COBAR_LIST);
        typeMap.put("killconnection", KILL_CONNECTION);
    }
}
