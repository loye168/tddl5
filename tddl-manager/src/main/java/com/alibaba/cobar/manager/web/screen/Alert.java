package com.alibaba.cobar.manager.web.screen;

import static com.alibaba.cobar.manager.util.ConstantDefine.CHOOSE_COBAR;
import static com.alibaba.cobar.manager.util.ConstantDefine.CHOOSE_DATANODE;
import static com.alibaba.cobar.manager.util.ConstantDefine.CONNECTION_FAIL;
import static com.alibaba.cobar.manager.util.ConstantDefine.DATANODE_DIFF;
import static com.alibaba.cobar.manager.util.ConstantDefine.LOGIN;
import static com.alibaba.cobar.manager.util.ConstantDefine.PASSWORD_NULL;
import static com.alibaba.cobar.manager.util.ConstantDefine.UNKNOW;
import static com.alibaba.cobar.manager.util.ConstantDefine.USER_NULL;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

public class Alert extends AbstractController implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        int reason = 0;
        String result = null;
        try {
            reason = Integer.parseInt(request.getParameter("reason").trim());
        } catch (NullPointerException e) {
            reason = 0;
        }
        result = typeMap.get(reason);
        if (null == result) {
            result = typeMap.get(UNKNOW);
        }
        return new ModelAndView("failure", "reason", result);
    }

    private static final Map<Integer, String> typeMap = new HashMap<Integer, String>();
    static {
        typeMap.put(UNKNOW, "未知原因");
        typeMap.put(CHOOSE_COBAR, "请选择cobar!");
        typeMap.put(CONNECTION_FAIL, "集群中存在cobar连接异常，请检查!");
        typeMap.put(DATANODE_DIFF, "集群中各cobar的数据节点不一致，请检查!");
        typeMap.put(LOGIN, "请先登录!");
        typeMap.put(USER_NULL, "用户名不能为空!");
        typeMap.put(PASSWORD_NULL, "密码不能为空!");
        typeMap.put(CHOOSE_DATANODE, "请选择数据节点!");
    }

}
