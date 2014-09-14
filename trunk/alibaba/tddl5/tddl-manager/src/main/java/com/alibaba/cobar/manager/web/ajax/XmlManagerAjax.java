package com.alibaba.cobar.manager.web.ajax;

import static com.alibaba.cobar.manager.util.ConstantDefine.ADD_STOP_TIME;
import static com.alibaba.cobar.manager.util.ConstantDefine.ADD_VIP;
import static com.alibaba.cobar.manager.util.ConstantDefine.CHANGE_ROLE;
import static com.alibaba.cobar.manager.util.ConstantDefine.CHECK_OLD_PWD;
import static com.alibaba.cobar.manager.util.ConstantDefine.CHECK_USER_NAME_REPEAT;
import static com.alibaba.cobar.manager.util.ConstantDefine.CLUSTER_NAME_REPEAT;
import static com.alibaba.cobar.manager.util.ConstantDefine.CLUSTER_NAME_REPEAT_EXCEPT_SELF;
import static com.alibaba.cobar.manager.util.ConstantDefine.COBAR_NAME_REPEAT;
import static com.alibaba.cobar.manager.util.ConstantDefine.COBAR_NAME_REPEAT_EXCEPT_SELF;
import static com.alibaba.cobar.manager.util.ConstantDefine.DELETE_STOP_TIME;
import static com.alibaba.cobar.manager.util.ConstantDefine.DELETE_VIP;
import static com.alibaba.cobar.manager.util.ConstantDefine.PASSWORD_VALIDATE;
import static com.alibaba.cobar.manager.util.ConstantDefine.STOP_TIME_REPEAT;
import static com.alibaba.cobar.manager.util.ConstantDefine.USER_NAME_REPEAT;
import static com.alibaba.cobar.manager.util.ConstantDefine.USER_NAME_REPEAT_EXCEPT_SELF;
import static com.alibaba.cobar.manager.util.ConstantDefine.VIP_NAME_REPEAT;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.HttpRequestHandler;

import com.alibaba.cobar.manager.dataobject.xml.UserDO;
import com.alibaba.cobar.manager.dataobject.xml.VipDO;
import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.EncryptUtil;
import com.alibaba.cobar.manager.util.Pair;
import com.alibaba.fastjson.JSON;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class XmlManagerAjax implements HttpRequestHandler, InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException,
                                                                                       IOException {
        AjaxParams param = new AjaxParams(request);
        String jsonRst = null;

        String st = param.getValueType();
        if (null == st || st.equals("")) {
            throw new IllegalArgumentException("parameter 'cobarControlValueType' is unknown: " + st);
        }
        int type = map.get(st);

        switch (type) {
            case CLUSTER_NAME_REPEAT_EXCEPT_SELF:
                String name = param.getKeyword().trim();
                long id = param.getClusterId();
                Pair<String, Boolean> pair1 = new Pair<String, Boolean>("result", xmlAccesser.getClusterDAO()
                    .checkName(name, id));
                jsonRst = JSON.toJSONString(pair1);
                break;
            case CLUSTER_NAME_REPEAT:
                String cname = param.getKeyword().trim();
                Pair<String, Boolean> pair2 = new Pair<String, Boolean>("result", xmlAccesser.getClusterDAO()
                    .checkName(cname));
                jsonRst = JSON.toJSONString(pair2);
                break;
            case COBAR_NAME_REPEAT:
                String cobarname = param.getKeyword().trim();
                long clusterId = param.getClusterId();
                Pair<String, Boolean> pair3 = new Pair<String, Boolean>("result", xmlAccesser.getCobarDAO()
                    .checkName(cobarname, clusterId));
                jsonRst = JSON.toJSONString(pair3);
                break;
            case COBAR_NAME_REPEAT_EXCEPT_SELF:
                String coname = param.getKeyword().trim();
                long cluId = param.getClusterId();
                long coId = param.getCobarNodeId();
                Pair<String, Boolean> pair4 = new Pair<String, Boolean>("result", xmlAccesser.getCobarDAO()
                    .checkName(coname, cluId, coId));
                jsonRst = JSON.toJSONString(pair4);
                break;
            case USER_NAME_REPEAT:
                String username = param.getKeyword().trim();
                Pair<String, Boolean> pair5 = new Pair<String, Boolean>("result", xmlAccesser.getUserDAO()
                    .checkName(username));
                jsonRst = JSON.toJSONString(pair5);
                break;
            case USER_NAME_REPEAT_EXCEPT_SELF:
                String uname = param.getKeyword().trim();
                long userId = param.getUserId();
                Pair<String, Boolean> pair6 = new Pair<String, Boolean>("result", xmlAccesser.getUserDAO()
                    .checkName(uname, userId));
                jsonRst = JSON.toJSONString(pair6);
                break;
            case STOP_TIME_REPEAT:
                int time = Integer.parseInt(param.getKeyword().trim());
                Pair<String, Boolean> pair7 = new Pair<String, Boolean>("result", !xmlAccesser.getPropertyDAO()
                    .getProperty()
                    .getStopTimes()
                    .contains(time));
                jsonRst = JSON.toJSONString(pair7);
                break;
            case DELETE_STOP_TIME:
                int dtime = Integer.parseInt(param.getKeyword().trim());
                Pair<String, Boolean> pair8 = new Pair<String, Boolean>("result", xmlAccesser.getPropertyDAO()
                    .deleteTime(dtime));
                jsonRst = JSON.toJSONString(pair8);
                break;
            case ADD_STOP_TIME:
                int atime = Integer.parseInt(param.getKeyword().trim());
                Pair<String, Boolean> pair9 = new Pair<String, Boolean>("result", xmlAccesser.getPropertyDAO()
                    .addTime(atime));
                jsonRst = JSON.toJSONString(pair9);
                break;
            case VIP_NAME_REPEAT:
                String vip = param.getKeyword().trim();
                Pair<String, Boolean> pair11 = new Pair<String, Boolean>("result", xmlAccesser.getVipMapDAO()
                    .checkSid(vip));
                jsonRst = JSON.toJSONString(pair11);
                break;
            case ADD_VIP:
                // TODO new Object
                @SuppressWarnings("unused")
                String vip1 = param.getKeyword().trim();
                Pair<String, Boolean> pair12 = new Pair<String, Boolean>("result", xmlAccesser.getVipMapDAO()
                    .addVip(new VipDO()));
                jsonRst = JSON.toJSONString(pair12);
                break;
            case DELETE_VIP:
                long vipId = Long.parseLong(param.getKeyword().trim());
                Pair<String, Boolean> pair13 = new Pair<String, Boolean>("result", xmlAccesser.getVipMapDAO()
                    .deleteVip(vipId));
                jsonRst = JSON.toJSONString(pair13);
                break;
            case PASSWORD_VALIDATE:
                String password = param.getKeyword().trim();
                UserDO user = (UserDO) request.getSession().getAttribute("user");
                Pair<String, Boolean> pairPassword = new Pair<String, Boolean>("result", user.getPassword()
                    .equals(EncryptUtil.encrypt(password)));
                jsonRst = JSON.toJSONString(pairPassword);
                break;
            default:
                throw new IllegalArgumentException("type " + param.getValueType() + " is not valible");
        }
        response.setHeader("Content-Type", "text/json; charset=utf-8");
        OutputStream out = response.getOutputStream();
        out.write(jsonRst.getBytes("utf-8"));
        out.flush();
    }

    private static final Map<String, Integer> map = new HashMap<String, Integer>();
    static {
        map.put("clusterNameRepeat", CLUSTER_NAME_REPEAT);
        map.put("changeRole", CHANGE_ROLE);
        map.put("clusterNameRepeatExceptSelf", CLUSTER_NAME_REPEAT_EXCEPT_SELF);
        map.put("checkOldPwd", CHECK_OLD_PWD);
        map.put("userNameRepeat", CHECK_USER_NAME_REPEAT);
        map.put("cobarNameRepeat", COBAR_NAME_REPEAT);
        map.put("cobarNameRepeatExceptSelf", COBAR_NAME_REPEAT_EXCEPT_SELF);
        map.put("userNameRepeatExceptSelf", USER_NAME_REPEAT_EXCEPT_SELF);
        map.put("userNameRepeat", USER_NAME_REPEAT);
        map.put("stopTimesRepeat", STOP_TIME_REPEAT);
        map.put("delStopTimes", DELETE_STOP_TIME);
        map.put("addStopTimes", ADD_STOP_TIME);
        map.put("addVip", ADD_VIP);
        map.put("deleteVIP", DELETE_VIP);
        map.put("vipRepeat", VIP_NAME_REPEAT);
        map.put("passwordValidate", PASSWORD_VALIDATE);
    }

}
