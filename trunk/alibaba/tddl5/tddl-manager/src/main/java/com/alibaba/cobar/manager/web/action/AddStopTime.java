package com.alibaba.cobar.manager.web.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.validation.BindException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.SimpleFormController;

import com.alibaba.cobar.manager.service.XmlAccesser;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class AddStopTime extends SimpleFormController implements InitializingBean {

    private XmlAccesser xmlAccesser;

    public void setXmlAccesser(XmlAccesser xmlAccesser) {
        this.xmlAccesser = xmlAccesser;
    }

    @SuppressWarnings("unused")
    private static class TimeForm {

        private int times;

        public int getTimes() {
            return times;
        }

        public void setTimes(int times) {
            this.times = times;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setCommandClass(TimeForm.class);
        if (xmlAccesser == null) {
            throw new IllegalArgumentException("property 'xmlAccesser' is null!");
        }
    }

    @Override
    protected ModelAndView onSubmit(HttpServletRequest request, HttpServletResponse response, Object command,
                                    BindException errors) throws Exception {

        TimeForm time = (TimeForm) command;

        boolean flag = xmlAccesser.getPropertyDAO().addTime(time.getTimes());
        if (flag) {
            return new ModelAndView("m_success", "info", "StopTime信息添加成功");
        } else {
            String reason = time.getTimes() + "已经存在";
            return new ModelAndView("failure", "reason", reason);
        }
    }

}
