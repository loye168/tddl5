package com.alibaba.cobar.manager.web.screen;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.service.XmlAccesser;
import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-6-27
 */
public class EditStopTimesPage extends AbstractController implements InitializingBean {

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
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {
        List<Integer> list = xmlAccesser.getPropertyDAO().getProperty().getStopTimes();
        return new ModelAndView("m_editStopTimes", new FluenceHashMap<String, Object>().putKeyValue("list", list));
    }

}
