package com.alibaba.cobar.manager.web.screen;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

import com.alibaba.cobar.manager.util.FluenceHashMap;

/**
 * @author haiqing.zhuhq 2011-8-17
 */
public class ChooseIndexPage extends AbstractController implements InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
    }

    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response)
                                                                                                          throws Exception {

        int maxIndex = Integer.parseInt(request.getParameter("maxIndex"));

        List<Integer> indexList = new ArrayList<Integer>(maxIndex);

        for (int i = 0; i < maxIndex; i++) {
            indexList.add(i);
        }

        return new ModelAndView("c_datanodeIndex", new FluenceHashMap<String, Object>().putKeyValue("indexList",
            indexList));
    }

}
