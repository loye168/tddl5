package com.ali.luna.mortred.tddl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ClassUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.rule.VirtualTableRoot;

public class AutoDetectedVirtualTableRoot extends VirtualTableRoot implements ApplicationContextAware {

    public static final String TABLERULE_CLASS = "com.taobao.tddl.rule.TableRule";
    private ApplicationContext context;

    @Override
    public void init() throws TddlException {
        Map vts = new HashMap();
        String[] tbeanNames = this.context.getBeanNamesForType(getClz());
        for (String name : tbeanNames) {
            Object obj = this.context.getBean(name);
            vts.put(name, obj);
        }
        setTableRules(vts);
        super.init();
    }

    private Class<?> getClz() {
        Class c2;
        try {
            c2 = ClassUtils.forName("com.taobao.tddl.rule.TableRule");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        return c2;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
}
