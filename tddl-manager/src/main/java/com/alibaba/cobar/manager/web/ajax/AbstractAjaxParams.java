package com.alibaba.cobar.manager.web.ajax;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * (created at 2010-9-15)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-3-28
 * @author haiqing.zhuhq 2011-9-1
 */
public abstract class AbstractAjaxParams {

    public AbstractAjaxParams(HttpServletRequest request){
        super();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        appendToStringBuilder(builder);
        return builder.toString();
    }

    protected abstract void appendToStringBuilder(ToStringBuilder builder);

    protected Integer getInt(HttpServletRequest request, String name, boolean required) {
        try {
            return Integer.parseInt(request.getParameter(name));
        } catch (Exception e) {
            if (required) throw new IllegalArgumentException("parameter '" + name + "' is invalid!");
            else return null;
        }
    }

    protected long getLong(HttpServletRequest request, String name, boolean required) {
        try {
            return Long.parseLong(request.getParameter(name));
        } catch (Exception e) {
            if (required) throw new IllegalArgumentException("parameter '" + name + "' is invalid!");
            else return -1;
        }
    }

    protected Long getLongObj(HttpServletRequest request, String name, boolean required) {
        try {
            return Long.parseLong(request.getParameter(name));
        } catch (Exception e) {
            if (required) throw new IllegalArgumentException("parameter '" + name + "' is invalid!");
            else return null;
        }
    }

    protected String getString(HttpServletRequest request, String name, boolean required) {

        String rst = request.getParameter(name);
        if (rst == null && required) {
            throw new NullPointerException("parameter '" + name + "' is null!");
        }
        return rst;
    }

}
