package com.alibaba.cobar.manager.util;

import java.util.ArrayList;

/**
 * (created at 2010-7-22)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class FluenceArrayList<E> extends ArrayList<E> {

    private static final long serialVersionUID = 1L;

    public FluenceArrayList<E> addElement(E e) {
        add(e);
        return this;
    }
}
