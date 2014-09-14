package com.alibaba.cobar.manager.util;

import java.util.HashSet;

/**
 * (created at 2010-9-16)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class FluenceHashSet<E> extends HashSet<E> {

    private static final long serialVersionUID = -2553186102347930216L;

    public FluenceHashSet(){
        super();
    }

    public FluenceHashSet(int initialCapacity){
        super(initialCapacity);
    }

    public FluenceHashSet<E> addElement(E e) {
        super.add(e);
        return this;
    }
}
