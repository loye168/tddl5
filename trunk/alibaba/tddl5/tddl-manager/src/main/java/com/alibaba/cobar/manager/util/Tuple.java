package com.alibaba.cobar.manager.util;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * (created at 2010-8-4)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class Tuple {

    private final Object[] objects;

    public Tuple(Object... objects){
        if (objects == null) throw new IllegalArgumentException("no argument!");
        this.objects = objects;
    }

    /**
     * @param index start from 0
     */
    public Object getElement(int index) {
        return objects[index];
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        for (Object obj : objects) {
            builder.append(obj);
        }
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof Tuple)) return false;
        Tuple that = (Tuple) obj;
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(this.objects, that.objects);
        return builder.isEquals();
    }
}
