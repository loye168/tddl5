package com.alibaba.cobar.manager.util;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * (created at 2010-7-21)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class Pair<K, V> {

    private final K first;
    private final V second;

    public Pair(K first, V second){
        this.first = first;
        this.second = second;
    }

    public K getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(first).append(second).toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(first).append(second).toHashCode();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Pair)) return false;
        Pair that = (Pair) obj;
        return new EqualsBuilder().append(this.first, that.first).append(this.second, that.second).isEquals();
    }
}
