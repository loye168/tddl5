package com.alibaba.cobar.manager.util;

import java.util.HashMap;

/**
 * (created at 2010-7-22)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class FluenceHashMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = 1L;

    public FluenceHashMap(){
        super();
    }

    public FluenceHashMap(int initCap){
        super(initCap);
    }

    public FluenceHashMap<K, V> putKeyValue(K key, V value) {
        put(key, value);
        return this;
    }

    public FluenceHashMap<K, V> putKeyValue(Pair<K, V> pair) {
        if (pair != null) put(pair.getFirst(), pair.getSecond());
        return this;
    }
}
