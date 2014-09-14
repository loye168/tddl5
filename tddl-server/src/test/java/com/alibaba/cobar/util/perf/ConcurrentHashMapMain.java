package com.alibaba.cobar.util.perf;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xianmao.hexm
 */
public class ConcurrentHashMapMain {

    private final ConcurrentMap<String, String> cm;

    public ConcurrentHashMapMain() {
        cm = new ConcurrentHashMap<String, String>();
        cm.put("abcdefg", "abcdefghijk");
    }

    public void tGet() {
        for (int i = 0; i < 1000000; i++) {
            cm.get("abcdefg");
        }
    }

    public void tGetNone() {
        for (int i = 0; i < 1000000; i++) {
            cm.get("abcdefghijk");
        }
    }

    public void tEmpty() {
        for (int i = 0; i < 1000000; i++) {
            cm.isEmpty();
        }
    }

    public void tRemove() {
        for (int i = 0; i < 1000000; i++) {
            cm.remove("abcdefg");
        }
    }

}
