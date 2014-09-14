package com.alibaba.cobar.util.perf;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;

/**
 * @author xianmao.hexm
 */
public class MapPerfMain {

    public void t1() {
        Map<String, Date> m = new HashMap<String, Date>();
        for (int i = 0; i < 100000; i++) {
            m.put(UUID.randomUUID().toString(), new Date());
        }
        remove1(m);
        Assert.assertEquals(0, m.size());
    }

    public void t2() {
        Map<String, Date> m = new HashMap<String, Date>();
        for (int i = 0; i < 100000; i++) {
            m.put(UUID.randomUUID().toString(), new Date());
        }
        remove2(m);
        Assert.assertEquals(0, m.size());
    }

    void remove1(Map<String, Date> m) {
        Iterator<Map.Entry<String, Date>> it = m.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue();
            it.remove();
        }
    }

    void remove2(Map<String, Date> m) {
        Iterator<Map.Entry<String, Date>> it = m.entrySet().iterator();
        while (it.hasNext()) {
            it.next().getValue();
        }
        m.clear();
    }

}
