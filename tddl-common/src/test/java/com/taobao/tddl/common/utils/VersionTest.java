package com.taobao.tddl.common.utils;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.utils.version.Version;

public class VersionTest {

    @Test
    public void testSimple() {
        System.out.println(Version.getVersion());
    }

    @Test
    public void testConvertVersion() {
        long l = Version.convertVersion("5.0.8");
        Assert.assertEquals(5000800l, l);
        l = Version.convertVersion("5.0.18-SNAPSHOT");
        Assert.assertEquals(5001800l, l);
        l = Version.convertVersion("15.18.18.6-b2b-SNAPSHOT");
        Assert.assertEquals(15181806l, l);
        l = Version.convertVersion("0.2.6-SNAPSHOT");
        Assert.assertEquals(20600, l);

        String v = Version.getVerionByPath("yunos-yunying-strom-1.0.0-jar-with-dependencies.jar");
        System.out.println(v);

    }
}
