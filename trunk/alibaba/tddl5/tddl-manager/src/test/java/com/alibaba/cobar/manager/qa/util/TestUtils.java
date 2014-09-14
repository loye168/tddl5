package com.alibaba.cobar.manager.qa.util;

public class TestUtils {

    public static void waitForMonment(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
