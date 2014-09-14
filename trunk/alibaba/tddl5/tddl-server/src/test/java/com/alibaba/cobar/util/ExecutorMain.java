package com.alibaba.cobar.util;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.cobar.net.util.ExecutorUtil;

/**
 * @author xianmao.hexm
 */
public class ExecutorMain {

    public static void main(String[] args) {
        final AtomicLong count = new AtomicLong(0L);
        final ThreadPoolExecutor executor = ExecutorUtil.create("TestExecutor", 5);

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    long c = count.get();
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("count:" + (count.get() - c) / 5);
                    System.out.println("active:" + executor.getActiveCount());
                    System.out.println("queue:" + executor.getQueue().size());
                    System.out.println("============================");
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                for (;;) {
                    executor.execute(new Runnable() {

                        @Override
                        public void run() {
                            count.incrementAndGet();
                        }
                    });
                }
            }
        }.start();
    }

}
