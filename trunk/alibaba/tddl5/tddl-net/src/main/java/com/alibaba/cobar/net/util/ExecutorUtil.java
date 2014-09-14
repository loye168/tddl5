package com.alibaba.cobar.net.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xianmao.hexm
 */
public class ExecutorUtil {

    public static final NameableExecutor create(String name, int size) {
        return create(name, size, true);
    }

    public static final NameableExecutor create(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, new LinkedBlockingQueue<Runnable>(), factory);
    }

    public static final NameableExecutor createCapacity(String name, int size) {
        return createCapacity(name, size, true);
    }

    public static final NameableExecutor createCapacity(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name,
            size,
            new ArrayBlockingQueue(size * 2),
            factory,
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static class NameableThreadFactory implements ThreadFactory {

        private final ThreadGroup   group;
        private final String        namePrefix;
        private final AtomicInteger threadId;
        private final boolean       isDaemon;

        public NameableThreadFactory(String name, boolean isDaemon){
            SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = name;
            this.threadId = new AtomicInteger(0);
            this.isDaemon = isDaemon;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadId.getAndIncrement());
            t.setDaemon(isDaemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

}
