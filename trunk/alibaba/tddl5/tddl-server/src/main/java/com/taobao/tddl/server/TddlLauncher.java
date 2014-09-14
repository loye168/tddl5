package com.taobao.tddl.server;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.cobar.CobarServer;

/**
 * 启动server
 * 
 * @author jianghang 2014-4-30 下午5:00:50
 * @since 5.1.0
 */
public final class TddlLauncher {

    private static final Logger logger = LoggerFactory.getLogger(TddlLauncher.class);

    public static void main(String[] args) throws Throwable {
        try {
            logger.info("## start the tddl server.");
            final CobarServer server = CobarServer.getInstance();
            server.init();
            logger.info("## the tddl server is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    try {
                        logger.info("## stop the tddl server");
                        server.destroy();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping tddl server:\n{}",
                            ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        logger.info("## tddl server is down.");
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the tddl server:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}
