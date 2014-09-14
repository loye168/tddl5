package com.alibaba.cobar;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.cobar.config.SystemConfig;
import com.alibaba.cobar.manager.ManagerConnectionFactory;
import com.alibaba.cobar.net.FrontendConnection;
import com.alibaba.cobar.net.NIOAcceptor;
import com.alibaba.cobar.net.NIOConnector;
import com.alibaba.cobar.net.NIOProcessor;
import com.alibaba.cobar.net.util.ExecutorUtil;
import com.alibaba.cobar.net.util.NameableExecutor;
import com.alibaba.cobar.net.util.TimeUtil;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.SQLLexer;
import com.alibaba.cobar.server.ServerConnectionFactory;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.common.utils.version.Version;

/**
 * @author xianmao.hexm 2011-4-19 下午02:58:59
 */
public class CobarServer extends AbstractLifecycle implements Lifecycle {

    public static final String       NAME     = "TDDL";
    public static final String       VERSION  = "5.5.16-TDDL-" + Version.getVersion();

    private static final Logger      logger   = LoggerFactory.getLogger(CobarServer.class);
    private static final CobarServer INSTANCE = new CobarServer();

    public static final CobarServer getInstance() {
        return INSTANCE;
    }

    private final CobarConfig      config;
    private final Timer            timer;
    private final NameableExecutor managerExecutor;
    private final NameableExecutor timerExecutor;
    private final NameableExecutor serverExectuor;
    private final AtomicBoolean    isOnline;
    private long                   startupTime;
    private NIOProcessor[]         processors;
    private NIOConnector           connector;
    private NIOAcceptor            manager;
    private NIOAcceptor            server;

    private CobarServer(){
        this.config = new CobarConfig();
        SystemConfig system = config.getSystem();
        SQLLexer.setCStyleCommentVersion(system.getParserCommentVersion());
        FrontendConnection.setServerVersion(VERSION);
        this.timer = new Timer(NAME + "Timer", true);
        this.timerExecutor = ExecutorUtil.create("TimerExecutor", system.getTimerExecutor());
        this.managerExecutor = ExecutorUtil.create("ManagerExecutor", system.getManagerExecutor());
        this.serverExectuor = ExecutorUtil.createCapacity("ServerExecutor", system.getServerExecutor());
        this.isOnline = new AtomicBoolean(true);
    }

    @Override
    protected void doInit() throws TddlException {
        try {
            // server startup
            logger.info("===============================================");
            logger.info(NAME + " is ready to startup ...");
            SystemConfig system = config.getSystem();
            logger.info("Startup Cluster : " + system.getClusterName());
            timer.schedule(updateTime(), 0L, system.getTimeUpdatePeriod());
            this.config.init();

            // startup processors
            logger.info("Startup processors ...");
            int handler = system.getProcessorHandler();
            int killExecutor = system.getProcessorKillExecutor();
            processors = new NIOProcessor[system.getProcessors()];
            for (int i = 0; i < processors.length; i++) {
                processors[i] = new NIOProcessor("Processor" + i, handler, killExecutor);
                processors[i].startup();
            }
            timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());

            // startup connector
            logger.info("Startup connector ...");
            connector = new NIOConnector(NAME + "Connector");
            connector.setProcessors(processors);
            connector.start();

            // startup manager
            ManagerConnectionFactory mf = new ManagerConnectionFactory();
            mf.setCharset(system.getCharset());
            mf.setIdleTimeout(system.getIdleTimeout());
            manager = new NIOAcceptor(NAME + "Manager", system.getManagerPort(), mf);
            manager.setProcessors(processors);
            manager.start();
            logger.info(manager.getName() + " is started and listening on " + manager.getPort());

            // startup server
            ServerConnectionFactory sf = new ServerConnectionFactory();
            sf.setCharset(system.getCharset());
            sf.setIdleTimeout(system.getIdleTimeout());
            server = new NIOAcceptor(NAME + "Server", system.getServerPort(), sf);
            server.setProcessors(processors);
            server.start();

            // server started
            logger.info(server.getName() + " is started and listening on " + server.getPort());
            logger.info("===============================================");
            this.startupTime = TimeUtil.currentTimeMillis();
        } catch (Throwable e) {
            throw new TddlException(ErrorCode.ERR_SERVER, e, "start failed");
        }
    }

    @Override
    protected void doDestroy() throws TddlException {
        try {
            logger.info("===============================================");
            logger.info(NAME + " is ready to stop ...");
            // 关闭接入
            server.interrupt();
            server.join(1 * 1000);
            manager.interrupt();
            manager.join(1 * 1000);
            connector.interrupt();
            connector.join(1 * 1000);
            // 关闭数据源
            this.config.destroy();
            logger.info(server.getName() + " is stoped");
            logger.info("===============================================");
        } catch (InterruptedException e) {
        }
    }

    public CobarConfig getConfig() {
        return config;
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public NIOConnector getConnector() {
        return connector;
    }

    public NameableExecutor getManagerExecutor() {
        return managerExecutor;
    }

    public NameableExecutor getServerExectuor() {
        return serverExectuor;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    // 系统时间定时更新任务
    private TimerTask updateTime() {
        return new TimerTask() {

            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // 处理器定时检查任务
    private TimerTask processorCheck() {
        return new TimerTask() {

            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {

                    @Override
                    public void run() {
                        for (NIOProcessor p : processors) {
                            p.check();
                        }
                    }
                });
            }
        };
    }

}
