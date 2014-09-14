package com.taobao.tddl.optimizer.parse.cobar;

import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.monitor.eagleeye.EagleeyeHelper;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.exception.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MysqlParameterizedSqlVistor;

/**
 * 基于cobar解析器实现parse
 */
public class CobarSqlParseManager extends AbstractLifecycle implements SqlParseManager {

    private long                               cacheSize  = 1000;
    private long                               expireTime = TddlConstants.DEFAULT_OPTIMIZER_EXPIRE_TIME;
    private static Cache<String, ParserResult> cache      = null;

    @Override
    protected void doInit() {
        cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(expireTime, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
    }

    @Override
    protected void doDestroy() {
        cache.invalidateAll();
    }

    @Override
    public SqlAnalysisResult parse(final String sql, boolean cached) throws SqlParserException {
        long time = System.currentTimeMillis();
        ParserResult st = null;
        try {
            // 只缓存sql的解析结果
            if (cached) {
                st = cache.get(sql, new Callable<ParserResult>() {

                    @Override
                    public ParserResult call() throws Exception {
                        return parse(sql);
                    }

                });
            } else {
                st = parse(sql);
            }
        } catch (SQLSyntaxErrorException e) {
            throw new SqlParserException(e, e.getMessage());
        } catch (ExecutionException e) {
            throw new OptimizerException(e);
        }

        // AstNode visitor结果不能做缓存
        CobarSqlAnalysisResult result = new CobarSqlAnalysisResult();
        result.build(sql, st.parameterizedSql, st.index, st.statement);
        time = Monitor.monitorAndRenewTime(Monitor.KEY1,
            Monitor.KEY2_TDDL_PARSE,
            Monitor.Key3Success,
            System.currentTimeMillis() - time);
        return result;

    }

    private ParserResult parse(String sql) throws SQLSyntaxErrorException {
        ParserResult result = new ParserResult();
        SQLStatement statement = SQLParserDelegate.parse(sql);
        result.statement = statement;

        StringBuilder appendable = new StringBuilder();
        MysqlParameterizedSqlVistor visitor = new MysqlParameterizedSqlVistor(appendable, true);
        statement.accept(visitor);
        result.parameterizedSql = visitor.getSql();
        // 和eagleEye约定，加上!前缀
        result.index = EagleeyeHelper.index("!" + result.parameterizedSql);
        return result;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public static class ParserResult {

        SQLStatement statement;
        String       parameterizedSql;
        String       index;
    }

}
