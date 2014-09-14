package com.taobao.tddl.common.model.lifecycle;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author jianghang 2013-10-23 下午5:19:38
 * @since 5.0.0
 */
public class AbstractLifecycle implements Lifecycle {

    protected final Object     lock     = new Object();
    protected volatile boolean isInited = false;

    public void init() throws TddlException {
        synchronized (lock) {
            if (isInited()) {
                return;
            }

            try {
                doInit();
                isInited = true;
            } catch (Exception e) {
                // 出现异常调用destory方法，释放
                try {
                    doDestroy();
                } catch (Exception e1) {
                    // ignore
                }
                throw new TddlException(e);
            }
        }
    }

    public void destroy() throws TddlException {
        synchronized (lock) {
            if (!isInited()) {
                return;
            }

            doDestroy();
            isInited = false;
        }
    }

    public boolean isInited() {
        return isInited;
    }

    protected void doInit() throws TddlException {
    }

    protected void doDestroy() throws TddlException {
    }

}
