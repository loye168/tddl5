package com.taobao.tddl.common.utils;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public final class Assert {

    /**
     * 确保对象不为空，否则抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_NULL</code>
     */
    public static void assertNotNull(Object object) {
        assertNotNull(object, null);
    }

    /**
     * 确保对象不为空，否则抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_NULL</code>
     */
    public static void assertNotNull(Object object, String message) {
        if (object == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_NULL, message);
        }
    }

    /**
     * 确保表达式为真，否则抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_TRUE</code>
     */
    public static void assertTrue(boolean expression) {
        assertTrue(expression, null);
    }

    /**
     * 确保表达式为真，否则抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_TRUE</code>
     */
    public static void assertTrue(boolean expression, String message) {
        if (!expression) {
            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_TRUE, message);
        }
    }

    /**
     * 抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_FAIL</code>
     */
    public static void fail() {
        fail(null);
    }

    /**
     * 抛出<code>TddlRuntimeException + ErrorCode.ERR_ASSERT_FAIL</code>
     */
    public static void fail(String message) {
        throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL, message);
    }

}
