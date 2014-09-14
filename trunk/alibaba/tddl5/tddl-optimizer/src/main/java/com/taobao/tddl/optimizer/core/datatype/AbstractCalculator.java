package com.taobao.tddl.optimizer.core.datatype;

public abstract class AbstractCalculator implements Calculator {

    public abstract Object doAdd(Object v1, Object v2);

    public abstract Object doSub(Object v1, Object v2);

    public abstract Object doMultiply(Object v1, Object v2);

    public abstract Object doDivide(Object v1, Object v2);

    public abstract Object doMod(Object v1, Object v2);

    public abstract Object doAnd(Object v1, Object v2);

    public abstract Object doOr(Object v1, Object v2);

    public abstract Object doXor(Object v1, Object v2);

    public abstract Object doNot(Object v1);

    public abstract Object doBitAnd(Object v1, Object v2);

    public abstract Object doBitOr(Object v1, Object v2);

    public abstract Object doBitXor(Object v1, Object v2);

    public abstract Object doBitNot(Object v1);

    @Override
    public Object add(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doAdd(v1, v2);
    }

    @Override
    public Object sub(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doSub(v1, v2);
    }

    @Override
    public Object multiply(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doMultiply(v1, v2);
    }

    @Override
    public Object divide(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doDivide(v1, v2);
    }

    @Override
    public Object mod(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doMod(v1, v2);
    }

    @Override
    public Object and(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doAnd(v1, v2);
    }

    @Override
    public Object or(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doOr(v1, v2);
    }

    @Override
    public Object xor(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doXor(v1, v2);
    }

    @Override
    public Object not(Object v1) {
        if (v1 == null) return null;

        return this.doNot(v1);
    }

    @Override
    public Object bitAnd(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doBitAnd(v1, v2);
    }

    @Override
    public Object bitOr(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doBitOr(v1, v2);
    }

    @Override
    public Object bitXor(Object v1, Object v2) {
        if (v1 == null || v2 == null) return null;

        return this.doBitXor(v1, v2);
    }

    @Override
    public Object bitNot(Object v1) {
        if (v1 == null) return null;

        return this.doBitNot(v1);
    }

}
