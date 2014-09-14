package com.taobao.tddl.rule.enumerator.handler;

import java.util.Set;

import com.taobao.tddl.common.model.sqljep.Comparative;

public abstract class NumberPartDiscontinousRangeEnumerator extends PartDiscontinousRangeEnumerator {

    protected static final int     LIMIT_UNIT_OF_LONG        = 1;
    protected static final int     DEFAULT_LONG_ATOMIC_VALUE = 1;
    protected static final boolean isAllowNegative;
    static {
        /**
         * 大多数整形的ID/分库分表字段默认都是大于零的。如果有小于0的系统，那么将这个参数设为true，
         * 同时自己要保证要么不出现id<3这样的条件，要么算出负的dbIndex也没有问题
         */
        isAllowNegative = "true".equals(System.getProperty("com.taobao.tddl.rule.isAllowNegativeShardValue", "false"));
    }

    @Override
    protected Comparative changeGreater2GreaterOrEq(Comparative from) {
        if (from.getComparison() == Comparative.GreaterThan) {
            Number fromComparable = cast2Number(from.getValue());
            return new Comparative(Comparative.GreaterThanOrEqual,
                (Comparable) plus(fromComparable, LIMIT_UNIT_OF_LONG));
        } else {
            return from;
        }
    }

    @Override
    protected Comparative changeLess2LessOrEq(Comparative to) {
        if (to.getComparison() == Comparative.LessThan) {
            Number toComparable = cast2Number(to.getValue());
            return new Comparative(Comparative.LessThanOrEqual,
                (Comparable) plus(toComparable, -1 * LIMIT_UNIT_OF_LONG));
        } else {
            return to;
        }
    }

    @Override
    protected Comparable getOneStep(Comparable source, Comparable atomIncVal) {
        if (atomIncVal == null) {
            atomIncVal = DEFAULT_LONG_ATOMIC_VALUE;
        }
        Number sourceLong = cast2Number(source);
        int atomIncValInt = (Integer) atomIncVal;
        return (Comparable) plus(sourceLong, atomIncValInt);
    }

    @SuppressWarnings("rawtypes")
    protected boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                     Integer cumulativeTimes,
                                                                     Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null) {
            return false;
        }
        if (atomIncrValue == null) {
            atomIncrValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        long fromLong = ((Number) from).longValue();
        long toLong = ((Number) to).longValue();
        int atomIncValLong = ((Number) atomIncrValue).intValue();
        int size = cumulativeTimes;
        if ((toLong - fromLong) > (atomIncValLong * size)) {
            return true;
        } else {
            return false;
        }
    }

    public void processAllPassableFields(Comparative begin, Set<Object> retValue, Integer cumulativeTimes,
                                         Comparable<?> atomicIncreationValue) {
        if (cumulativeTimes == null) {
            throw new IllegalStateException("在没有提供叠加次数的前提下，不能够根据当前范围条件选出对应的定义域的枚举值，sql中不要出现> < >= <=");
        }
        if (atomicIncreationValue == null) {
            atomicIncreationValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        // 把> < 替换为>= <=
        begin = changeGreater2GreaterOrEq(begin);
        begin = changeLess2LessOrEq(begin);

        // long beginInt = (Long) toPrimaryValue(begin.getValue());
        Number beginInt = getNumber(begin.getValue());
        int atomicIncreateValueInt = ((Number) atomicIncreationValue).intValue();
        int comparasion = begin.getComparison();

        if (comparasion == Comparative.GreaterThanOrEqual) {
            for (int i = 0; i < cumulativeTimes; i++) {
                retValue.add(plus(beginInt, atomicIncreateValueInt * i));
            }
        } else if (comparasion == Comparative.LessThanOrEqual) {
            for (int i = 0; i < cumulativeTimes; i++) {
                // 这里可能出现不期望的负数
                Number value = (Number) plus(beginInt, -1 * atomicIncreateValueInt * i);
                if (!isAllowNegative && value.longValue() < 0) {
                    break;
                }
                retValue.add(value);
            }
        }
    }

    protected abstract Number cast2Number(Comparable begin);

    protected abstract Number getNumber(Comparable begin);

    protected abstract Number plus(Number begin, int plus);
}
