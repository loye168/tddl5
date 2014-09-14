package com.taobao.tddl.rule.model;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.taobao.tddl.rule.Rule.RuleColumn;
import com.taobao.tddl.rule.utils.AdvancedParameterParser;

/**
 * 通过{@linkplain AdvancedParameterParser}.getAdvancedParamByParamTokenNew()进行构建
 */
public class AdvancedParameter extends RuleColumn {

    /**
     * 自增，给枚举器用的
     */
    public final Comparable<?>    atomicIncreateValue;

    /**
     * 叠加次数，给枚举器用的
     */
    public final Integer          cumulativeTimes;

    /**
     * 决定当前参数是否允许范围查询如>= <= ...
     */
    public final boolean          needMergeValueInCloseInterval;

    /**
     * 自增的类型，包括
     */
    public final AtomIncreaseType atomicIncreateType;

    /**
     * 起始与结束值对象列表，通过"|"分割
     */
    public final Range[]          rangeArray;

    public AdvancedParameter(String key, Comparable<?> atomicIncreateValue, Integer cumulativeTimes,
                             boolean needAppear, AtomIncreaseType atomicIncreateType, Range[] rangeObjectArray){
        super(key, needAppear);
        this.atomicIncreateValue = atomicIncreateValue;
        this.atomicIncreateType = atomicIncreateType;
        this.cumulativeTimes = cumulativeTimes;
        this.rangeArray = rangeObjectArray;

        if (atomicIncreateValue != null) {
            this.needMergeValueInCloseInterval = true;
        } else {
            this.needMergeValueInCloseInterval = false;
        }
    }

    /**
     * 枚举所有值
     * 
     * @param basepoint
     * @return
     */
    public Set<Object> enumerateRange() {
        Set<Object> values = new HashSet<Object>();
        if (atomicIncreateType.isTime()) {
            Calendar c = Calendar.getInstance();
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    values.add(evalTime(c, i));
                }

                // 特别处理下，如果补偿不为1的情况
                if (atomicIncreateValue instanceof Number && ((Number) atomicIncreateValue).longValue() > 1) {
                    long incV = ((Number) atomicIncreateValue).longValue();
                    for (int i = ro.start; i <= ro.end; i++) {
                        values.add(evalTime(c, (int) (i * incV)));
                    }
                }
            }
        } else if (AtomIncreaseType.STRING.equals(atomicIncreateType)) {
            // 以前string类型，常见的写法会是 #name,1,1024#.toString().hashcode() % 1024
            // 问题就处在toString()方法，如果是只添加Integer类型的数字，toString之后的hashcode就不是联系了.
            // 要么去掉.toString(),要么就是改为1_string类型
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    // Character.MAX_VALUE , 16位,最大值为65536，一般很少有这么多分表数
                    // 如果真出现了>65536，那范围枚举的值可能就不准了
                    values.add(String.valueOf((char) i));
                }
            }
        } else {
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    values.add(i);
                }

                // 特别处理下，如果补偿不为1的情况
                if (atomicIncreateValue instanceof Number && ((Number) atomicIncreateValue).longValue() > 1) {
                    long incV = ((Number) atomicIncreateValue).longValue();
                    for (int i = ro.start; i <= ro.end; i++) {
                        values.add(i * incV);
                    }
                }
            }
        }
        return values;
    }

    /**
     * 枚举所有值
     * 
     * @param basepoint
     * @return
     */
    public Set<Object> enumerateRange(Object basepoint) {
        if (basepoint instanceof Number) {
            return enumerateRange(((Number) basepoint).intValue());
        } else if (basepoint instanceof String) {
            // 目前该方法只是在初始化拓扑的时候会用到，如果出现string，一定是配置了1_string，拿第一个char进行处理
            return enumerateRange(((String) basepoint).charAt(0));
        } else if (basepoint instanceof Calendar) {
            return enumerateRange((Calendar) basepoint);
        } else if (basepoint instanceof Date) {
            // add by junyu,因为后面evalTime的时候把结果返回了Date类型，所以这边也要增加这个逻辑
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date) basepoint);
            return enumerateRange(cal);
        } else {
            throw new IllegalArgumentException(basepoint + " applies on atomicIncreateType: " + atomicIncreateType);
        }
    }

    /**
     * 枚举所有值
     * 
     * @param basepoint
     * @return
     */
    public Set<Object> enumerateRange(int basepoint) {
        Set<Object> values = new HashSet<Object>();
        if (AtomIncreaseType.NUMBER.equals(atomicIncreateType)) {
            int start = basepoint;
            int end = start + this.cumulativeTimes;
            for (int i = start; i <= end; i++) {
                values.add(i);
            }
        } else if (AtomIncreaseType.STRING.equals(atomicIncreateType)) {
            int start = basepoint;
            int end = start + this.cumulativeTimes;
            for (int i = start; i <= end; i++) {
                values.add(String.valueOf((char) i));
            }
        } else {
            throw new IllegalArgumentException("Number applies on atomicIncreateType: " + atomicIncreateType);
        }
        return values;
    }

    /**
     * 枚举所有值
     * 
     * @param basepoint
     * @return
     */
    public Set<Object> enumerateRange(Calendar basepoint) {
        Set<Object> values = new HashSet<Object>();
        if (atomicIncreateType.isTime()) {
            for (int i = 0; i < this.cumulativeTimes; i++) {
                values.add(evalTime(basepoint, i));
            }
        } else {
            throw new IllegalArgumentException("Calendar applies on atomicIncreateType: " + atomicIncreateType);
        }
        return values;
    }

    private Object evalTime(Calendar base, int i) {
        Calendar c = (Calendar) base.clone();
        if (AtomIncreaseType.YEAR.equals(atomicIncreateType)) {
            c.add(Calendar.YEAR, i);
        } else if (AtomIncreaseType.MONTH.equals(atomicIncreateType)) {
            c.add(Calendar.MONTH, i);
        } else if (AtomIncreaseType.DATE.equals(atomicIncreateType)) {
            c.add(Calendar.DATE, i);
        } else if (AtomIncreaseType.HOUR.equals(atomicIncreateType)) {
            c.add(Calendar.HOUR_OF_DAY, i);
        } else {
            throw new IllegalArgumentException("atomicIncreateType:" + atomicIncreateType);
        }
        // return c;
        // modify by junyu,与sql参数保持一致类型
        return c.getTime();
    }

    /**
     * 参数自增类型，现在支持4种(#2011-12-5,modify by junyu,add HOUR type)
     */
    public static enum AtomIncreaseType {
        HOUR, DATE, MONTH, YEAR, NUMBER, STRING;

        public boolean isTime() {
            return this.ordinal() < NUMBER.ordinal();
        }
    }

    public static class Range {

        public final Integer start; // 起始值
        public final Integer end;  // 结束值

        public Range(Integer start, Integer end){
            this.start = start;
            this.end = end;
        }
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
