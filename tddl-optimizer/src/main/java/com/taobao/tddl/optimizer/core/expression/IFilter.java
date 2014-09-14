package com.taobao.tddl.optimizer.core.expression;

/**
 * 条件表达式
 * 
 * @author jianghang 2013-11-8 下午1:55:54
 * @since 5.0.0
 */
public interface IFilter<RT extends IFilter> extends IFunction<RT> {

    public enum OPERATION {

        AND(0), OR(1), GT(2), LT(3), GT_EQ(4), LT_EQ(5), EQ(6), LIKE(7), IS_NULL(8), IS_NOT_NULL(9), NOT_EQ(10),
        IN(11), IS(12), CONSTANT(13), NULL_SAFE_EQUAL(14), XOR(15), IS_FALSE(16), IS_NOT_FALSE(17), IS_TRUE(18),
        IS_NOT_TRUE(19), EXISTS(20), GROUP_OR(21),
        // ALL
        GT_ALL(30), LT_ALL(31), GT_EQ_ALL(32), LT_EQ_ALL(33), EQ_ALL(34), NOT_EQ_ALL(35),
        // ANY
        GT_ANY(40), LT_ANY(41), GT_EQ_ANY(42), LT_EQ_ANY(43), EQ_ANY(44), NOT_EQ_ANY(45);

        private final int i;

        OPERATION(int i){
            this.i = i;
        }

        public int getValue() {
            return i;
        }

        public static OPERATION valueOf(int i) {
            for (OPERATION op : values()) {
                if (op.i == i) {
                    return op;
                }
            }

            throw new IndexOutOfBoundsException("Invalid ordinal");
        }

        public boolean isAllOp() {
            return i >= OPERATION.GT_ALL.getValue() && i <= OPERATION.NOT_EQ_ALL.getValue();
        }

        public boolean isAnyOp() {
            return i >= OPERATION.GT_ANY.getValue() && i <= OPERATION.NOT_EQ_ANY.getValue();
        }

        @Override
        public String toString() {
            return String.valueOf(i);
        }

        public String getOPERATIONString() {
            switch (this) {
                case AND:
                    return "AND";
                case OR:
                    return "OR";
                case GT:
                    return ">";
                case LT:
                    return "<";
                case IN:
                    return "IN";
                case GT_EQ:
                    return ">=";
                case LT_EQ:
                    return "<=";
                case EQ:
                    return "=";
                case LIKE:
                    return "LIKE";
                case IS_NULL:
                    return "IS NULL";
                case IS:
                    return "IS";
                case IS_NOT_NULL:
                    return "IS NOT NULL";
                case NOT_EQ:
                    return "!=";
                case CONSTANT:
                    return "CONSTANT";
                case NULL_SAFE_EQUAL:
                    return "<=>";
                case XOR:
                    return "XOR";
                case IS_TRUE:
                    return "IS TRUE";
                case IS_NOT_TRUE:
                    return "IS NOT TRUE";
                case IS_FALSE:
                    return "IS FALSE";
                case IS_NOT_FALSE:
                    return "IS NOT FALSE";
                case EXISTS:
                    return "EXISTS";
                case GT_ALL:
                    return "> ALL";
                case LT_ALL:
                    return "< ALL";
                case GT_EQ_ALL:
                    return ">= ALL";
                case LT_EQ_ALL:
                    return "<= ALL";
                case EQ_ALL:
                    return "= ALL";
                case NOT_EQ_ALL:
                    return "!= ALL";
                case GT_ANY:
                    return "> ANY";
                case LT_ANY:
                    return "< ANY";
                case GT_EQ_ANY:
                    return ">= ANY";
                case LT_EQ_ANY:
                    return "<= ANY";
                case EQ_ANY:
                    return "= ANY";
                case NOT_EQ_ANY:
                    return "!= ANY";
                case GROUP_OR:
                    return "OR";
                default:
                    return null;
            }
        }
    }

    public RT setOperation(OPERATION operation);

    public OPERATION getOperation();

}
