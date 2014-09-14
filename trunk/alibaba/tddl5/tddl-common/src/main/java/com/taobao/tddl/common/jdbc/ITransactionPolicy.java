package com.taobao.tddl.common.jdbc;

public interface ITransactionPolicy {

    public final static Tddl      TDDL                = new Tddl();
    public final static Free      FREE                = new Free();
    public final static AllowRead ALLOW_READ_CROSS_DB = new AllowRead();

    public enum TransactionType {

        /**
         * 允许跨库的读
         */
        ALLOW_READ_CROSS_DB,

        COBAR_STYLE,

        AUTO_COMMIT,
        /**
         * 禁止任跨库操作的事物
         */
        STRICT
    }

    TransactionType getTransactionType(boolean isAutoCommit);

    public class Tddl implements ITransactionPolicy {

        @Override
        public TransactionType getTransactionType(boolean isAutoCommit) {
            if (isAutoCommit) {
                return TransactionType.AUTO_COMMIT;
            }
            return TransactionType.STRICT;
        }

    }

    public class Free implements ITransactionPolicy {

        @Override
        public TransactionType getTransactionType(boolean isAutoCommit) {
            if (isAutoCommit) {

                return TransactionType.AUTO_COMMIT;
            }

            return TransactionType.COBAR_STYLE;

        }
    }

    public class AllowRead implements ITransactionPolicy {

        @Override
        public TransactionType getTransactionType(boolean isAutoCommit) {
            if (isAutoCommit) {
                return TransactionType.AUTO_COMMIT;
            }
            return TransactionType.ALLOW_READ_CROSS_DB;

        }

    }

}
