package com.taobao.tddl.rule.enumerator.handler;

import java.math.BigInteger;

public class BigIntegerPartDiscontinousRangeEnumerator extends NumberPartDiscontinousRangeEnumerator {

    @Override
    protected BigInteger cast2Number(Comparable begin) {
        return (BigInteger) begin;
    }

    @Override
    protected BigInteger getNumber(Comparable begin) {
        return (BigInteger) begin;
    }

    @Override
    protected BigInteger plus(Number begin, int plus) {
        return ((BigInteger) begin).add(BigInteger.valueOf(plus));
    }

    protected boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                     Integer cumulativeTimes,
                                                                     Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null) {
            return false;
        }
        if (atomIncrValue == null) {
            atomIncrValue = DEFAULT_LONG_ATOMIC_VALUE;
        }
        BigInteger fromBig = (BigInteger) cast2Number(from);
        BigInteger toBig = (BigInteger) cast2Number(to);
        int atomIncValLong = ((Number) atomIncrValue).intValue();
        int size = cumulativeTimes;
        if ((toBig.subtract(fromBig).compareTo(BigInteger.valueOf(atomIncValLong * size)) > 0)) {
            return true;
        } else {
            return false;
        }
    }
}
