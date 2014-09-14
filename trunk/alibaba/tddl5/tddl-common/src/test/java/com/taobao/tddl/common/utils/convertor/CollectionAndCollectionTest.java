package com.taobao.tddl.common.utils.convertor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import junit.framework.TestCase;

import org.junit.Test;

import com.taobao.tddl.common.utils.convertor.Convertor;
import com.taobao.tddl.common.utils.convertor.ConvertorHelper;

/**
 * @author jianghang 2011-6-21 下午09:43:46
 */
public class CollectionAndCollectionTest extends TestCase {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testArrayToList() {
        Convertor intList = helper.getConvertor(int[].class, List.class);
        Convertor integerList = helper.getConvertor(Integer[].class, List.class);

        int[] intArray = new int[] { 1, 2 };
        Integer[] integerArray = new Integer[] { 1, 2 };
        List intListValue = (List) intList.convert(intArray, List.class);
        List integerListValue = (List) integerList.convert(integerArray, List.class);
        assertEquals(intListValue.size(), intArray.length);
        assertEquals(intListValue.get(0), intArray[0]);
        assertEquals(integerListValue.size(), integerArray.length);
        assertEquals(integerListValue.get(0), integerArray[0]);
        // 测试不同类型转化, common对象
        List<BigInteger> intListValueOther = (List) intList.convertCollection(intArray, List.class, BigInteger.class); // int强制转为BigInteger
        List<BigDecimal> integerListValueOther = (List) intList.convertCollection(intArray,
            List.class,
            BigDecimal.class); // int强制转为BigDecimal
        assertEquals(intListValueOther.size(), intArray.length);
        assertEquals(intListValueOther.get(0).intValue(), intArray[0]);
        assertEquals(integerListValueOther.size(), integerArray.length);
        assertEquals(integerListValueOther.get(0).intValue(), integerArray[0].intValue());

        // BigDecimal & BigInteger
        Convertor bigDecimalList = helper.getConvertor(BigDecimal[].class, ArrayList.class);
        Convertor bigIntegerList = helper.getConvertor(BigInteger[].class, Vector.class);

        BigDecimal[] bigDecimalArray = new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ONE };
        BigInteger[] bigIntegerArray = new BigInteger[] { BigInteger.ZERO, BigInteger.ONE };
        List bigDecimalListValue = (List) bigDecimalList.convert(bigDecimalArray, ArrayList.class);
        List bigIntegerListValue = (List) bigIntegerList.convert(bigIntegerArray, Vector.class);
        assertEquals(bigDecimalListValue.size(), bigDecimalArray.length);
        assertEquals(bigDecimalListValue.get(0), bigDecimalArray[0]);
        assertEquals(bigIntegerListValue.size(), bigIntegerArray.length);
        assertEquals(bigIntegerListValue.get(0), bigIntegerArray[0]);

    }

    @Test
    public void testArrayAndSet() {
        Convertor intSet = helper.getConvertor(int[].class, Set.class);
        Convertor integerSet = helper.getConvertor(Integer[].class, Set.class);

        int[] intArray = new int[] { 1, 2 };
        Integer[] integerArray = new Integer[] { 1, 2 };
        Set intSetValue = (Set) intSet.convert(intArray, Set.class);
        Set integerSetValue = (Set) integerSet.convert(integerArray, Set.class);
        assertEquals(intSetValue.size(), intArray.length);
        assertEquals(intSetValue.iterator().next(), intArray[0]);
        assertEquals(integerSetValue.size(), integerArray.length);
        assertEquals(integerSetValue.iterator().next(), integerArray[0]);
        // 测试不同类型转化, common对象
        Set<BigInteger> intSetValueOther = (Set) intSet.convertCollection(intArray, Set.class, BigInteger.class); // int强制转为BigInteger
        Set<BigDecimal> integerSetValueOther = (Set) integerSet.convertCollection(intArray, Set.class, BigDecimal.class); // int强制转为BigDecimal
        assertEquals(intSetValueOther.size(), intArray.length);
        assertEquals(intSetValueOther.iterator().next().intValue(), intArray[0]);
        assertEquals(integerSetValueOther.size(), integerArray.length);

        // BigDecimal & BigInteger
        Convertor bigDecimalSet = helper.getConvertor(BigDecimal[].class, HashSet.class);
        Convertor bigIntegerSet = helper.getConvertor(BigInteger[].class, LinkedHashSet.class);

        BigDecimal[] bigDecimalArray = new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ONE };
        BigInteger[] bigIntegerArray = new BigInteger[] { BigInteger.ZERO, BigInteger.ONE };
        Set bigDecimalSetValue = (Set) bigDecimalSet.convert(bigDecimalArray, HashSet.class);
        Set bigIntegerSetValue = (Set) bigIntegerSet.convert(bigIntegerArray, LinkedHashSet.class);
        assertEquals(bigDecimalSetValue.size(), bigDecimalArray.length);
        assertEquals(bigDecimalSetValue.iterator().next(), bigDecimalArray[0]);
        assertEquals(bigIntegerSetValue.size(), bigIntegerArray.length);
        assertEquals(bigIntegerSetValue.iterator().next(), bigIntegerArray[0]);

    }

    @Test
    public void testCollectionToArray() {
        // 进行List -> Array处理
        List<Integer> intListValue = Arrays.asList(1);
        // 测试不同类型转化, common对象
        Convertor intList = helper.getConvertor(List.class, int[].class);
        Convertor integerList = helper.getConvertor(List.class, Integer[].class);
        int[] intArray = (int[]) intList.convert(intListValue, int[].class);
        Integer[] integerArray = (Integer[]) integerList.convert(intListValue, Integer[].class);
        assertEquals(intListValue.size(), intArray.length);
        assertEquals(intListValue.get(0).intValue(), intArray[0]);
        assertEquals(intListValue.size(), integerArray.length);
        assertEquals(intListValue.get(0), integerArray[0]);
        // 测试不同类型转化, common对象
        BigInteger[] bigIntegerValueOther = (BigInteger[]) intList.convertCollection(intListValue,
            BigInteger[].class,
            BigInteger.class); // int强制转为BigInteger
        BigDecimal[] bigDecimalValueOther = (BigDecimal[]) intList.convertCollection(intListValue,
            BigDecimal[].class,
            BigDecimal.class); // int强制转为BigDecimal
        assertEquals(bigIntegerValueOther.length, intListValue.size());
        assertEquals(bigIntegerValueOther[0].intValue(), intListValue.get(0).intValue());
        assertEquals(bigDecimalValueOther.length, intListValue.size());
        assertEquals(bigDecimalValueOther[0].intValue(), intListValue.get(0).intValue());

        // BigDecimal & BigInteger
        Convertor bigDecimalSet = helper.getConvertor(List.class, BigDecimal[].class);
        Convertor bigIntegerSet = helper.getConvertor(List.class, BigInteger[].class);

        List<BigDecimal> bigDecimalList = Arrays.asList(BigDecimal.ONE);
        List<BigInteger> bigIntegerList = Arrays.asList(BigInteger.ONE);
        BigDecimal[] bigDecimalArrayValue = (BigDecimal[]) bigDecimalSet.convert(bigDecimalList, BigDecimal[].class);
        BigInteger[] bigIntegerArrayValue = (BigInteger[]) bigIntegerSet.convert(bigIntegerList, BigInteger[].class);
        assertEquals(bigDecimalArrayValue.length, bigDecimalList.size());
        assertEquals(bigDecimalArrayValue[0].intValue(), bigDecimalList.get(0).intValue());
        assertEquals(bigIntegerArrayValue.length, bigIntegerList.size());
        assertEquals(bigIntegerArrayValue[0].intValue(), bigIntegerList.get(0).intValue());

    }

    @Test
    public void testCollectionAndCollection() {
        Convertor intSet = helper.getConvertor(List.class, Set.class);

        List intList = Arrays.asList(1);
        Set intSetValue = (Set) intSet.convert(intList, Set.class);
        assertEquals(intSetValue.size(), intList.size());
        assertEquals(intSetValue.iterator().next(), intList.get(0));
        // 测试不同类型转化, common对象
        Set<BigInteger> intSetValueOther = (Set) intSet.convertCollection(intList, Set.class, BigInteger.class); // int强制转为BigInteger
        Set<BigDecimal> decimalSetValueOther = (Set) intSet.convertCollection(intList, Set.class, BigDecimal.class); // int强制转为BigDecimal
        assertEquals(intSetValueOther.size(), intList.size());
        assertEquals(intSetValueOther.iterator().next().intValue(), intList.get(0));
        assertEquals(decimalSetValueOther.size(), intList.size());
        assertEquals(decimalSetValueOther.iterator().next().intValue(), intList.size());

        // BigDecimal & BigInteger
        Convertor bigDecimalSet = helper.getConvertor(List.class, HashSet.class);
        Convertor bigIntegerSet = helper.getConvertor(List.class, LinkedHashSet.class);

        List bigDecimalList = Arrays.asList(BigDecimal.ONE);
        List bigIntegerList = Arrays.asList(BigInteger.ONE);
        Set bigDecimalSetValue = (Set) bigDecimalSet.convert(bigDecimalList, HashSet.class);
        Set bigIntegerSetValue = (Set) bigIntegerSet.convert(bigIntegerList, LinkedHashSet.class);
        assertEquals(bigDecimalSetValue.size(), bigDecimalList.size());
        assertEquals(bigDecimalSetValue.iterator().next(), bigDecimalList.get(0));
        assertEquals(bigIntegerSetValue.size(), bigIntegerList.size());
        assertEquals(bigIntegerSetValue.iterator().next(), bigIntegerList.get(0));
    }

}
