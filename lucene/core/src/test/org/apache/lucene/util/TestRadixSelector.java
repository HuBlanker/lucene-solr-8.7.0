/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.junit.Test;

public class TestRadixSelector extends LuceneTestCase {

  public void testSelect() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSelect();
    }
  }

  private void doTestSelect() {
    final int from = random().nextInt(5);
    final int to = from + TestUtil.nextInt(random(), 1, 10000);
    final int maxLen = TestUtil.nextInt(random(), 1, 12);
    BytesRef[] arr = new BytesRef[from + to + random().nextInt(5)];
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[TestUtil.nextInt(random(), 0, maxLen)];
      random().nextBytes(bytes);
      arr[i] = new BytesRef(bytes);
    }
    doTest(arr, from, to, maxLen);
  }

  public void testSharedPrefixes() {
    for (int iter = 0; iter < 100; ++iter) {
      doTestSharedPrefixes();
    }
  }

  private void doTestSharedPrefixes() {
    final int from = random().nextInt(5);
    final int to = from + TestUtil.nextInt(random(), 1, 10000);
    final int maxLen = TestUtil.nextInt(random(), 1, 12);
    BytesRef[] arr = new BytesRef[from + to + random().nextInt(5)];
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[TestUtil.nextInt(random(), 0, maxLen)];
      random().nextBytes(bytes);
      arr[i] = new BytesRef(bytes);
    }
    final int sharedPrefixLength = Math.min(arr[0].length, TestUtil.nextInt(random(), 1, maxLen));
    for (int i = 1; i < arr.length; ++i) {
      System.arraycopy(arr[0].bytes, arr[0].offset, arr[i].bytes, arr[i].offset, Math.min(sharedPrefixLength, arr[i].length));
    }
    doTest(arr, from, to, maxLen);
  }

  private void doTest(BytesRef[] arr, int from, int to, int maxLen) {
    final int k = TestUtil.nextInt(random(), from, to - 1);

    BytesRef[] expected = arr.clone();
    Arrays.sort(expected, from, to);

    BytesRef[] actual = arr.clone();
    final int enforcedMaxLen = random().nextBoolean() ? maxLen : Integer.MAX_VALUE;
    RadixSelector selector = new RadixSelector(enforcedMaxLen) {

      @Override
      protected void swap(int i, int j) {
        ArrayUtil.swap(actual, i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        assertTrue(k < enforcedMaxLen);
        BytesRef b = actual[i];
        if (k >= b.length) {
          return -1;
        } else {
          return Byte.toUnsignedInt(b.bytes[b.offset + k]);
        }
      }

    };
    selector.select(from, to, k);

    assertEquals(expected[k], actual[k]);
    for (int i = 0; i < actual.length; ++i) {
      if (i < from || i >= to) {
        assertSame(arr[i], actual[i]);
      } else if (i <= k) {
        assertTrue(actual[i].compareTo(actual[k]) <= 0);
      } else {
        assertTrue(actual[i].compareTo(actual[k]) >= 0);
      }
    }
  }

  public void testHuyanSelect() {
    BytesRef[] arr = new BytesRef[110];
    int max = Integer.MAX_VALUE;
    for (int i = 0; i < 110; i++) {
      byte[] bytes = new byte[1];
      bytes[0] = (byte) (110 - i);
      max = Math.min(max, bytes.length);
      arr[i] = new BytesRef(bytes);
    }
    System.out.println("max = " + max);
    System.out.println("===============");
    for (int i = 0; i < arr.length; i++) {
      System.out.println(i + "\t" + (int) arr[i].bytes[0]);
    }

    new TestHuyanRadixSelector(1, arr).select(0, 110, 15);

    System.out.println("===============");
    for (int i = 0; i < arr.length; i++) {
      System.out.println(i + "\t" + (int) arr[i].bytes[0]);
    }

    System.out.println("===============");
    System.out.println((int) arr[15].bytes[0]);

  }


  public static class TestHuyanRadixSelector extends RadixSelector {

    // 字节数组的数组，对吧，相当于每一个元素都是一个字节数组呢
    BytesRef[] actual;

    public TestHuyanRadixSelector(int maxLength, BytesRef[] actual) {
      super(maxLength);
      this.actual = actual;
    }

    protected TestHuyanRadixSelector(int maxLength) {
      super(maxLength);
    }

    @Override
    protected void swap(int i, int j) {
      ArrayUtil.swap(actual, i, j);
    }

    @Override
    protected int byteAt(int i, int k) {
      BytesRef b = actual[i];
      if (k >= b.length) {
        return -1;
      } else {
        return Byte.toUnsignedInt(b.bytes[b.offset + k]);
      }
    }
  }

  public static class TestHuyanRadixSelector2 extends RadixSelector2 {

    // 字节数组的数组，对吧，相当于每一个元素都是一个字节数组呢
    BytesRef[] actual;

    public TestHuyanRadixSelector2(int maxLength, BytesRef[] actual) {
      super(maxLength);
      this.actual = actual;
    }

    protected TestHuyanRadixSelector2(int maxLength) {
      super(maxLength);
    }

    @Override
    protected void swap(int i, int j) {
      ArrayUtil.swap(actual, i, j);
    }

    @Override
    protected int byteAt(int i, int k) {
      BytesRef b = actual[i];
      if (k >= b.length) {
        return -1;
      } else {
        return Byte.toUnsignedInt(b.bytes[b.offset + k]);
      }
    }
  }

  @Test
  public void testWhenAllLongCommonPrefix() {
    // 10 common
    int from = 0;
    int to = 1000000;
    int k = 300000;

    BytesRef[] arr = new BytesRef[to];
    byte[] commonPrefix = new byte[10];
    random().nextBytes(commonPrefix);
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[12];
      System.arraycopy(commonPrefix, 0, bytes, 0, 10);
      bytes[10] = (byte) random().nextInt();
      bytes[11] = (byte) random().nextInt();
      arr[i] = new BytesRef(bytes);
    }
    new TestHuyanRadixSelector(12, arr).select(from, to, k);
  }

  public static final int[] length_ = new int[]{8, 15, 16, 31, 32};
  public static final int[] nums = new int[]{100000, 1000000, 10000000, 100000000};

  // 定长的话
  @Test
  public void perfBugTest() {
    // some warm
    for (int i = 0; i < 100; i++) {
      epoch(i + 1, 100000);
    }

    // 数量
    for (int num : nums) {
      // do  1000times then avg min max p99
      // 1 10w
      System.out.println("==========分割线==========");
      System.out.println("num=" + num + " | " + "avg1 | min1 | max1 | t991 | avg2 | min2 | max2 | t992 | new-agv/old");
      System.out.println("--- | --- | --- | --- | --- | --- | --- | --- | --- | ---  | ---");


      // 长度
      for (int length : length_) {
        long[] times = new long[1000];
        long[] times2 = new long[1000];
        // times
        for (int i = 0; i < 1000; i++) {
          long[] epoch = epoch(length, num);
          times[i] = epoch[0];
          times2[i] = epoch[1];
        }
        System.out.println(length + " | " + metrics(times, times2));
      }
    }
  }

  public String metrics(long[] times1, long[] times2) {
    StringBuilder sb = new StringBuilder();
    double avg1 = getString(times1, sb);
    sb.append(" | ");
    double avg2 = getString(times2, sb);
    return sb.append(" | ").append(avg2 / avg1).toString();
  }

  private double getString(long[] times, StringBuilder sb) {
    int t99Idx = times.length / 100 * 99;
    long min = Integer.MAX_VALUE;
    long max = 0;
    long t99 = -1;
    long all = 0;

    Arrays.sort(times);

    for (int i = 0; i < times.length; i++) {
      min = Math.min(min, times[i]);
      max = Math.max(max, times[i]);
      if (i == t99Idx) t99 = times[i];
      all += times[i];
    }


    double avg = all * 1.0 / times.length;
    String format = String.format("%.03f |  %d |  %d |  %d", avg,
        min, max, t99);
    sb.append(format);
    return avg;
  }

  // k is random
  public long[] epoch(int length, int num) {
    long[] res = new long[2];
    int from = 0;
    int to = num;
    final int k = TestUtil.nextInt(random(), from, to - 1);

    BytesRef[] arr = new BytesRef[to];
    for (int i = 0; i < arr.length; ++i) {
      byte[] bytes = new byte[length];
      random().nextBytes(bytes);
      arr[i] = new BytesRef(bytes);
    }

    // check , so we sort it
//    BytesRef[] expected = arr.clone();
//    Arrays.sort(expected, from, to);


    BytesRef[] a1 = arr.clone();
    long before = System.nanoTime();
    new TestHuyanRadixSelector(length, a1).select(from, to, k);
    long cost = System.nanoTime() - before;
    res[0] = cost;

    BytesRef[] a2 = arr.clone();
    long before2 = System.nanoTime();
    new TestHuyanRadixSelector2(length, a2).select(from, to, k);
    long cost2 = System.nanoTime() - before2;
    res[1] = cost2;


     // no check
//    assertEquals(expected[k], a1[k]);
//    for (int i = 0; i < arr.length; ++i) {
//      if (i < from || i >= to) {
//        assertSame(a1[i], a1[i]);
//      } else if (i <= k) {
//        assertTrue(a1[i].compareTo(a1[k]) <= 0);
//      } else {
//        assertTrue(a1[i].compareTo(a1[k]) >= 0);
//      }
//    }
//
//    assertEquals(expected[k], a2[k]);
//    for (int i = 0; i < arr.length; ++i) {
//      if (i < from || i >= to) {
//        assertSame(a2[i], a2[i]);
//      } else if (i <= k) {
//        assertTrue(a2[i].compareTo(a2[k]) <= 0);
//      } else {
//        assertTrue(a2[i].compareTo(a2[k]) >= 0);
//      }
//    }
    return res;
  }

}
