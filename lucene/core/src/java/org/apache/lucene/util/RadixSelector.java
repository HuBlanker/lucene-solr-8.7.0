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

import java.util.Arrays;

/** Radix selector.
 *  <p>This implementation works similarly to a MSB radix sort except that it
 *  only recurses into the sub partition that contains the desired value.
 *  // 基数选择算法.
 *  // 大概类似于一个MSB的基数排序.
 *  但是只递归的处理包含有想要的值的子块.
 *  @lucene.internal */
public abstract class RadixSelector extends Selector {

  // after that many levels of recursion we fall back to introselect anyway
  // this is used as a protection against the fact that radix sort performs
  // worse when there are long common prefixes (probably because of cache
  // locality)
  // 递归的层数做一个限制，　保护一下基数选择的性能，无线递归太惨了
  private static final int LEVEL_THRESHOLD = 8;
  // size of histograms: 256 + 1 to indicate that the string is finished
  // 为啥是257呢，因为每次比较是一个字节,一个字节256,再加上1个桶用来表示没这么长的值.
  // 1 111 11
  // 在比较第三位的时候，　就有两个在第0号桶上了. 约等于计数排序.这个桶是用来计数排序的，　总体是为了基数排序打工
  private static final int HISTOGRAM_SIZE = 257;
  // buckets below this size will be sorted with introselect
  // 如果桶的数量小于100,那么使用introselect进行排序了.
  private static final int LENGTH_THRESHOLD = 100;

  // we store one histogram per recursion level
  private final int[] histogram = new int[HISTOGRAM_SIZE];
  private final int[] commonPrefix;

  private final int maxLength;

  /**
   * Sole constructor.
   * @param maxLength the maximum length of keys, pass {@link Integer#MAX_VALUE} if unknown.
   *                  元素的最大长度
   */
  protected RadixSelector(int maxLength) {
    this.maxLength = maxLength;
    this.commonPrefix = new int[Math.min(24, maxLength)];
  }

  /** Return the k-th byte of the entry at index {@code i}, or {@code -1} if
   * its length is less than or equal to {@code k}. This may only be called
   * with a value of {@code i} between {@code 0} included and
   * {@code maxLength} excluded. */
  // 返回i处的元素的第k个字节. 或者不行就返回-1.
  // 只能调用的范围是　[0,maxLength)
  protected abstract int byteAt(int i, int k);

  /** Get a fall-back selector which may assume that the first {@code d} bytes
   *  of all compared strings are equal. This fallback selector is used when
   *  the range becomes narrow or when the maximum level of recursion has
   *  been exceeded. */
  // 获得一个备用的选择器，　该选择其可以假设所有字符车的前d个字节相等.
  // 当范围变窄或者超过了最大的递归级别时，　使用这个备用的选择器.
  // 默认的话是一个快速选择算法的实现
  protected Selector getFallbackSelector(int d) {
    return new IntroSelector() {
      @Override
      protected void swap(int i, int j) {
        RadixSelector.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        for (int o = d; o < maxLength; ++o) {
          final int b1 = byteAt(i, o);
          final int b2 = byteAt(j, o);
          if (b1 != b2) {
            return b1 - b2;
          } else if (b1 == -1) {
            break;
          }
        }
        return 0;
      }

      @Override
      protected void setPivot(int i) {
        pivot.setLength(0);
        for (int o = d; o < maxLength; ++o) {
          final int b = byteAt(i, o);
          if (b == -1) {
            break;
          }
          pivot.append((byte) b);
        }
      }

      @Override
      protected int comparePivot(int j) {
        for (int o = 0; o < pivot.length(); ++o) {
          final int b1 = pivot.byteAt(o) & 0xff;
          final int b2 = byteAt(j, d + o);
          if (b1 != b2) {
            return b1 - b2;
          }
        }
        if (d + pivot.length() == maxLength) {
          return 0;
        }
        return -1 - byteAt(j, d + pivot.length());
      }

      private final BytesRefBuilder pivot = new BytesRefBuilder();
    };
  }

  @Override
  // k, 就是topk的那个k
  public void select(int from, int to, int k) {
    // check
    checkArgs(from, to, k);
    // 在这个范围上比较所有值
    // k 使我们求的topk的k
    // 每个值从第0个字符开始比较
    // 这是第一层的递归，用来检测递归太深了
    select(from, to, k, 0, 0);
  }

  // 又他妈的是递归吗？？？？
   // * @param d the character number to compare
  //　开始比较的字符的编号，也就是index

   // * @param l the level of recursion
  private void select(int from, int to, int k, int d, int l) {
    // 如果数据很少了，或者已经递归的太多了，是不是就换个策略，不要再递归了呢
    // 数据变窄了，超过递归深度了, 就使用备用的选择算法
    if (to - from <= LENGTH_THRESHOLD || d >= LEVEL_THRESHOLD) {
      getFallbackSelector(d).select(from, to, k);
    } else {
      // 继续递归
      radixSelect(from, to, k, d, l);
    }
  }

  /**
   * k 是你要找的第k个
   * @param d the character number to compare, 要开始比较的编号，可能前5位不用比了呢，因为是递归的，所以存在这种情况
   * @param l the level of recursion
   */
  private void radixSelect(int from, int to, int k, int d, int l) {
    // 基数排序、选择使用的直方图
    final int[] histogram = this.histogram;
    // 填充0
    Arrays.fill(histogram, 0);

    // 当前的from->to 的公共前缀长度. 并且当这个方法执行完，所有值的第d位的直方图也统计好了
    final int commonPrefixLength = computeCommonPrefixLengthAndBuildHistogram(from, to, d, histogram);
    // 如果有公共前缀，就说明d位上，一模一样.
    if (commonPrefixLength > 0) {
      // if there are no more chars to compare or if all entries fell into the
      // first bucket (which means strings are shorter than d) then we are done
      // otherwise recurse
      // 如果 d 加上公共前缀小于最大长度，　说明后面还有没搞好的位.
      // 如果全部都在第一个桶，说明所有的值的长度都小于d了，就没必要继续搞了.
      // 那么如果不是全部在第一个桶. 且d加上公共前缀还小于最大长度，说明还有要搞的，那就继续递归
      if (d + commonPrefixLength < maxLength
          && histogram[0] < to - from) {
        radixSelect(from, to, k, d + commonPrefixLength, l);
      }
      return;
    }
    assert assertHistogram(commonPrefixLength, histogram);

    int bucketFrom = from;
    // 每个bucket开始统计咯
    for (int bucket = 0; bucket < HISTOGRAM_SIZE; ++bucket) {
      //
      final int bucketTo = bucketFrom + histogram[bucket];

      if (bucketTo > k) {
        partition(from, to, bucket, bucketFrom, bucketTo, d);

        if (bucket != 0 && d + 1 < maxLength) {
          // all elements in bucket 0 are equal so we only need to recurse if bucket != 0
          select(bucketFrom, bucketTo, k, d + 1, l + 1);
        }
        return;
      }
      bucketFrom = bucketTo;
    }
    throw new AssertionError("Unreachable code");
  }

  // only used from assert
  private boolean assertHistogram(int commonPrefixLength, int[] histogram) {
    int numberOfUniqueBytes = 0;
    for (int freq : histogram) {
      if (freq > 0) {
        numberOfUniqueBytes++;
      }
    }
    if (numberOfUniqueBytes == 1) {
      assert commonPrefixLength >= 1;
    } else {
      assert commonPrefixLength == 0;
    }
    return true;
  }

  /** Return a number for the k-th character between 0 and {@link #HISTOGRAM_SIZE}. */
  // 0-256之间的第k个元素的编号???
  // 为什么要+1呢，因为可能不存在这个字节，不存在返回了-1. 加上１就变成了0.
  // 所以0号位放的是所有没这么长的值的计数
  // 所以这个方法的作用是，获取i位置上的值的第k个字节，在直方图里的下标.也就是在直方图里面的桶的编号.
  private int getBucket(int i, int k) {
    return byteAt(i, k) + 1;
  }

  /** Build a histogram of the number of values per {@link #getBucket(int, int) bucket}
   *  and return a common prefix length for all visited values.
   *  @see #buildHistogram */
  // 构建一个每个桶的值的数量的直方图
  // 返回所有值的公共前缀长度
  // 这里的k变了，　这里的k是每个值从第x位开始比较
  private int computeCommonPrefixLengthAndBuildHistogram(int from, int to, int k, int[] histogram) {
    // 公共前缀
    final int[] commonPrefix = this.commonPrefix;
    // 所以刚开始认为公共前缀的长度，　要么是原有的，要么就是最大长度减去k, 其实就是要比的除了k位之外的，都是公共前缀
    // 这是估计出来的
    int commonPrefixLength = Math.min(commonPrefix.length, maxLength - k);
    for (int j = 0; j < commonPrefixLength; ++j) {
      // 第一个元素的, k+j 个字节
      final int b = byteAt(from, k + j);
      // 公众前缀的数组，在这里其实等于等一个值的k位及以后
      commonPrefix[j] = b;
      // 说明第一个长度不够了.即第一个元素全放进去，还没到你预估的公共前缀长度。
      // 说明你算错了，那么真正的公共前缀长度就是第一个元素的值
      if (b == -1) {
        commonPrefixLength = j + 1;
        break;
      }
    }

    // 所有的事情都是从k位开始的，　因此之前的位都在以前的递归里面解决了。
    // 上面是进行了假设，假设公共前缀是第一个值的k位及以后.
    // 那么公共前缀长度就是k位以后的位数
    // 公共前缀是从k位开始

    int i;
    // 这轮遍历，是数组上的全部遍历
    // 不算第一个数,因为第一个数已经放到公共前缀里面了，大家都和他比较呢.
    // 假设最后算出来的公共前缀是3. 那么说明k->k+3这期间大家都一样.
    outer: for (i = from + 1; i < to; ++i) {
      for (int j = 0; j < commonPrefixLength; ++j) {
        // 这个我看不懂了啊
        // 这里拿到第二个数字的k+0位, k+1位之类
        final int b = byteAt(i, k + j);
        // 去和公共前缀里面比较，公共前缀里面放的是第一个数字的对应的位
        // 等于就好说，继续算公共前缀
        // 不等于的话，就说明有某一个值的某一个位和第一个值的对应位置不一样了，那就不公共了.
        if (b != commonPrefix[j]) {
          // 公共前缀最长也只能有第一个值那么长
          commonPrefixLength = j;
          if (commonPrefixLength == 0) { // we have no common prefix
            // 如果公共前缀长度为0，那就没必要继续后面的操作了，
            // 比如第二个值和第一个值完全不一样，那就说明不会有公共前缀了
            // 如果所有的数字没有公共前缀，那么第一个值的第一位,有i-from个.
            // 比如我计算到第8个的时候，发现大家完全没有公共前缀，但是我既然能到第8个. 说明前面7个值的第一位肯定是一样
            // 那么第一个值的第k位的个数就是7
            histogram[commonPrefix[0] + 1] = i - from;
            // 当前这个值的k为至少也有一个.　我都遍历到这里了，当然有一个了。
            histogram[b + 1] = 1;
            // 跳出，没有公共前缀，不算了
            break outer;
          }
          // 如果公共前缀还不是0，那就说明还有的玩. 就继续下一个值来进行比较，一直到求到了真正的公共前缀
          break;
        }
      }
    }
    // 上面是一个完整的算公共前缀的过程，要么算完，要么知道发现没有公共前缀
    // 在计算的过程中，根据已有的信息，顺手写了一点点直方图. 主要是写了第一位相同的有多少个. 以及在我判断挂掉的那一瞬间，那个值有一个.

    if (i < to) {
      // the loop got broken because there is no common prefix
      // 说明上面的循环是跳出了，所以应该没有公共前缀
      assert commonPrefixLength == 0;
      buildHistogram(i + 1, to, k, histogram);
    } else {
      // 有公共前缀
      assert commonPrefixLength > 0;
      // 只要有公共前缀，那么第一个值的第k位，大家肯定都是一样的了
      // 我就可以写这个第k位这个值，有to-from个一样的.
      histogram[commonPrefix[0] + 1] = to - from;
    }

    // 返回公共前缀的长度
    return commonPrefixLength;
  }

  /** Build an histogram of the k-th characters of values occurring between
   *  offsets {@code from} and {@code to}, using {@link #getBucket}. */
  // 使用getBucket构建在from-to 中间出现的值的第k个字符的直方图
  // 遍历给定的所有范围. 统计所有值的第k位的直方图，就是相同的计数排序那个思想.
  // 比如第k位是2的有８个，这个样子
  private void buildHistogram(int from, int to, int k, int[] histogram) {
    for (int i = from; i < to; ++i) {
      histogram[getBucket(i, k)]++;
    }
  }

  /** Reorder elements so that all of them that fall into {@code bucket} are
   *  between offsets {@code bucketFrom} and {@code bucketTo}. */
  // 重排所有元素，　来让所有在这个桶的元素，都放到from->to之间
  // 值的第d位
  private void partition(int from, int to, int bucket, int bucketFrom, int bucketTo, int d) {
    int left = from;
    int right = to - 1;

    int slot = bucketFrom;

    for (;;) {
      //
      // 第一个值的d位上是啥
      int leftBucket = getBucket(left, d);
      // 最后一个值的d为上是啥
      int rightBucket = getBucket(right, d);

      while (leftBucket <= bucket && left < bucketFrom) {
        if (leftBucket == bucket) {
          swap(left, slot++);
        } else {
          ++left;
        }
        leftBucket = getBucket(left, d);
      }

      while (rightBucket >= bucket && right >= bucketTo) {
        if (rightBucket == bucket) {
          swap(right, slot++);
        } else {
          --right;
        }
        rightBucket = getBucket(right, d);
      }

      if (left < bucketFrom && right >= bucketTo) {
        swap(left++, right--);
      } else {
        assert left == bucketFrom;
        assert right == bucketTo - 1;
        break;
      }
    }
  }
}
