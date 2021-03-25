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

import java.util.Comparator;

/** Implementation of the quick select algorithm.
 *  <p>It uses the median of the first, middle and last values as a pivot and
 *  falls back to a median of medians when the number of recursion levels exceeds
 *  {@code 2 lg(n)}, as a consequence it runs in linear time on average.</p>
 *  @lucene.internal */
// 快速选择算法的实现
// 他使用第一个，中间，最后一个值的中位数作为分割点，　
// 当递归的数量超过2log(n)的时候，会退回到中位数的中位数 !!!!! 这是一种保底的策略，防止找分割点耽误太久
  // 因此，　这个算法平均在线性时间内运行.
  // pivot 这个词语，一般是快速排序的那个分割点的名字，所以其实也是选择分割点咯
  /*
  看明白了后的注释:
  主要使用的是,三者(头尾中间)中位数来作为分隔点.但是这种方法有极小的可能变成最坏的复杂度.
  那么处理方法是, 如果递归了 2* lg(n)次. 还没结束, 之后找主元也就是分隔点, 就不用这个方法了.
  用bfptr方法. 也就是中位数的中位数法.
  为啥不一直用这个方法呢. 就是这个方法理论上不会有最坏的情况. 都是O(n). 但是常数可能比较大. 也不好一直用这个.
   */

public abstract class IntroSelector extends Selector {

  @Override
  public final void select(int from, int to, int k) {
    checkArgs(from, to, k);
    // 递归的最大深度
    final int maxDepth = 2 * MathUtil.log(to - from, 2);
    quickSelect(from, to, k, maxDepth);
  }

  // 慢的方法，也是保底方法，如果别的方法递归太久了，就终止掉，走这个.
  int slowSelect(int from, int to, int k) {
    return medianOfMediansSelect(from, to-1, k);
  }

  // 这玩意又是一个递归的方法
  int medianOfMediansSelect(int left, int right, int k) {
    // 这里是一个递归的左右某一边来回找的过程. 最终找到的一个是left=k
    do {
      // Defensive check, this is also checked in the calling
      // method. Including here so this method can be used
      // as a self contained quickSelect implementation.
      // 保守点check一下
      if (left == right) {
        return left;
      }

      // 这个值是当前的通过中位数的中位数找到的分隔点.
      int pivotIndex = pivot(left, right);
      // 这是实际操作后的分隔点
      pivotIndex = partition(left, right, k, pivotIndex);
      // 如果分隔点正好是k, 那就是找到了
      if (k == pivotIndex) {
        return k;
      } else if (k < pivotIndex) {
        right = pivotIndex-1;
      } else {
        left = pivotIndex+1;
      }
    } while (left != right);
    return left;
  }

  // 进行了实际的左右分区操作
  private int partition(int left, int right, int k, int pivotIndex) {
    // 以这个当做分隔点
    setPivot(pivotIndex);
    // 分隔点放到最右边
    swap(pivotIndex, right);
    int storeIndex = left;
    for (int i = left; i < right; i++) {
      if (comparePivot(i) > 0) {
        swap(storeIndex, i);
        storeIndex++;
      }
    }
    int storeIndexEq = storeIndex;
    for (int i = storeIndex; i < right; i++) {
      if (comparePivot(i) == 0) {
        swap(storeIndexEq, i);
        storeIndexEq++;
      }
    }
    swap(right, storeIndexEq);
    if (k < storeIndex) {
      return storeIndex;
    } else if (k <= storeIndexEq) {
      return k;
    }
    return storeIndexEq;
  }

  // 求分割点
  private int pivot(int left, int right) {
    // 左右范围在5以内的话, 插入排序搞一下，就找到中间点了
    // 这里又是一个递归的结束条件
    if (right - left < 5) {
      int pivotIndex = partition5(left, right);
      return pivotIndex;
    }

    for (int i = left; i <= right; i=i+5) {
      int subRight = i + 4;
      if (subRight > right) {
        subRight = right;
      }
      // 这是左边的值和每5个的中位数
      int median5 = partition5(i, subRight);
      // 把这次的中位数和xx交换一下
      swap(median5, left + ((i-left)/5));
    }
    // 上面的东西搞完之后, left之后的几个, 全是原来的中位数
    int mid = ((right - left) / 10) + left + 1;
    // 这里算的就是这一轮里,一共有多少个中位数,因为是递归的过程嘛,所以这里每次除以5的速度往下少的.
    int to = left + ((right - left)/5);
    return medianOfMediansSelect(left, to, mid);
  }

  // selects the median of a group of at most five elements,
  // implemented using insertion sort. Efficient due to
  // bounded nature of data set.
  // 在最多５个值里面求中位数，使用了插入排序法。由于数据集的自然边界，所以这个算法还是很高效的呢
  private int partition5(int left, int right) {
    int i = left + 1;
    while( i <= right) {
      int j = i;
      while (j > left && compare(j-1,j)>0) {
        swap(j-1, j);
        j--;
      }
      i++;
    }
    return (left + right) >>> 1;
  }

  private void quickSelect(int from, int to, int k, int maxDepth) {
    // check
    assert from <= k;
    assert k < to;
    // 一个节点，肯定满足的呢
    if (to - from == 1) {
      return;
    }

    // 最大深度-1
    // 最大深度为0,也就是说,当前已经到了不能使用三者中位数的临界点了. 要使用稳妥的可能慢的方法.也就是中位数的中位数,是bfptr方法.
    if (--maxDepth < 0) {
      slowSelect(from, to, k);
      return;
    }

    // 中间的那个
    final int mid = (from + to) >>> 1;
    // heuristic: we use the median of the values at from, to-1 and mid as a pivot
    // 如果第一个大于最后一个. 交换
    if (compare(from, to - 1) > 0) {
      swap(from, to - 1);
    }
    // 中间这个和to-1, 谁大谁放在中间.
    if (compare(to - 1, mid) > 0) {
      swap(to - 1, mid);
      // 左右两个放对位置
      if (compare(from, to - 1) > 0) {
        swap(from, to - 1);
      }
    }
    // 到这里, to-1上放着的是三者中位数.
    setPivot(to - 1);

    // 左右, 快排
    int left = from + 1;
    int right = to - 2;

    for (;;) {
      while (comparePivot(left) > 0) {
        ++left;
      }

      while (left < right && comparePivot(right) <= 0) {
        --right;
      }

      if (left < right) {
        swap(left, right);
        --right;
      } else {
        break;
      }
    }
    swap(left, to - 1);

    // 递归的进行快排
    if (left == k) {
      return;
    } else if (left < k) {
      quickSelect(left + 1, to, k, maxDepth);
    } else {
      quickSelect(from, left, k, maxDepth);
    }
  }

  /** Compare entries found in slots <code>i</code> and <code>j</code>.
   *  The contract for the returned value is the same as
   *  {@link Comparator#compare(Object, Object)}. */
  // 比较一下两个槽的数据的大小
  protected int compare(int i, int j) {
    setPivot(i);
    return comparePivot(j);
  }

  /** Save the value at slot <code>i</code> so that it can later be used as a
   * pivot, see {@link #comparePivot(int)}. */
  // 把i当做中间点,就可以和j比较了
  protected abstract void setPivot(int i);

  /** Compare the pivot with the slot at <code>j</code>, similarly to
   *  {@link #compare(int, int) compare(i, j)}. */
  // 可以和i比较
  // 其实这两个方法配合起来compare(int,int), so boring
  protected abstract int comparePivot(int j);
}
