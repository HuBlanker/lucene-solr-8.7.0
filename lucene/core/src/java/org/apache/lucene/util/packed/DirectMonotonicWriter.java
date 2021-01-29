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
package org.apache.lucene.util.packed;


import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * Write monotonically-increasing sequences of integers. This writer splits
 * data into blocks and then for each block, computes the average slope, the
 * minimum value and only encode the delta from the expected value using a
 * {@link DirectWriter}.
 * <p>
 * average slope 还真是平均斜率
 * <p>
 * 写入单调递增的整数序列
 * 这个类把数据分成块，　然后对每个块计算平均斜率，最小值，然后只使用 DirectWriter来 编码 期望值的delta.
 * <p>
 * <br/>
 * <br/>
 *
 *
 * <B>单调递增的数组写入，对单调递增的数据进行了增量编码，比如[100,101,102,103], 可以编码成[100,1,1,1] 每一个数是前一个数的增量。
 * 这样有个好处，就是数字全部变小了，</B>
 * <p>
 * <br/>
 * <br/>
 *
 * <B> 配合 DirectWriter　的数字越小压缩率越高，可以有效的压缩存储单调递增数组,
 * 比如很合适的就是文件偏移量，因为文件一直写，一直增多，符合单调递增。</B>
 *
 * @lucene.internal
 * @see DirectMonotonicReader
 */
public final class DirectMonotonicWriter {

  // 一块有多少个int,　这里是 2的shift次方个
  public static final int MIN_BLOCK_SHIFT = 2;
  public static final int MAX_BLOCK_SHIFT = 22;

  // 这个类，　其实不知道是为了谁写
  // 但是仍然不妨碍一个记录元数据，一个记录真正的数据，
  // 写field信息可以用，其他的docValue之类的也可以
  final IndexOutput meta;
  final IndexOutput data;

  // 总数, 不区分chunk,block等等，对于这个类来说，就是你想要我写多少个。
  final long numValues;

  // data文件初始化的时候的文件写入地址.
  final long baseDataPointer;

  // 内部缓冲区
  final long[] buffer;
  // 当前已经buffer了多少个
  int bufferSize;
  // 总数计数，bufferSize会被清除的
  long count;
  boolean finished;

  DirectMonotonicWriter(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    if (blockShift < MIN_BLOCK_SHIFT || blockShift > MAX_BLOCK_SHIFT) {
      throw new IllegalArgumentException("blockShift must be in [" + MIN_BLOCK_SHIFT + "-" + MAX_BLOCK_SHIFT + "], got " + blockShift);
    }
    if (numValues < 0) {
      throw new IllegalArgumentException("numValues can't be negative, got " + numValues);
    }


    // 根据总数，以及每块的数据，来算总共需要的块的数量。　算法约等于，总数 / (2 ^ blockShift);
    // 这里只是校验一下这两个数字的合法性，实际限制在
    final long numBlocks = numValues == 0 ? 0 : ((numValues - 1) >>> blockShift) + 1;
    if (numBlocks > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("blockShift is too low for the provided number of values: blockShift=" + blockShift +
          ", numValues=" + numValues + ", MAX_ARRAY_LENGTH=" + ArrayUtil.MAX_ARRAY_LENGTH);
    }
    this.meta = metaOut;
    this.data = dataOut;
    this.numValues = numValues;
    // blockSize算到了，　然后缓冲区的大小就是blockSize或者极限情况下很少，就是numValues.
    final int blockSize = 1 << blockShift;
    this.buffer = new long[(int) Math.min(numValues, blockSize)];
    this.bufferSize = 0;
    this.baseDataPointer = dataOut.getFilePointer();
  }


  /**
   * // 一个块满了，或者最终调用finish了，就写一次
   * <br/>
   * <br/>
   * <b>计算方法终于搞明白了，存储一个单调递增数组，要存储斜率，最小值，以及delta，再加上index就可以算出来</b>
   * 举例 [100,101,108] 经过计算之后存储的[3,0,3], 斜率4.0. 最小值97.
   * 开始计算：
   * 1. 100 = 97 + 3 + 0 * 4.0
   * 2. 101 = 97 + 0 + 1 * 4.0
   * 3. 108 = 97 + 3 + 2 * 4.0
   * 完美
   * <br/>
   * <br/>
   * 一个block，这么搞一下
   *
   * @throws IOException
   */
  private void flush() throws IOException {
    assert bufferSize != 0;

    // 斜率算法, 最大减去最小除以个数，常见算法
    final float avgInc = (float) ((double) (buffer[bufferSize - 1] - buffer[0]) / Math.max(1, bufferSize - 1));

    // 根据斜率，算出当前位置上的数字，比按照斜率算出来的数字，多了多少或者小了多少，这就是增量编码
    // 当前存了个３，预期是500,那就存储-497.
    // 有啥意义么？　能把大数字变成小数字？节省点空间？
    // 这里会把单调递增的数字，算一条执行出来，首尾连接点. 然后每个数字对着线上对应点的偏移距离，画个图会好说很多，一个一元一次方程么？
    for (int i = 0; i < bufferSize; ++i) {
      final long expected = (long) (avgInc * (long) i);
      buffer[i] -= expected;
    }

    // 但是存的不是真实值，而是偏移量
    long min = buffer[0];
    for (int i = 1; i < bufferSize; ++i) {
      min = Math.min(buffer[i], min);
    }

    // 每个位置上存储的，不是偏移量了，而是偏移量与最小的值的偏移量
    // 然后算个最大偏移量
    long maxDelta = 0;
    for (int i = 0; i < bufferSize; ++i) {
      buffer[i] -= min;
      // use | will change nothing when it comes to computing required bits
      // but has the benefit of working fine with negative values too
      // (in case of overflow)
      maxDelta |= buffer[i];
    }

    // 元数据里面开始写, 最小值，平均斜率，data文件从开始到现在写了多少，
    meta.writeLong(min);
    meta.writeInt(Float.floatToIntBits(avgInc));
    // 当前block, 相对于整个类开始写的时候, 的偏移量
    meta.writeLong(data.getFilePointer() - baseDataPointer);
    // 是不是意味着全是0, 也就是绝对的单调递增,等差数列的意思？
    // 如果是等差数列，就不在data里面写了，直接在meta里面记一下最小值就完事了，之后等差就好了
    if (maxDelta == 0) {
      // 最大偏移量为，那就写个0
      meta.writeByte((byte) 0);
    } else {
      // 最大需要多少位
      final int bitsRequired = DirectWriter.unsignedBitsRequired(maxDelta);
      // 把缓冲的数据实际的写到data文件去
      DirectWriter writer = DirectWriter.getInstance(data, bufferSize, bitsRequired);
      for (int i = 0; i < bufferSize; ++i) {
        writer.add(buffer[i]);
      }
      writer.finish();

      // 写一下算出来的最大需要多少位
      meta.writeByte((byte) bitsRequired);
    }

    // 缓冲的数据归零，这样就能一直用内存里的buffer了
    bufferSize = 0;
  }

  long previous = Long.MIN_VALUE;

  /**
   * Write a new value. Note that data might not make it to storage until
   * {@link #finish()} is called.
   *
   * @throws IllegalArgumentException if values don't come in order
   *                                  写一个新的值，
   *                                  但是不一定立即存储，可能在finish的时候才存储
   *                                  如果传入的值不是递增的，就报错
   */
  public void add(long v) throws IOException {
    // 检查是否是单调递增
    if (v < previous) {
      throw new IllegalArgumentException("Values do not come in order: " + previous + ", " + v);
    }
    // 内部缓冲区满，意味着，分块的一块满了, 缓冲区是之前根据分块大小算好的
    if (bufferSize == buffer.length) {
      flush();
    }

    // 缓冲区没满，先放到内存buffer里面
    buffer[bufferSize++] = v;
    previous = v;
    count++;
  }

  /**
   * This must be called exactly once after all values have been {@link #add(long) added}.
   * 所有数字都被调用过all之后，
   * 要调用且只能调用一次finish.
   */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    // 保证只能调用一次
    if (finished) {
      throw new IllegalStateException("#finish has been called already");
    }
    // 调用finish的时候，有缓冲就直接写，反正也只能调用一次
    if (bufferSize > 0) {
      flush();
    }
    finished = true;
  }

  /**
   * Returns an instance suitable for encoding {@code numValues} into monotonic
   * blocks of 2<sup>{@code blockShift}</sup> values. Metadata will be written
   * to {@code metaOut} and actual data to {@code dataOut}.
   * <p>
   * 返回一个适合编码　numValues 个数字　到 (2 ^ blockShift) 大小的递增块的实例
   * 元数据写到meta, 实际的数据写到dataOut.
   */
  public static DirectMonotonicWriter getInstance(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    return new DirectMonotonicWriter(metaOut, dataOut, numValues, blockShift);
  }

}
