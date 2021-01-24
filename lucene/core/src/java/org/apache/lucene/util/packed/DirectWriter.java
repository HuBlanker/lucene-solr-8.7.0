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


import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataOutput;

/** 
 * Class for writing packed integers to be directly read from Directory.
 * Integers can be read on-the-fly via {@link DirectReader}.
 * <p>
 * Unlike PackedInts, it optimizes for read i/o operations and supports &gt; 2B values.
 * Example usage:
 * <pre class="prettyprint">
 *   int bitsPerValue = DirectWriter.bitsRequired(100); // values up to and including 100
 *   IndexOutput output = dir.createOutput("packed", IOContext.DEFAULT);
 *   DirectWriter writer = DirectWriter.getInstance(output, numberOfValues, bitsPerValue);
 *   for (int i = 0; i &lt; numberOfValues; i++) {
 *     writer.add(value);
 *   }
 *   writer.finish();
 *   output.close();
 * </pre>
 * 直接从Directory读取包装好的int.
 * 不像PackedInts，　这个类优化了io操作并且支持大于2B的数据，
 * 然后给了个使用示例.
 * 分析代码完成后看看解析，然后跑一跑单测
 *
 * <br>
 * <B>存储int,本来需要32位，这个类根据最大的数字，用他所需要的位数来存储所有数据。很明显起到了压缩效果,
 * 而且数据都比较小，压缩率会更高。如果都是最大的位数那个范围的，就没有压缩效果了。</B>
 *
 *
 * @see DirectReader
 */
public final class DirectWriter {
  // 每一个值需要几个bit
  final int bitsPerValue;
  // 总数
  final long numValues;
  // 输出方
  final DataOutput output;
  // 当前写了多少
  long count;
  boolean finished;
  // for now, just use the existing writer under the hood
  // 当前写入了多少个, 在nextValues里面的偏移
  int off;

  // 这两个是符合对应关系的, 因此 nextValues.length * bitsPerValue = nextBlocks.length * 8
  // 编码后的所有数据
  final byte[] nextBlocks;
  // 所有的原始数据, 打算存这么多数字, 每个数字用bitsPerValue. 那么总共需要nextValues.length * bitsPerValue.
  // 这些都要存在nextBlocks里面, 所以除以8就是nextBlocks的长度.
  final long[] nextValues;

  // 编码器
  final BulkOperation encoder;
  // 1024内存能够缓存多少个完整的块.
  final int iterations;
  
  DirectWriter(DataOutput output, long numValues, int bitsPerValue) {
    this.output = output;
    this.numValues = numValues;
    this.bitsPerValue = bitsPerValue;
    // 因为你需要的位不一样，那么需要的顺序读写的编码器就不一样，为了性能吧，搞了很多东西
    // 搞了很多个编码解码器, 根据存储的位数不一样而不一样
    encoder = BulkOperation.of(PackedInts.Format.PACKED, bitsPerValue);
    // 这里计算一下的目的是, 内存buffer尽量刚刚好用1024字节, 不要太小, 导致吞吐量降低, 不要太大, 导致oom.
    // 用1024字节的内存, 能缓存多少个编码块. 如果用不了1024, 就只申请刚刚的大小.
    iterations = encoder.computeIterations((int) Math.min(numValues, Integer.MAX_VALUE), PackedInts.DEFAULT_BUFFER_SIZE);
    // 申请内存里的对应buffer array.
    nextBlocks = new byte[iterations * encoder.byteBlockCount()];
    nextValues = new long[iterations * encoder.byteValueCount()];
  }
  
  /** Adds a value to this writer
   * 添加一个值
   *
   */
  public void add(long l) throws IOException {
    // 几个校验
    assert bitsPerValue == 64 || (l >= 0 && l <= PackedInts.maxValue(bitsPerValue)) : bitsPerValue;
    assert !finished;
    if (count >= numValues) {
      throw new EOFException("Writing past end of stream");
    }

    // 当前缓冲的数量，够了就flush
    nextValues[off++] = l;
    if (off == nextValues.length) {
      flush();
    }

    count++;
  }
  
  private void flush() throws IOException {
    // 把当前缓冲的值，编码起来到nextBlocks
    // 当前缓冲的在nextValues，把他按照编码，搞到nextBlocks里面
    // 反正就是存储啦，编码没搞懂，草
    encoder.encode(nextValues, 0, nextBlocks, 0, iterations);

    final int blockCount = (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    // 写入到磁盘
    output.writeBytes(nextBlocks, blockCount);
    // 缓冲归0
    Arrays.fill(nextValues, 0L);
    off = 0;
  }

  /** finishes writing
   *
   * 检查数据，检查完最后一次flush 掉
   */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    assert !finished;
    flush();
    // pad for fast io: we actually only need this for certain BPV, but its just 3 bytes...
    for (int i = 0; i < 3; i++) {
      output.writeByte((byte) 0);
    }
    finished = true;
  }
  
  /** Returns an instance suitable for encoding {@code numValues} using {@code bitsPerValue} */
  public static DirectWriter getInstance(DataOutput output, long numValues, int bitsPerValue) {
    if (Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) < 0) {
      throw new IllegalArgumentException("Unsupported bitsPerValue " + bitsPerValue + ". Did you use bitsRequired?");
    }
    return new DirectWriter(output, numValues, bitsPerValue);
  }
  
  /** 
   * Round a number of bits per value to the next amount of bits per value that
   * is supported by this writer.
   *
   * 类似于检查下需要几位来存储一个数字，　这个几位行不行
   * 
   * @param bitsRequired the amount of bits required
   * @return the next number of bits per value that is gte the provided value
   *         and supported by this writer
   */
  private static int roundBits(int bitsRequired) {
    int index = Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsRequired);
    if (index < 0) {
      return SUPPORTED_BITS_PER_VALUE[-index-1];
    } else {
      return bitsRequired;
    }
  }

  /**
   * Returns how many bits are required to hold values up
   * to and including maxValue
   *
   * 存储这个数字，需要几位
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#bitsRequired(long)
   */
  public static int bitsRequired(long maxValue) {
    return roundBits(PackedInts.bitsRequired(maxValue));
  }

  /**
   * Returns how many bits are required to hold values up
   * to and including maxValue, interpreted as an unsigned value.
   *  上一个方法的无符号版本，存储全是整数的话，　能多存一点【
   *
   * @param maxValue the maximum value that should be representable.
   * @return the amount of bits needed to represent values from 0 to maxValue.
   * @see PackedInts#unsignedBitsRequired(long)
   */
  public static int unsignedBitsRequired(long maxValue) {
    return roundBits(PackedInts.unsignedBitsRequired(maxValue));
  }

  final static int SUPPORTED_BITS_PER_VALUE[] = new int[] {
    1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 56, 64
  };
}
