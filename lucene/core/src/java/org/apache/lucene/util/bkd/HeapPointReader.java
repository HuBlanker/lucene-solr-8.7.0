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
package org.apache.lucene.util.bkd;

import org.apache.lucene.util.BytesRef;

/**
 * Utility class to read buffered points from in-heap arrays.
 *
 * @lucene.internal
 * */
public final class HeapPointReader implements PointReader {
  private int curRead;
  final byte[] block;
  final BKDConfig config;
  final int end;
  private final HeapPointValue pointValue;

  public HeapPointReader(BKDConfig config, byte[] block, int start, int end) {
    this.block = block;
    curRead = start-1;
    this.end = end;
    this.config = config;
    if (start < end) {
      this.pointValue = new HeapPointValue(config, block);
    } else {
      //no values
      this.pointValue = null;
    }
  }

  @Override
  public boolean next() {
    // 下一个的意思，就是指针变一下咯
    curRead++;
    return curRead < end;
  }

  @Override
  public PointValue pointValue() {
    // 获取当前位置的打包的值，包括点的值和docId哦
    pointValue.setOffset(curRead * config.bytesPerDoc);
    return pointValue;
  }

  @Override
  public void close() {
  }

  /**
   * Reusable implementation for a point value on-heap
   * // 一个堆上的复用的实现.
   * // 实现了一个点的类型. 里面保存了这个点的内容，对应的docId.
   */
  static class HeapPointValue implements PointValue {

    // 给定的数组，是全部的长度。那么第一个的时候就是控制offset就好了
    final BytesRef packedValue;
    // 打包的点的值和docId的组合
    final BytesRef packedValueDocID;
    // 点的值的长度
    final int packedValueLength;

    HeapPointValue(BKDConfig config, byte[] value) {
      this.packedValueLength = config.packedBytesLength;
      this.packedValue = new BytesRef(value, 0, packedValueLength);
      this.packedValueDocID = new BytesRef(value, 0, config.bytesPerDoc);
    }

    /**
     * Sets a new value by changing the offset.
     * // 这个描述不错，通过改变偏移，来换的一个新的值
     */
    public void setOffset(int offset) {
      packedValue.offset = offset;
      packedValueDocID.offset = offset;
    }

    @Override
    // 点的值，　直接返回就可以
    public BytesRef packedValue() {
      return packedValue;
    }

    @Override
    public int docID() {
      int position = packedValueDocID.offset + packedValueLength;
      // [point,docId]. 所以从docId的偏移量开始，　跳过一个point.　然后下一个int就是他的docId. 取了４个字节嘛.
      return ((packedValueDocID.bytes[position] & 0xFF) << 24) | ((packedValueDocID.bytes[++position] & 0xFF) << 16)
          | ((packedValueDocID.bytes[++position] & 0xFF) <<  8) |  (packedValueDocID.bytes[++position] & 0xFF);
    }

    @Override
    // 要是想要点的值和对应的docId的组合，那就直接返回就完事了.
    public BytesRef packedValueDocIDBytes() {
      return packedValueDocID;
    }
  }
}
