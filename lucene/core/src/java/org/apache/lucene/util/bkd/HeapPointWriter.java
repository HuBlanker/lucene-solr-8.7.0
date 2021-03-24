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
import org.apache.lucene.util.FutureArrays;

/**
 * Utility class to write new points into in-heap arrays.
 * // 这个就是论文中提到的哪个内存中的buffer.
 *
 * 可以把一些点，缓存在内存中，用来提升效率.
 *
 * 通过抽象，写入内存buffer. 和写入磁盘，是同一套接口.
 *
 *  @lucene.internal
 *  */
public final class HeapPointWriter implements PointWriter {
  // 长度是点的个数乘以每个docId的长度
  public final byte[] block;
  final int size;
  // 配置
  final BKDConfig config;
  // 一个docId的草稿本
  private final byte[] scratch;
  private int nextWrite;
  private boolean closed;

  private HeapPointReader.HeapPointValue pointValue;


  // 这size，是指最多存储的点的数量
  public HeapPointWriter(BKDConfig config, int size) {
    //
    this.config = config;
    this.block = new byte[config.bytesPerDoc * size];
    this.size = size;
    this.scratch = new byte[config.bytesPerDoc];

    if (size > 0) {
      // 在这里，搞的是第一个点.
      pointValue = new HeapPointReader.HeapPointValue(config, block);
    } else {
      // no values
      pointValue =  null;
    }
  }

  /** Returns a reference, in <code>result</code>, to the byte[] slice holding this value */
  // 简单点说，　就是直接拿到了第index个点的值
  public PointValue getPackedValueSlice(int index) {
    assert index < nextWrite : "nextWrite=" + (nextWrite) + " vs index=" + index;
    pointValue.setOffset(index * config.bytesPerDoc);
    return pointValue;
  }

  @Override
  public void append(byte[] packedValue, int docID) {
    assert closed == false : "point writer is already closed";
    assert packedValue.length == config.packedBytesLength : "[packedValue] must have length [" + config.packedBytesLength + "] but was [" + packedValue.length + "]";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    // 把点的值拷贝过去,
    System.arraycopy(packedValue, 0, block, nextWrite * config.bytesPerDoc, config.packedBytesLength);
    int position = nextWrite * config.bytesPerDoc + config.packedBytesLength;
    // 后面4个字节用来写docId
    block[position] = (byte) (docID >> 24);
    block[++position] = (byte) (docID >> 16);
    block[++position] = (byte) (docID >> 8);
    block[++position] = (byte) (docID >> 0);
    nextWrite++;
  }

  @Override
  public void append(PointValue pointValue) {
    assert closed == false : "point writer is already closed";
    assert nextWrite < size : "nextWrite=" + (nextWrite + 1) + " vs size=" + size;
    // 拿到点的数据和docId一起打包好的数据
    BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
    assert packedValueDocID.length == config.bytesPerDoc : "[packedValue] must have length [" + (config.bytesPerDoc) + "] but was [" + packedValueDocID.length + "]";
    // 拷贝到block去
    System.arraycopy(packedValueDocID.bytes, packedValueDocID.offset, block, nextWrite * config.bytesPerDoc, config.bytesPerDoc);
    nextWrite++;
  }

  // 交换第i个和第j个的数据
  public void swap(int i, int j) {

    int indexI = i * config.bytesPerDoc;
    int indexJ = j * config.bytesPerDoc;

    // scratch1 = values[i]
    System.arraycopy(block, indexI, scratch, 0, config.bytesPerDoc);
    // values[i] = values[j]
    System.arraycopy(block, indexJ, block, indexI, config.bytesPerDoc);
    // values[j] = scratch1
    System.arraycopy(scratch, 0, block, indexJ, config.bytesPerDoc);
  }

  // 计算不同的点的数量
  public int computeCardinality(int from, int to, int[] commonPrefixLengths) {
    int leafCardinality = 1;
    for (int i = from + 1; i < to; i++) {
      for (int dim = 0; dim < config.numDims; dim++) {
        final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * config.bytesPerDim + config.bytesPerDim;
        if (FutureArrays.mismatch(block, i * config.bytesPerDoc + start, i * config.bytesPerDoc + end,
                block, (i - 1) * config.bytesPerDoc  + start, (i - 1) * config.bytesPerDoc + end) != -1) {
          leafCardinality++;
          break;
        }
      }
    }
    return leafCardinality;
  }

  @Override
  // 多少个点
  public long count() {
    return nextWrite;
  }

  @Override
  public PointReader getReader(long start, long length) {
    assert closed : "point writer is still open and trying to get a reader";
    assert start + length <= size: "start=" + start + " length=" + length + " docIDs.length=" + size;
    assert start + length <= nextWrite: "start=" + start + " length=" + length + " nextWrite=" + nextWrite;
    // 返回一个Reader
    return new HeapPointReader(config, block, (int) start, Math.toIntExact(start+length));
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public void destroy() {
  }

  @Override
  public String toString() {
    return "HeapPointWriter(count=" + nextWrite + " size=" + size + ")";
  }
}
