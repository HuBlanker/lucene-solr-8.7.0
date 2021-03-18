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
package org.apache.lucene.codecs.lucene80;

import java.io.IOException;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import static org.apache.lucene.codecs.lucene80.Lucene80NormsFormat.VERSION_CURRENT;

/**
 * Writer for {@link Lucene80NormsFormat}
 */
final class Lucene80NormsConsumer extends NormsConsumer {
  IndexOutput data, meta;
  final int maxDoc;

  Lucene80NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension) throws IOException {
    boolean success = false;
    try {
      // data header
      String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

      // metaHeader
      String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      // max Doc
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  @Override
  public void addNormsField(FieldInfo field, NormsProducer normsProducer) throws IOException {
    NumericDocValues values = normsProducer.getNorms(field);
    // 这个域上有值的doc数量
    int numDocsWithValue = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithValue++;
      long v = values.longValue();
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    assert numDocsWithValue <= maxDoc;

    // field Number
    meta.writeInt(field.number);

    // 如果没有文档在这个域上有norms值
    if (numDocsWithValue == 0) {
      // -2
      meta.writeLong(-2); // docsWithFieldOffset
      // 0
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithValue == maxDoc) {
      // 如果每一个都有值
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      // 之外的情况
      long offset = data.getFilePointer();
      // 在data文件中的偏移位置
      meta.writeLong(offset); // docsWithFieldOffset
      values = normsProducer.getNorms(field);
      // 写了点data进去
      final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      // 写了多长的数据
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      // jump什么万一
      meta.writeShort(jumpTableEntryCount);
      // 这是个啥
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    // 这个域有多少个doc有值
    meta.writeInt(numDocsWithValue);
    int numBytesPerValue = numBytesPerValue(min, max);

    // 每个值占用多少字节
    meta.writeByte((byte) numBytesPerValue);
    // 全部一样
    if (numBytesPerValue == 0) {
      // 写个min,
      meta.writeLong(min);
    } else {
      // 记录下指针
      meta.writeLong(data.getFilePointer()); // normsOffset
      values = normsProducer.getNorms(field);
      // 给data里面写数据
      writeValues(values, numBytesPerValue, data);
    }
  }

  private int numBytesPerValue(long min, long max) {
    if (min >= max) {
      return 0;
    } else if (min >= Byte.MIN_VALUE && max <= Byte.MAX_VALUE) {
      return 1;
    } else if (min >= Short.MIN_VALUE && max <= Short.MAX_VALUE) {
      return 2;
    } else if (min >= Integer.MIN_VALUE && max <= Integer.MAX_VALUE) {
      return 4;
    } else {
      return 8;
    }
  }

  private void writeValues(NumericDocValues values, int numBytesPerValue, IndexOutput out) throws IOException, AssertionError {
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      long value = values.longValue();
      switch (numBytesPerValue) {
        case 1:
          out.writeByte((byte) value);
          break;
        case 2:
          out.writeShort((short) value);
          break;
        case 4:
          out.writeInt((int) value);
          break;
        case 8:
          out.writeLong(value);
          break;
        default:
          throw new AssertionError();
      }
    }
  }
}
