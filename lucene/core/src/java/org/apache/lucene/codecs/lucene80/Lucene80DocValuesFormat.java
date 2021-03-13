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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Lucene 8.0 DocValues format.
 * <p>
 * Documents that have a value for the field are encoded in a way that it is always possible to
 * know the ordinal of the current document in the set of documents that have a value. For instance,
 * say the set of documents that have a value for the field is <tt>{1, 5, 6, 11}</tt>. When the
 * iterator is on <tt>6</tt>, it knows that this is the 3rd item of the set. This way, values can
 * be stored densely and accessed based on their index at search time. If all documents in a segment
 * have a value for the field, the index is the same as the doc ID, so this case is encoded implicitly
 * and is very fast at query time. On the other hand if some documents are missing a value for the
 * field then the set of documents that have a value is encoded into blocks. All doc IDs that share
 * the same upper 16 bits are encoded into the same block with the following strategies:
 * <ul>
 *     <li>SPARSE: This strategy is used when a block contains at most 4095 documents. The lower 16
 *         bits of doc IDs are stored as {@link DataOutput#writeShort(short) shorts} while the upper
 *         16 bits are given by the block ID.
 *     <li>DENSE: This strategy is used when a block contains between 4096 and 65535 documents. The
 *         lower bits of doc IDs are stored in a bit set. Advancing &lt; 512 documents is performed using
 *         {@link Long#numberOfTrailingZeros(long) ntz} operations while the index is computed by
 *         accumulating the {@link Long#bitCount(long) bit counts} of the visited longs.
 *         Advancing &gt;= 512 documents is performed by skipping to the start of the needed 512 document
 *         sub-block and iterating to the specific document within that block. The index for the
 *         sub-block that is skipped to is retrieved from a rank-table positioned beforethe bit set.
 *         The rank-table holds the origo index numbers for all 512 documents sub-blocks, represented
 *         as an unsigned short for each 128 blocks.
 *     <li>ALL: This strategy is used when a block contains exactly 65536 documents, meaning that
 *         the block is full. In that case doc IDs do not need to be stored explicitly. This is
 *         typically faster than both SPARSE and DENSE which is a reason why it is preferable to have
 *         all documents that have a value for a field using contiguous doc IDs, for instance by
 *         using {@link IndexWriterConfig#setIndexSort(org.apache.lucene.search.Sort) index sorting}.
 * </ul>
 * <p>
 * Skipping blocks to arrive at a wanted document is either done on an iterative basis or by using the
 * jump-table stored at the end of the chain of blocks. The jump-table holds the offset as well as the
 * index for all blocks, packed in a single long per block.
 * </p>
 * <p>
 * Then the five per-document value types (Numeric,Binary,Sorted,SortedSet,SortedNumeric) are
 * encoded using the following strategies:
 * <p>
 * {@link DocValuesType#NUMERIC NUMERIC}:
 * <ul>
 *    <li>Delta-compressed: per-document integers written as deltas from the minimum value,
 *        compressed with bitpacking. For more information, see {@link DirectWriter}.
 *    <li>Table-compressed: when the number of unique values is very small (&lt; 256), and
 *        when there are unused "gaps" in the range of values used (such as {@link SmallFloat}),
 *        a lookup table is written instead. Each per-document entry is instead the ordinal
 *        to this table, and those ordinals are compressed with bitpacking ({@link DirectWriter}).
 *    <li>GCD-compressed: when all numbers share a common divisor, such as dates, the greatest
 *        common denominator (GCD) is computed, and quotients are stored using Delta-compressed Numerics.
 *    <li>Monotonic-compressed: when all numbers are monotonically increasing offsets, they are written
 *        as blocks of bitpacked integers, encoding the deviation from the expected delta.
 *    <li>Const-compressed: when there is only one possible value, no per-document data is needed and
 *        this value is encoded alone.
 * </ul>
 * <p>
 * Depending on calculated gains, the numbers might be split into blocks of 16384 values. In that case,
 * a jump-table with block offsets is appended to the blocks for O(1) access to the needed block.
 * </p>
 * <p>
 * {@link DocValuesType#BINARY BINARY}:
 * <ul>
 *    <li>Fixed-width Binary: one large concatenated byte[] is written, along with the fixed length.
 *        Each document's value can be addressed directly with multiplication ({@code docID * length}).
 *    <li>Variable-width Binary: one large concatenated byte[] is written, along with end addresses
 *        for each document. The addresses are written as Monotonic-compressed numerics.
 *    <li>Prefix-compressed Binary: values are written in chunks of 16, with the first value written
 *        completely and other values sharing prefixes. chunk addresses are written as Monotonic-compressed
 *        numerics. A reverse lookup index is written from a portion of every 1024th term.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED SORTED}:
 * <ul>
 *    <li>Sorted: a mapping of ordinals to deduplicated terms is written as Prefix-compressed Binary,
 *        along with the per-document ordinals written using one of the numeric strategies above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_SET SORTED_SET}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like SORTED.
 *    <li>SortedSet: a mapping of ordinals to deduplicated terms is written as Binary,
 *        an ordinal list and per-document index into this list are written using the numeric strategies
 *        above.
 * </ul>
 * <p>
 * {@link DocValuesType#SORTED_NUMERIC SORTED_NUMERIC}:
 * <ul>
 *    <li>Single: if all documents have 0 or 1 value, then data are written like NUMERIC.
 *    <li>SortedNumeric: a value list and per-document index into this list are written using the numeric
 *        strategies above.
 * </ul>
 * <p>
 * Files:
 * <ol>
 *   <li><tt>.dvd</tt>: DocValues data</li>
 *   <li><tt>.dvm</tt>: DocValues metadata</li>
 * </ol>
 *
 * <br/>
 * Lucene8.0的DocValue格式.
 *
 * <br/>
 *  在field中有值的doc被编码为：　始终可以知道具有之的一组文档中当前文档的序数.
 *
 *  假设具有该字段的文档集合为{1,5,6,11}. 当遍历到6的时候，我们知道遍历到集合的第三个元素了.
 *
 *  这样的话，值可以被密集的存储并且在搜索的时候，　可以根据他们的索引/下标进行访问.
 *
 *  如果一个分片中的所有文档都包含有该filed.　索引就和他的docId是一样的，因此这种情况下编码是隐含的，并且在查找的时候非常的快.
 *
 *  <br/>
 *  在另外一种情况下，如果一些文档没有这个字段，有值的文档就会被编码成块. 一个块中的所有docIds将按照下面的策略共享高位的 16bits.
 *
 *  <ul>
 *    <li>
 *      <b>SPAESE(稀疏的)</b>: 如果一个块含有最多4095个doc,那么将使用这个策略. 低16位将使用shorts存储，高16位由docId给定.
 *    </li>
 *    <li>
 *      <B>DENSE:(密集的)</B> 如果一个块含有4096~65535个docId，那么将使用这个策略.低位的bit将存储在一个bitSet中. 512之前的文档将使用`ntz`
 *      操作进行优化，　因为索引是根据访问的longs的bitCounts累加计算的？？？
 *      <p> 大于等于512的文档，通过跳过需要的512个文档子块，迭代到所需的文档快. 子块的索引是跳过ｘｘｘ的查找的.排名表包含有原使用的所有编号。
 *      使用无符号的short来存储.</p>
 *    </li>
 *    <li>
 *      <b>all: </b> 这个策略在一个块含有准确的65536个文档时使用. 意味着块是满的.
 *      这种情况下，docIds不需要明确的存储。这个策略通常是比前两个策略快的。因为所有文档都含有某个字段，那么就是连续的docIds.　可以使用`Index sorting`
 *      。这是更加可取的.
 *    </li>
 *  </ul>
 *  跳过一些块来找到想要的文档也是ＯＫ的. 也可以使用存储在块链末尾的跳转表来进行。
 *  跳转表保存所有块的偏移量和索引，每个块打包成一个long.
 *
 *  使用一下策略对五种每个文档的值类型进行编码.
 *
 *  <B> NUMERIC:</B>
 *
 *  <ul>
 *    <li>
 *      <b>delta-compressed:</b>: 每一个文档的整数,使用增量编码后用bitPacking进行压缩.更多的信息可以看:DirectWriter.我看过了嘻嘻》
 *      <b>Table-compressed: </b> 如果唯一值的数量很小(<256),　同时在已有的值区间里有一些没有使用的gaps？，写入一个表？？
 *      每个文档节点用他们的序数表示在这个表中，这些序数也是用bitpacking压缩.
 *      <b>GCD-compressed:</b> 如果所有的数字都共享一个相同的除数(是指他们有公约数?)., 比如dates，最大公约数算出来，然后把商使用增量编码压缩起来.
 *      <b>Monotonic-compressed:</b> 如果所有的数字都是单调递增的，那么就能比delta-compressed压缩的更狠一点，详细的可以看：　MonotonicDirectWriter.
 *      <b> Const-compressed: </b>  只有一个可能的值，不需要每个文档都存一遍了，直接把这个值自己存起来.
 *    </li>
 *  </ul>
 *
 *  取决于计算收益，数字可能被分割到块里去.( 16384个)
 *  这种情况下，一个`块的偏移量的跳表` 被添加在所有的blocks后面，为了能在o(1)的时间复杂度内访问想要的块.
 *
 * <B> BINARY:</B>
 * <ul>
 *   <li>
 *     <B>　Fixed-width Binary:</B> 一个大的级联的字节数组. 固定长度. 每一个文档的值可以被直接找到，通过 (docId * length)
 *     <B> Variable-width Binary:</B> 一个大的级联的字节数组，记录了每个文档的结束地址，地址使用单调递增压缩存储.
 *     <B> Prefix-compressed Binary:</B> 前缀压缩. 所有的值写在16个一块的块里. 块里的第一个值全部写入而后面的值共享前缀.
 *     chunk的地址使用单调递增压缩存储. 反向查询索引是每个第1024个词的位置写入的.
 *   </li>
 * </ul>
 *
 * <B> SORTED:</B>
 *
 * sorted: 一个普通元素到重复元素的映射，　使用前缀压缩。　每个原始文档使用上面的某一种Numertic策略.
 *
 * <B> SORTED SET</B>
 * <ul>
 *   <li>
 *     <B> Single: </B> 如果所有的文档有１个或者０个值，那么数据将使用上面的SORTED存储.
 *   </li>
 *   <li>
 *     <B> SortedSet </B>
 * 一   个普通元素到重复元素的映射，　使用前缀压缩。　每个原始文档的列表及他的索引使用上面的某一种Numertic策略.
 *   </li>
 * </ul>
 *
 * <B> Sorted Numeric</B>
 * <ul>
 *   <li>
 *     <B> Single</B>: 如果有文档有1个或者0个值，那么使用numeric存储.
 *   </li>
 *   <li>
 *     <B> SortedNumeric</B>:
 * 一   个普通元素到重复元素的映射，　使用前缀压缩。　每个原始文档的列表及他的索引使用上面的某一种Numertic策略.
 *   </li>
 * </ul>

 *
 */
public final class Lucene80DocValuesFormat extends DocValuesFormat {

  /** Sole Constructor */
  public Lucene80DocValuesFormat() {
    super("Lucene80");
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene80DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
    return new Lucene80DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  static final String DATA_CODEC = "Lucene80DocValuesData";
  static final String DATA_EXTENSION = "dvd";
  static final String META_CODEC = "Lucene80DocValuesMetadata";
  static final String META_EXTENSION = "dvm";
  static final int VERSION_START = 0;
  static final int VERSION_BIN_COMPRESSED = 1;  
  static final int VERSION_CURRENT = VERSION_BIN_COMPRESSED;

  // indicates docvalues type
  static final byte NUMERIC = 0;
  static final byte BINARY = 1;
  static final byte SORTED = 2;
  static final byte SORTED_SET = 3;
  static final byte SORTED_NUMERIC = 4;

  static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  static final int NUMERIC_BLOCK_SHIFT = 14;
  static final int NUMERIC_BLOCK_SIZE = 1 << NUMERIC_BLOCK_SHIFT;

  static final int BINARY_BLOCK_SHIFT = 5;
  static final int BINARY_DOCS_PER_COMPRESSED_BLOCK = 1 << BINARY_BLOCK_SHIFT;
  
  static final int TERMS_DICT_BLOCK_SHIFT = 4;
  static final int TERMS_DICT_BLOCK_SIZE = 1 << TERMS_DICT_BLOCK_SHIFT;
  static final int TERMS_DICT_BLOCK_MASK = TERMS_DICT_BLOCK_SIZE - 1;

  static final int TERMS_DICT_REVERSE_INDEX_SHIFT = 10;
  static final int TERMS_DICT_REVERSE_INDEX_SIZE = 1 << TERMS_DICT_REVERSE_INDEX_SHIFT;
  static final int TERMS_DICT_REVERSE_INDEX_MASK = TERMS_DICT_REVERSE_INDEX_SIZE - 1;
}
