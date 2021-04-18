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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 * Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 * and smaller N-dim rectangles (cells) until the number of points in a given
 * rectangle is &lt;= <code>config.maxPointsInLeafNode</code>.  The tree is
 * partially balanced, which means the leaf nodes will have
 * the requested <code>config.maxPointsInLeafNode</code> except one that might have less.
 * Leaf nodes may straddle the two bottom levels of the binary tree.
 * Values that fall exactly on a cell boundary may be in either cell.
 *
 * <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 * <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>,
 * a <code>byte[numLeaves*(1+config.bytesPerDim)]</code> and then uses up to the specified
 * {@code maxMBSortInHeap} heap space for writing.
 *
 * <p>
 * <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>config.maxPointsInLeafNode</code> / config.bytesPerDim
 * total points.
 *
 * @lucene.experimental 递归的构建kd树.
 * 将n维空间中的所有点，分配给越来越小的ｎ维空间. 知道给定的空间中的数量小于config.maxPosintsInLeafNode。
 * <p>
 * 该树是部分平衡的。意味着叶子节点都有config.maxPointsInLeafNode。该是该节点可能更少. 叶子节点可以跨越二叉树的两个底层. 恰好落在单元格边界的值可能会在任意一个中。
 * <p>
 * 维度可以为1-8，但是每个字节数组都是固定的长度.
 * 再写入过程中，小号队，分配一个long[numLeaves]和一个. 然后使用指定的最大内存进行写入.
 * <p>
 * 总的点数: 入Integer.MAX_VALUE * config.maxPointsInLeafNode / config.bytesPerDim
 */

public class BKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";
  public static final int VERSION_START = 4; // version used by Lucene 7.0
  //public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_LOW_CARDINALITY_LEAVES = 7;
  public static final int VERSION_META_FILE = 9;
  // 好多个版本哦，这个是说加入了元数据文件的这个版本，也就是8.6.0哦
  public static final int VERSION_CURRENT = VERSION_META_FILE;


  /**
   * Number of splits before we compute the exact bounding box of an inner node.
   */
  // 在我们计算一个内部节点的精确边界之前，　进行几次分裂
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;
  /**
   * Default maximum heap to use, before spilling to (slower) disk
   */
  // 最多这个类型只能使用16MB的内存
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /**
   * BKD tree configuration
   */
  // 配置
  protected final BKDConfig config;

  // 中间临时变量文件夹
  final TrackingDirectoryWrapper tempDir;
  // 临时文件的前缀
  final String tempFileNamePrefix;
  // 最大内存
  final double maxMBSortInHeap;

  // 草稿吗三个？？
  final byte[] scratchDiff;
  final byte[] scratch1;
  final byte[] scratch2;

  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();

  // 公共前缀长度
  final int[] commonPrefixLengths;

  // 所有写过的docId的集合
  // 用bitset的每一位来表示这个docId见过没有，因为docId是不重复的，且是基本上一直递增的int/long.因此完全可以这样用
  protected final FixedBitSet docsSeen;

  // 点的写入者，完蛋，又要看了
  private PointWriter pointWriter;
  private boolean finished;

  // 临时的输出？
  private IndexOutput tempInput;
  // 最多有多少个point在堆上进行排序
  private final int maxPointsSortInHeap;

  /**
   * Minimum per-dim values, packed
   */
  // 最小的一个点
  protected final byte[] minPackedValue;

  /**
   * Maximum per-dim values, packed
   */
  // 最大的一个点
  protected final byte[] maxPackedValue;

  // 当前写入数量的计数器
  protected long pointCount;

  /**
   * An upper bound on how many points the caller will add (includes deletions)
   */
  // 最多写入多少个数字，包含删除的哦
  private final long totalPointCount;

  // 最大的文档ID
  private final int maxDoc;

  public BKDWriter(int maxDoc, Directory tempDir, String tempFileNamePrefix, BKDConfig config,
                   double maxMBSortInHeap, long totalPointCount) {
    verifyParams(maxMBSortInHeap, totalPointCount);
    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    // 其实就是在当前目录下
    this.tempDir = new TrackingDirectoryWrapper(tempDir);
    // 这里用的就是当前segment的名字
    this.tempFileNamePrefix = tempFileNamePrefix;
    // 16.0
    this.maxMBSortInHeap = maxMBSortInHeap;

    // point总数，也是最大的数量?
    this.totalPointCount = totalPointCount;
    // segment.maxDoc
    this.maxDoc = maxDoc;

    // bkd config
    this.config = config;

    // 构建一个maxDoc的集合，之后可以表示每个doc见过没有.
    docsSeen = new FixedBitSet(maxDoc);


    scratchDiff = new byte[config.bytesPerDim];
    scratch1 = new byte[config.packedBytesLength];
    scratch2 = new byte[config.packedBytesLength];
    // 每个维度的前缀
    commonPrefixLengths = new int[config.numDims];

    minPackedValue = new byte[config.packedIndexBytesLength];
    maxPackedValue = new byte[config.packedIndexBytesLength];

    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (config.bytesPerDoc));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < config.maxPointsInLeafNode) {
      throw new IllegalArgumentException("maxMBSortInHeap=" + maxMBSortInHeap + " only allows for maxPointsSortInHeap="
          + maxPointsSortInHeap + ", but this is less than maxPointsInLeafNode=" + config.maxPointsInLeafNode + "; "
          + "either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }
  }

  private static void verifyParams(double maxMBSortInHeap, long totalPointCount) {
    if (maxMBSortInHeap < 0.0) {
      throw new IllegalArgumentException("maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  // 初始化一下
  private void initPointWriter() throws IOException {
    assert pointWriter == null : "Point writer is already initialized";
    // Total point count is an estimation but the final point count must be equal or lower to that number.
    if (totalPointCount > maxPointsSortInHeap) {
      pointWriter = new OfflinePointWriter(config, tempDir, tempFileNamePrefix, "spill", 0);
      tempInput = ((OfflinePointWriter) pointWriter).out;
    } else {
      pointWriter = new HeapPointWriter(config, Math.toIntExact(totalPointCount));
    }
  }

  public void add(byte[] packedValue, int docID) throws IOException {
    // 数据check
    if (packedValue.length != config.packedBytesLength) {
      throw new IllegalArgumentException("packedValue should be length=" + config.packedBytesLength + " (got: " + packedValue.length + ")");
    }
    if (pointCount >= totalPointCount) {
      throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (pointCount + 1) + " values");
    }
    // 初始化
    if (pointCount == 0) {
      initPointWriter();
      System.arraycopy(packedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
    } else {
      // 每个维度进行写入
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        int offset = dim * config.bytesPerDim;

        // 进行最大最小值的写入
        if (FutureArrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, minPackedValue, offset, offset + config.bytesPerDim) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, config.bytesPerDim);
        } else if (FutureArrays.compareUnsigned(packedValue, offset, offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, config.bytesPerDim);
        }
      }
    }
    // 追加当前点
    pointWriter.append(packedValue, docID);
    pointCount++;
    // 记录docId
    docsSeen.set(docID);
  }

  private static class MergeReader {
    final BKDReader bkd;
    final BKDReader.IntersectState state;
    final MergeState.DocMap docMap;

    /**
     * Current doc ID
     */
    public int docID;

    /**
     * Which doc in this block we are up to
     */
    private int docBlockUpto;

    /**
     * How many docs in the current block
     */
    private int docsInBlock;

    /**
     * Which leaf block we are up to
     */
    private int blockID;

    private final byte[] packedValues;

    public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
      this.bkd = bkd;
      state = new BKDReader.IntersectState(bkd.in.clone(),
          bkd.config,
          null,
          null);
      this.docMap = docMap;
      state.in.seek(bkd.getMinLeafBlockFP());
      this.packedValues = new byte[bkd.config.maxPointsInLeafNode * bkd.config.packedBytesLength];
    }

    public boolean next() throws IOException {
      //System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            //System.out.println("  done!");
            return false;
          }
          //System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchIterator);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(state.commonPrefixLengths, state.scratchDataPackedValue, state.scratchMinIndexPackedValue, state.scratchMaxIndexPackedValue, state.in, state.scratchIterator, docsInBlock, new IntersectVisitor() {
            int i = 0;

            @Override
            public void visit(int docID) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              assert docID == state.scratchIterator.docIDs[i];
              System.arraycopy(packedValue, 0, packedValues, i * bkd.config.packedBytesLength, bkd.config.packedBytesLength);
              i++;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }

          });

          blockID++;
        }

        final int index = docBlockUpto++;
        int oldDocID = state.scratchIterator.docIDs[index];

        int mappedDocID;
        if (docMap == null) {
          mappedDocID = oldDocID;
        } else {
          mappedDocID = docMap.get(oldDocID);
        }

        if (mappedDocID != -1) {
          // Not deleted!
          docID = mappedDocID;
          System.arraycopy(packedValues, index * bkd.config.packedBytesLength, state.scratchDataPackedValue, 0, bkd.config.packedBytesLength);
          return true;
        }
      }
    }
  }

  private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
    private final int bytesPerDim;

    public BKDMergeQueue(int bytesPerDim, int maxSize) {
      super(maxSize);
      this.bytesPerDim = bytesPerDim;
    }

    @Override
    public boolean lessThan(MergeReader a, MergeReader b) {
      assert a != b;

      int cmp = FutureArrays.compareUnsigned(a.state.scratchDataPackedValue, 0, bytesPerDim, b.state.scratchDataPackedValue, 0, bytesPerDim);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /**
   * flat representation of a kd-tree
   */
  // kdtree的一个平坦的实现
  // 叶子节点的集合
  // 我发现这群人很喜欢写这种集合类啊
  private interface BKDTreeLeafNodes {
    /**
     * number of leaf nodes
     */
    int numLeaves();

    /**
     * pointer to the leaf node previously written. Leaves are order from
     * left to right, so leaf at {@code index} 0 is the leftmost leaf and
     * the the leaf at {@code numleaves()} -1 is the rightmost leaf
     */
    // 叶子的指针什么的
    long getLeafLP(int index);

    /**
     * split value between two leaves. The split value at position n corresponds to the
     * leaves at (n -1) and n.
     */
    // 两个叶子之间的分割值. n处的分割值对应(n-1,n)的值.
    BytesRef getSplitValue(int index);

    /**
     * split dimension between two leaves. The split dimension at position n corresponds to the
     * leaves at (n -1) and n.
     */
    // 分割的维度，使用哪个分割呢
    int getSplitDimension(int index);
  }

  /**
   * Write a field from a {@link MutablePointValues}. This way of writing
   * points is faster than regular writes with {@link BKDWriter#add} since
   * there is opportunity for reordering points before writing them to
   * disk. This method does not use transient disk in order to reorder points.
   *
   * // 使用MutablePointValue来写入一个域，比使用BKDWriter.add更加快. 因为这样可以在写入磁盘之前，对数据点进行重排序.
   *
   * // 这个方法不使用短暂的磁盘去排序？？
   */
  public Runnable writeField(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues reader) throws IOException {
    // 如果这个树，　是一维的，那就调用一维，否则就走多维.
    if (config.numDims == 1) {
      return writeField1Dim(metaOut, indexOut, dataOut, fieldName, reader);
    } else {
      return writeFieldNDims(metaOut, indexOut, dataOut, fieldName, reader);
    }
  }

  private void computePackedValueBounds(MutablePointValues values, int from, int to, byte[] minPackedValue, byte[] maxPackedValue, BytesRef scratch) {
    if (from == to) {
      return;
    }
    values.getValue(from, scratch);
    System.arraycopy(scratch.bytes, scratch.offset, minPackedValue, 0, config.packedIndexBytesLength);
    System.arraycopy(scratch.bytes, scratch.offset, maxPackedValue, 0, config.packedIndexBytesLength);
    for (int i = from + 1; i < to; ++i) {
      values.getValue(i, scratch);
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        final int startOffset = dim * config.bytesPerDim;
        final int endOffset = startOffset + config.bytesPerDim;
        if (FutureArrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, minPackedValue, startOffset, config.bytesPerDim);
        } else if (FutureArrays.compareUnsigned(scratch.bytes, scratch.offset + startOffset, scratch.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
          System.arraycopy(scratch.bytes, scratch.offset + startOffset, maxPackedValue, startOffset, config.bytesPerDim);
        }
      }
    }
  }

  /* In the 2+D case, we recursively pick the split dimension, compute the
   * median value and partition other values around it. */
  // 在2维及以上，我们递归的选择切割维度，计算中位数然后用中位数来分割值.
  private Runnable writeFieldNDims(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues values) throws IOException {
    // 必须是空的，开整
    if (pointCount != 0) {
      throw new IllegalStateException("cannot mix add and writeField");
    }

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    // Mark that we already finished:
    finished = true;

    // 点的总数
    pointCount = values.size();

    // 多少个叶子节点
    final int numLeaves = Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);
    // 那么就需要n-1次切割
    final int numSplits = numLeaves - 1;

    // some check
    checkMaxLeafNodeCount(numLeaves);

    // 切割点的值
    final byte[] splitPackedValues = new byte[numSplits * config.bytesPerDim];
    // 切割维度的值,记录下每一次切割使用的维度
    final byte[] splitDimensionValues = new byte[numSplits];
    // 每个叶子的文件指针
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    // 最小最大值
    computePackedValueBounds(values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);
    // 拿到所有的docId
    for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
      docsSeen.set(values.getDocID(i));
    }

    // 这开始的data文件指针
    final long dataStartFP = dataOut.getFilePointer();
    // 父节点使用各个维度进行切割的次数.所以长度是维度. 比如１维3次. 2维８次.
    final int[] parentSplits = new int[config.numIndexDims];
    // 构建整个树
    build(0, numLeaves, values, 0, Math.toIntExact(pointCount), dataOut,
        minPackedValue.clone(), maxPackedValue.clone(), parentSplits,
        splitPackedValues, splitDimensionValues, leafBlockFPs,
        new int[config.maxPointsInLeafNode]);

    assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

    scratchBytesRef1.length = config.bytesPerDim;
    scratchBytesRef1.bytes = splitPackedValues;

    BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
      @Override
      public long getLeafLP(int index) {
        return leafBlockFPs[index];
      }

      @Override
      public BytesRef getSplitValue(int index) {
        scratchBytesRef1.offset = index * config.bytesPerDim;
        return scratchBytesRef1;
      }

      @Override
      public int getSplitDimension(int index) {
        return splitDimensionValues[index] & 0xff;
      }

      @Override
      public int numLeaves() {
        return leafBlockFPs.length;
      }
    };

    return () -> {
      try {
        // 把整棵树的索引，写到元数据文件和索引文件中去
        writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  private Runnable writeField1Dim(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, String fieldName, MutablePointValues reader) throws IOException {
    // 对点进行了重排序
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, Math.toIntExact(reader.size()));

    // 一个单个维度的bkdwriter
    final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    reader.intersect(new IntersectVisitor() {

      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {
        oneDimWriter.add(packedValue, docID);
      }

      @Override
      public void visit(int docID) {
        throw new IllegalStateException();
      }

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        return Relation.CELL_CROSSES_QUERY;
      }
    });

    // 开始的时候一个个加，最后finish是一个任务，我们拿到了可以等他完事，就相当于树搞好了.
    return oneDimWriter.finish();
  }

  /**
   * More efficient bulk-add for incoming {@link BKDReader}s.  This does a merge sort of the already
   * sorted values and currently only works when numDims==1.  This returns -1 if all documents containing
   * dimensional values were deleted.
   */
  public Runnable merge(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut, List<MergeState.DocMap> docMaps, List<BKDReader> readers) throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(config.bytesPerDim, readers.size());

    for (int i = 0; i < readers.size(); i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish();
  }

  // Reused when writing leaf blocks
  private final ByteBuffersDataOutput scratchOut = ByteBuffersDataOutput.newResettableInstance();

  // 一维的bkd的写入
  private class OneDimensionBKDWriter {

    // meta, index, data 三个文件
    final IndexOutput metaOut, indexOut, dataOut;
    // data文件的初始
    final long dataStartFP;
    // 叶子节点的块的文件位置列表
    final List<Long> leafBlockFPs = new ArrayList<>();
    // 叶子块的开始值，这个开始值是什么奇怪的东西
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    //　叶子节点的所有值，通过每个叶子的最大点数*每个点的长度计算
    final byte[] leafValues = new byte[config.maxPointsInLeafNode * config.packedBytesLength];
    // 叶子节点上的点对应的docIds. 通过点的计数器作为下标，　可以对应出每个点是哪个docId的
    final int[] leafDocs = new int[config.maxPointsInLeafNode];

    // 多少个值
    private long valueCount;
    // 叶子节点内的点的计数器
    private int leafCount;
    // 叶子的基数/ 好像也可以理解为叶子的势，就是有多少个子节点的那个势
    private int leafCardinality;

    // 初始化了点东西，没什么特殊的
    OneDimensionBKDWriter(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut) {
      if (config.numIndexDims != 1) {
        throw new UnsupportedOperationException("config.numIndexDims must be 1 but got " + config.numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (finished) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      this.dataOut = dataOut;
      this.dataStartFP = dataOut.getFilePointer();

      lastPackedValue = new byte[config.packedBytesLength];
    }

    // for asserts
    // 上一个点的值
    final byte[] lastPackedValue;
    // 上一个点的docId
    private int lastDocID;

    // 添加一个点进去
    void add(byte[] packedValue, int docID) throws IOException {
      // 检查一下是否有序的，也就是当前肯定要大于上一个.
      assert valueInOrder(config, valueCount + leafCount,
          0, lastPackedValue, packedValue, 0, docID, lastDocID);

      // 叶子上的点数量是0, 势直接+1
      // 要写入的点和上一个点不一样,势+1
      // 所以这个所谓的势，　是指这个叶子节点上，不同的点的个数,因为可能有很多点都是一样的
      if (leafCount == 0 || FutureArrays.mismatch(leafValues, (leafCount - 1) * config.bytesPerDim, leafCount * config.bytesPerDim, packedValue, 0, config.bytesPerDim) != -1) {
        leafCardinality++;
      }
      // 把当前给如的值，写入到叶子的值里面去
      System.arraycopy(packedValue, 0, leafValues, leafCount * config.packedBytesLength, config.packedBytesLength);
      // leaf count 是一个从0递增的数据，所以以他为下标
      leafDocs[leafCount] = docID;
      // 见过这个DocId了
      docsSeen.set(docID);
      // 叶子内部的点的数量的计数器
      leafCount++;

      // check
      if (valueCount + leafCount > totalPointCount) {
        throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + (valueCount + leafCount) + " values");
      }

      // 叶子节点上的点满了，　也就是一个叶子装好了
      if (leafCount == config.maxPointsInLeafNode) {
        // We write a block once we hit exactly the max count ... this is different from
        // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
        // 当前填满的这个叶子节点，写入到磁盘去.
        writeLeafBlock(leafCardinality);
        // 当前叶子节点上的势，和点的数量都清零
        leafCardinality = 0;
        leafCount = 0;
      }

      // docId 要大于0
      assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
    }

    // 完成一棵树的写入呗,返回的是一个runnable,可以等等异步操作之类的.
    public Runnable finish() throws IOException {
      // 如果当前叶子节点上还有数据点,把他写入
      if (leafCount > 0) {
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      // 总数为0，返回个null
      if (valueCount == 0) {
        return null;
      }

      // 总共写入了多少个point
      pointCount = valueCount;

      scratchBytesRef1.length = config.bytesPerDim;
      scratchBytesRef1.offset = 0;
      assert leafBlockStartValues.size() + 1 == leafBlockFPs.size();
      BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
        @Override
        public long getLeafLP(int index) {
          return leafBlockFPs.get(index);
        }

        @Override
        public BytesRef getSplitValue(int index) {
          // 第多少个切割点，就是第多少个叶子节点的第一个数据点
          scratchBytesRef1.bytes = leafBlockStartValues.get(index);
          return scratchBytesRef1;
        }

        @Override
        public int getSplitDimension(int index) {
          // 我是只有　一维的，所以永远是0咯
          return 0;
        }

        @Override
        public int numLeaves() {
          // 每个叶子的开始的文件指针，所以就是叶子的个数
          return leafBlockFPs.size();
        }
      };
      return () -> {
        try {
          writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    }

    // 把一个装满了数据点的叶子节点写入到磁盘去.
    private void writeLeafBlock(int leafCardinality) throws IOException {
      assert leafCount != 0;
      // 如果是第一个叶子节点，也就是第一次调用这个方法进行写入
      if (valueCount == 0) {
        // 当前维度最小的点
        System.arraycopy(leafValues, 0, minPackedValue, 0, config.packedIndexBytesLength);
      }
      // 当前维度最大的点
      System.arraycopy(leafValues, (leafCount - 1) * config.packedBytesLength, maxPackedValue, 0, config.packedIndexBytesLength);

      // 总数里加上本次叶子节点的数据点的值
      valueCount += leafCount;

      // 如果不是第一个叶子节点
      if (leafBlockFPs.size() > 0) {
        // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
        // 保存每个叶子节点上的最小的点，第一个叶子节点不保存，这份数据用于分析切割点
        leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength));
      }
      // 当前叶子节点的所有点开始存储的data文件的文件位置
      leafBlockFPs.add(dataOut.getFilePointer());
      // some check
      checkMaxLeafNodeCount(leafBlockFPs.size());

      // 寻找每一个维度上的公共前缀
      // Find per-dim common prefix:
      int offset = (leafCount - 1) * config.packedBytesLength;
      // 第一个和最后一个的公共前缀，由于是有序的，　所以这就是所有的数据的公共前缀
      int prefix = FutureArrays.mismatch(leafValues, 0, config.bytesPerDim, leafValues, offset, offset + config.bytesPerDim);
      // 如果=-1,说明是完全匹配的，也就是说，所有的值都一样.
      if (prefix == -1) {
        prefix = config.bytesPerDim;
      }

      // 由于这个方法只被一维的调用，所以这里就是写入了一维的公共前缀长度
      commonPrefixLengths[0] = prefix;

      assert scratchOut.size() == 0;
      // 节点上的所有点对应的docID进行写入,写到了草稿里
      writeLeafBlockDocs(scratchOut, leafDocs, 0, leafCount);
      // 写入公共前缀
      writeCommonPrefixes(scratchOut, commonPrefixLengths, leafValues);

      // 草稿1, 保存了节点上的所有点的值,
      // 但是长度只有一个点的值的长度那么长
      scratchBytesRef1.length = config.packedBytesLength;
      scratchBytesRef1.bytes = leafValues;

      // 一个lambda. 传入个啥玩意，给出一个byteRef
      final IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          // 这里不断的改变offset，就可以遍历这个叶子节点上的所有点了
          scratchBytesRef1.offset = config.packedBytesLength * i;
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(config, leafCount, 0, ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength),
          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * config.packedBytesLength, leafCount * config.packedBytesLength),
          packedValues, leafDocs, 0);
      // 把当前节点的所有数据，都写入到草稿里去, 里面用了特别花里胡哨的操作，看的差不多
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, leafCount, 0, packedValues, leafCardinality);
      // 从草稿挪到真正的输出，然后重置草稿
      scratchOut.copyTo(dataOut);
      scratchOut.reset();
    }
  }

  // 根据总的叶子的数量, 算出左边的子树的叶子节点数量.
  private int getNumLeftLeafNodes(int numLeaves) {
    assert numLeaves > 1 : "getNumLeftLeaveNodes() called with " + numLeaves;
    // return the level that can be filled with this number of leaves
    int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
    // how many leaf nodes are in the full level
    int leavesFullLevel = 1 << lastFullLevel;
    // half of the leaf nodes from the full level goes to the left
    int numLeftLeafNodes = leavesFullLevel / 2;
    // leaf nodes that do not fit in the full level
    int unbalancedLeafNodes = numLeaves - leavesFullLevel;
    // distribute unbalanced leaf nodes
    numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
    // we should always place unbalanced leaf nodes on the left
    // 所有不平衡的叶子节点，都应该放在左边
    assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
    return numLeftLeafNodes;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

  // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*config.bytesPerDim, config.bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

  // some check
  private void checkMaxLeafNodeCount(int numLeaves) {
    if (config.bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("too many nodes; increase config.maxPointsInLeafNode (currently " + config.maxPointsInLeafNode + ") and reindex");
    }
  }

  /**
   * Writes the BKD tree to the provided {@link IndexOutput}s and returns a {@link Runnable} that
   * writes the index of the tree if at least one point has been added, or {@code null} otherwise.
   */
  // 把bkd树写入到给定的输出中去. 如果至少写入了一个点,那就返回一个runnable.否则返回一个空.
  public Runnable finish(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut) throws IOException {
    // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + " heapWriter=" + heapPointWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on recurse...)

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    // 一个点都没有, 空!
    if (pointCount == 0) {
      return null;
    }

    //mark as finished
    finished = true;

    // 点关闭了,不让写了. 要开始build了.
    pointWriter.close();
    // 要开始选择半径什么的了
    BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
    //clean up pointers
    tempInput = null;
    pointWriter = null;

    // 叶子节点的总数
    final int numLeaves = Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);
    // 分隔点= 叶子节点数量-1
    final int numSplits = numLeaves - 1;

    // some check
    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a somewhat costly check at each
    // step of the recursion to recompute the split dim:

    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each recursion says which dim we split on.
    // 切割点的值
    byte[] splitPackedValues = new byte[Math.toIntExact((long) numSplits * config.bytesPerDim)];
    // 切割维度的值
    byte[] splitDimensionValues = new byte[numSplits];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g. 7)
    // 叶子节点的文件指针
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= config.maxPointsInLeafNode : "pointCount=" + pointCount + " numLeaves=" + numLeaves + " config.maxPointsInLeafNode=" + config.maxPointsInLeafNode;

    //We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector = new BKDRadixSelector(config, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    // data文件的起始位置
    final long dataStartFP = dataOut.getFilePointer();
    boolean success = false;
    try {

      // 父切割点
      final int[] parentSplits = new int[config.numIndexDims];
      build(0, numLeaves, points,
          dataOut, radixSelector,
          minPackedValue.clone(), maxPackedValue.clone(),
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          new int[config.maxPointsInLeafNode]);
      //
      assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      //long t2 = System.nanoTime();
      //System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (!success) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    // 多维度的数据处理方法
    // 所有的切割的值
    scratchBytesRef1.bytes = splitPackedValues;
    // 每一个维度的长度
    scratchBytesRef1.length = config.bytesPerDim;
    BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
      @Override
      public long getLeafLP(int index) {
        // 返回的是第x个叶子在data文件中的文件指针
        return leafBlockFPs[index];
      }

      @Override
      public BytesRef getSplitValue(int index) {
        // 第x个分隔点的值
        scratchBytesRef1.offset = index * config.bytesPerDim;
        return scratchBytesRef1;
      }

      @Override
      // 第x个切割点,使用的维度是哪个
      public int getSplitDimension(int index) {
        return splitDimensionValues[index] & 0xff;
      }

      @Override
      public int numLeaves() {
        // 叶子节点的数量
        return leafBlockFPs.length;
      }
    };

    // 多维度的一个树的索引,写入到元数据文件和索引文件中去
    return () -> {
      // Write index:
      try {
        writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * Packs the two arrays, representing a semi-balanced binary tree, into a compact byte[] structure.
   */
  // 打包两个数组，使用一个半平衡的二叉树，使用紧凑的字节数组结构
  private byte[] packIndex(BKDTreeLeafNodes leafNodes) throws IOException {
    /** Reused while packing the index */
    ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();

    // This is the "file" we append the byte[] to: // 一个假的，在内存的"文件"
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[config.bytesPerDim * config.numIndexDims];
    //System.out.println("\npack index");
    // 这里可以拿到整棵树的索引的字节长度
    int totalSize = recursePackIndex(writeBuffer, leafNodes, 0l, blocks, lastSplitValues, new boolean[config.numIndexDims], false,
        0, leafNodes.numLeaves());

    // Compact the byte[] blocks into single byte index:
    // 那么这就是所有的索引了
    byte[] index = new byte[totalSize];
    int upto = 0;
    for (byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    // 这相当于把整个树的索引全部放到了一个字节数组里面，可以根据自己的技巧读出来而已
    return index;
  }

  /**
   * Appends the current contents of writeBuffer as another block on the growing in-memory file
   */
  // 把当前的内容写到blocks里面
  private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) {
    byte[] block = writeBuffer.toArrayCopy();
    blocks.add(block);
    writeBuffer.reset();
    return block.length;
  }

  /**
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each inner node
   * // 上一个切割点是每个维度的切割垫，　我们使用这种前缀编码来编码每一个内部节点
   * // 这个递归的玩意，必须一直到只有一个叶子才结束
   */
  // 递归的打包索引
  private int recursePackIndex(ByteBuffersDataOutput writeBuffer, BKDTreeLeafNodes leafNodes, long minBlockFP, List<byte[]> blocks,
                               byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft, int leavesOffset, int numLeaves) throws IOException {
    // 只有一个叶子
    if (numLeaves == 1) {
      if (isLeft) {
        assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
        // 只有一个叶子且是左叶子的话，就可以结束递归了
        return 0;
      } else {
        long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;

        // 写一个第一个叶子节点的增量,
        writeBuffer.writeVLong(delta);
        // 相当于把上面写的这个增量挪到blocks里面去
        // 如果只有一个节点，还是右叶子的话，也是可以结束递归的，只是要记录一下这个节点到上一个点的偏移增量嘛
        return appendBlock(writeBuffer, blocks);
      }
    } else {
      // 可以说很多叶子了
      // 叶子的文件指针
      long leftBlockFP;
      if (isLeft) {
        // The left tree's left most leaf block FP is always the minimal FP:
        assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
        leftBlockFP = minBlockFP;
      } else {
        // 从指针里面开始拿了
        leftBlockFP = leafNodes.getLeafLP(leavesOffset);
        // 增量编码
        long delta = leftBlockFP - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        // 写进去增量编码
        writeBuffer.writeVLong(delta);
      }

      // 一共这么多叶子，你说应该有多少在左边呢
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // 右边的叶子的偏移量，也就是第一个在右边的叶子的编号
      final int rightOffset = leavesOffset + numLeftLeafNodes;
      // 分割点
      final int splitOffset = rightOffset - 1;

      // 获取切割维度
      int splitDim = leafNodes.getSplitDimension(splitOffset);
      // 切割点的数据
      BytesRef splitValue = leafNodes.getSplitValue(splitOffset);
      // 切割点的开始偏移量
      int address = splitValue.offset;

      //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, config.bytesPerDim));

      // find common prefix with last split value in this dim:
      // 这次的切割点和上一个切割点的公共前缀
      int prefix = FutureArrays.mismatch(splitValue.bytes, address, address + config.bytesPerDim, lastSplitValues,
          splitDim * config.bytesPerDim, splitDim * config.bytesPerDim + config.bytesPerDim);
      // 如果全一样，公共前缀就是全部的长度
      if (prefix == -1) {
        prefix = config.bytesPerDim;
      }

      //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " config.bytesPerDim=" + config.bytesPerDim + " prefix=" + prefix);

      // 第一个不一样的字节增量
      int firstDiffByteDelta;
      if (prefix < config.bytesPerDim) {
        // 不是和上一个切割点完全一样
        //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * config.bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
        // 第一个不一样的字节大于了多少???
        firstDiffByteDelta = (splitValue.bytes[address + prefix] & 0xFF) - (lastSplitValues[splitDim * config.bytesPerDim + prefix] & 0xFF);
        if (negativeDeltas[splitDim]) {
          firstDiffByteDelta = -firstDiffByteDelta;
        }
        //System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0;
      } else {
        firstDiffByteDelta = 0;
      }

      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      // 就像他说的，　把这几个内容搞到一个int里面去，就这还要用vint来写,太过分了
      int code = (firstDiffByteDelta * (1 + config.bytesPerDim) + prefix) * config.numIndexDims + splitDim;

      //System.out.println("  code=" + code);
      //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, config.bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      // 当前切割点和上一个不一样的后缀
      // 写入到缓冲区去
      int suffix = config.bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];
      if (suffix > 1) {
        writeBuffer.writeBytes(splitValue.bytes, address + prefix + 1, suffix - 1);
      }

      byte[] cmp = lastSplitValues.clone();

      // 把上一个切割点的后缀也保留下来干啥呢
      System.arraycopy(lastSplitValues, splitDim * config.bytesPerDim + prefix, savSplitValue, 0, suffix);

      // copy our split value into lastSplitValues for our children to prefix-code against
      // 把当前切割点的后缀放到上一个的位置，　让孩子节点也能用
      System.arraycopy(splitValue.bytes, address + prefix, lastSplitValues, splitDim * config.bytesPerDim + prefix, suffix);

      // 把缓冲写到blocks里面
      int numBytes = appendBlock(writeBuffer, blocks);

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
      // quickly seek to its starting point
      // 写入了一个null. 意味着左子树写完了，在搜索的时候，如果只需要遍历右子树，我们可以快速找到这个点,也就是右子树的开始点
      int idxSav = blocks.size();
      blocks.add(null);

      boolean savNegativeDelta = negativeDeltas[splitDim];
      // 反向增量？？？？
      negativeDeltas[splitDim] = true;


      // 这里就是递归咯，递归的看一下左子树一共用了多少字节呢
      int leftNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, true,
          leavesOffset, numLeftLeafNodes);

      // 只要左边的节点不是只有一个,写一下左子树总共的字节长度
      if (numLeftLeafNodes != 1) {
        writeBuffer.writeVInt(leftNumBytes);
      } else {
        assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
      }

      byte[] bytes2 = writeBuffer.toArrayCopy();
      writeBuffer.reset();
      // replace our placeholder:
      // 在左右子树中间，放着的是左子树的字节长度
      blocks.set(idxSav, bytes2);

      // 反向增量，这玩意是用来控制左右的
      // 现在来搞右子树了
      negativeDeltas[splitDim] = false;
      int rightNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, false,
          rightOffset, numLeaves - numLeftLeafNodes);

      // 搞完右子树，把人家的原有的左右给人家恢复回来
      negativeDeltas[splitDim] = savNegativeDelta;

      // restore lastSplitValues to what caller originally passed us:
      // 相当于是保留了所有的切割垫，每次左右搞完，那么当前的切割点递归的给孩子用，上一个就可以保存起来了。那么最后就可以拿到所有的切割点
      System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * config.bytesPerDim + prefix, suffix);

      assert Arrays.equals(lastSplitValues, cmp);

      // 返回的总的长度,这个值也是递归的值
      // 父节点等于左右子节点加上自己的一些数据，那么从树根开始是一个最大的值，之后左右都慢慢小了下去.
      return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
    }
  }

  private void writeIndex(IndexOutput metaOut, IndexOutput indexOut, int countPerLeaf, BKDTreeLeafNodes leafNodes, long dataStartFP) throws IOException {
    // 这是整颗树的索引.全部放在了一个字节数组中
    byte[] packedIndex = packIndex(leafNodes);
    writeIndex(metaOut, indexOut, countPerLeaf, leafNodes.numLeaves(), packedIndex, dataStartFP);
  }

  private void writeIndex(IndexOutput metaOut, IndexOutput indexOut, int countPerLeaf, int numLeaves, byte[] packedIndex, long dataStartFP) throws IOException {
    // 给元数据的文件写一个header
    CodecUtil.writeHeader(metaOut, CODEC_NAME, VERSION_CURRENT);
    // 写入维度
    metaOut.writeVInt(config.numDims);
    // 维度
    metaOut.writeVInt(config.numIndexDims);
    // 每个叶子的最大point数量
    metaOut.writeVInt(countPerLeaf);
    // 配置里面的每个维度的字节长度
    metaOut.writeVInt(config.bytesPerDim);

    // 叶子节点必须大于0
    assert numLeaves > 0;
    // 叶子的数量
    metaOut.writeVInt(numLeaves);
    // 最小的值
    metaOut.writeBytes(minPackedValue, 0, config.packedIndexBytesLength);
    // 最大的一个点
    metaOut.writeBytes(maxPackedValue, 0, config.packedIndexBytesLength);

    // 总的点数
    metaOut.writeVLong(pointCount);
    // 出现的总的文档数量
    metaOut.writeVInt(docsSeen.cardinality());
    // 打包好的索引字节的长度
    metaOut.writeVInt(packedIndex.length);

    // data文件的开始文件位置
    metaOut.writeLong(dataStartFP);
    // If metaOut and indexOut are the same file, we account for the fact that
    // writing a long makes the index start 8 bytes later.
    // 如果索引文件等于元数据文件,那就多存一个long的长度,否则这里记录的是索引文件的开始指针
    metaOut.writeLong(indexOut.getFilePointer() + (metaOut == indexOut ? Long.BYTES : 0));

    // 把整个索引的字节数组存储到索引文件中去
    indexOut.writeBytes(packedIndex, 0, packedIndex.length);
  }

  // 写入叶子节点上的点对应的所有docId
  private void writeLeafBlockDocs(DataOutput out, int[] docIDs, int start, int count) throws IOException {
    assert count > 0 : "config.maxPointsInLeafNode=" + config.maxPointsInLeafNode;
    // 写入一个数量.
    out.writeVInt(count);
    // 写入的具体操作看了，　这里只需要知道写入了就ok
    DocIdsWriter.writeDocIds(docIDs, start, count, out);
  }

  // 写入某个叶子节点上的所有点的值，这些值都被编码成了byte
  private void writeLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int leafCardinality) throws IOException {
    // 每一个维度的公共前缀长度求和
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();

    // 如果每一个维度的公共前缀的长度加起来，　和一个数据点打包后的长度一样，那么说明所有数据是一样的，存个-1得了
    if (prefixLenSum == config.packedBytesLength) {
      // all values in this block are equal
      out.writeByte((byte) -1);
    } else {
      // 必须小于每个维度的长度
      assert commonPrefixLengths[sortedDim] < config.bytesPerDim;
      // estimate if storing the values with cardinality is cheaper than storing all values.
      int compressedByteOffset = sortedDim * config.bytesPerDim + commonPrefixLengths[sortedDim];
      // 算一下这个叶子节点上的值的相似度，以此来决定是否要存储所有的值，还是说存储一下势就好了，也就是压缩不压缩
      int highCardinalityCost;
      int lowCardinalityCost;
      // 节点的势等于节点的数量，说明这个叶子上的所有值都是不一样的.
      if (count == leafCardinality) {
        // all values in this block are different
        highCardinalityCost = 0;
        lowCardinalityCost = 1;
      } else {
        // compute cost of runLen compression
        int numRunLens = 0;
        for (int i = 0; i < count; ) {
          // do run-length compression on the byte at compressedByteOffset
          // 其实就是找到相同高位的最长的位置. 用string来距离[111,111,1112,1122'],那么找到的就是前三个，因为都是111开头的，
          // 相当于是，找一下当前这次压缩能连续压多少个，一次性压缩写入，之后的又是另外一批前缀差不多的.
          int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
          assert runLen <= 0xff;
          numRunLens++;
          i += runLen;
        }
        // Add cost of runLen compression
        highCardinalityCost = count * (config.packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;
        // +1 is the byte needed for storing the cardinality
        lowCardinalityCost = leafCardinality * (config.packedBytesLength - prefixLenSum + 1);
      }
      if (lowCardinalityCost <= highCardinalityCost) {
        // 如果值得压缩，存个标记
        out.writeByte((byte) -2);
        writeLowCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, packedValues);
      } else {
        // 如果不值得压缩，存一下当前维度的编号, 然后存储所有值
        out.writeByte((byte) sortedDim);
        writeHighCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, compressedByteOffset);
      }
    }
  }

  // 目的是为了存储叶子节点上的所有point的打包后的字节数组
  // 特殊的是，低的势，代表了所有point中相等的值越多
  private void writeLowCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
    // 如果不是只有一维，就写入个边界
    if (config.numIndexDims != 1) {
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    }
    // 第一个
    BytesRef value = packedValues.apply(0);
    // 写入草稿１
    System.arraycopy(value.bytes, value.offset, scratch1, 0, config.packedBytesLength);
    int cardinality = 1;
    for (int i = 1; i < count; i++) {
      // 迭代的拿到当前节点的每一个point
      value = packedValues.apply(i);
      // 对这个point进行所有维度的写入
      for (int dim = 0; dim < config.numDims; dim++) {
        final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
        final int end = dim * config.bytesPerDim + config.bytesPerDim;
        // 当前的point的当前维度和上一个point的当前维度一样不
        if (FutureArrays.mismatch(value.bytes, value.offset + start, value.offset + end, scratch1, start, end) != -1) {
          // 不一样，写一下当前的势
          out.writeVInt(cardinality);
          // 然后把当前点写入
          for (int j = 0; j < config.numDims; j++) {
            out.writeBytes(scratch1, j * config.bytesPerDim + commonPrefixLengths[j], config.bytesPerDim - commonPrefixLengths[j]);
          }
          // 记录上一个值
          System.arraycopy(value.bytes, value.offset, scratch1, 0, config.packedBytesLength);
          cardinality = 1;
          break;
        } else if (dim == config.numDims - 1) {
          // 如果当前值和上一个完全一样并且当前维度是最后一个维度,势+1
          cardinality++;
        }
      }
    }
    // 写一下势
    out.writeVInt(cardinality);
    // 写一下最后一个点
    for (int i = 0; i < config.numDims; i++) {
      out.writeBytes(scratch1, i * config.bytesPerDim + commonPrefixLengths[i], config.bytesPerDim - commonPrefixLengths[i]);
    }
  }

  // 目的是为了存储叶子节点上的所有point的打包后的字节数组
  // 特殊的是，高的势，代表了所有的point一样的很少
  private void writeHighCardinalityLeafBlockPackedValues(DataOutput out, int[] commonPrefixLengths, int count, int sortedDim, IntFunction<BytesRef> packedValues, int compressedByteOffset) throws IOException {
    // 如果不是只有一维，写入一个边界
    if (config.numIndexDims != 1) {
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    }
    // 这是啥子意思
    commonPrefixLengths[sortedDim]++;
    for (int i = 0; i < count; ) {
      // do run-length compression on the byte at compressedByteOffset
      int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
      assert runLen <= 0xff;
      // 遍历的拿到每一个点
      BytesRef first = packedValues.apply(i);
      byte prefixByte = first.bytes[first.offset + compressedByteOffset];
      // 压缩的长度
      out.writeByte(prefixByte);
      // runLen
      out.writeByte((byte) runLen);
      writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
      i += runLen;
      assert i <= count;
    }
  }

  // 写一个实际的边界, 也就是这堆值里的最小最大都写入了
  private void writeActualBounds(DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues) throws IOException {
    // 每个维度遍历
    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      // 当前维度的公共前缀
      int commonPrefixLength = commonPrefixLengths[dim];
      // 后缀长度
      int suffixLength = config.bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        // 0 = 最小的点.  1=最大的点
        BytesRef[] minMax = computeMinMax(count, packedValues, dim * config.bytesPerDim + commonPrefixLength, suffixLength);
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        // 最小最大都写进去，还写了他的偏移量和长度
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /**
   * Return an array that contains the min and max values for the [offset, offset+length] interval
   * of the given {@link BytesRef}s.
   */
  private static BytesRef[] computeMinMax(int count, IntFunction<BytesRef> packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    // 迭代器开始啦, 先把最小最大都搞成第一个
    BytesRef first = packedValues.apply(0);
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);

    for (int i = 1; i < count; ++i) {
      // 迭代的拿到下一个点的值
      BytesRef candidate = packedValues.apply(i);
      if (FutureArrays.compareUnsigned(min.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) > 0) {
        // 最小的点
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (FutureArrays.compareUnsigned(max.bytes(), 0, length, candidate.bytes, candidate.offset + offset, candidate.offset + offset + length) < 0) {
        // 最大的点
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[]{min.get(), max.get()};
  }

  // 写入当前节点的所有数据点的值的范围？？？？
  private void writeLeafBlockPackedValuesRange(DataOutput out, int[] commonPrefixLengths, int start, int end, IntFunction<BytesRef> packedValues) throws IOException {
    for (int i = start; i < end; ++i) {
      // 遍历拿到值
      BytesRef ref = packedValues.apply(i);
      assert ref.length == config.packedBytesLength;

      // 每个维度进行写入
      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
//       // 每个节点写入不同的后缀的值
        out.writeBytes(ref.bytes, ref.offset + dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
    }
  }

  private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset) {
    // 第一个
    BytesRef first = packedValues.apply(start);
    byte b = first.bytes[first.offset + byteOffset];
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) {
        return i - start;
      }
    }
    return end - start;
  }

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue) throws IOException {
    for (int dim = 0; dim < config.numDims; dim++) {
      // 写一个公共前缀的长度.
      out.writeVInt(commonPrefixes[dim]);
      //System.out.println(commonPrefixes[dim] + " of " + config.bytesPerDim);
      // 写入公共前缀
      out.writeBytes(packedValue, dim * config.bytesPerDim, commonPrefixes[dim]);
    }
  }

  @Override
  public void close() throws IOException {
    finished = true;
    if (tempInput != null) {
      // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
      try {
        tempInput.close();
      } finally {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      }
    }
  }

  /**
   * Called on exception, to check whether the checksum is also corrupt in this source, and add that
   * information (checksum matched or didn't) as a suppressed exception.
   */
  private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
    assert priorException != null;

    // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
    // right reader after recursing to children, and possibly within recursed children,
    // since all together they make a single pass through the file.  But this is a sizable re-org,
    // and would mean leaving readers (IndexInputs) open for longer:
    if (writer instanceof OfflinePointWriter) {
      // We are reading from a temp file; go verify the checksum:
      String tempFileName = ((OfflinePointWriter) writer).name;
      if (tempDir.getCreatedFiles().contains(tempFileName)) {
        try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
          CodecUtil.checkFooter(in, priorException);
        }
      }
    }

    // We are reading from heap; nothing to add:
    throw IOUtils.rethrowAlways(priorException);
  }

  /**
   * Pick the next dimension to split.
   *
   * @param minPackedValue the min values for all dimensions
   * @param maxPackedValue the max values for all dimensions
   * @param parentSplits   how many times each dim has been split on the parent levels
   * @return the dimension to split
   */
  protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
    // First look at whether there is a dimension that has split less than 2x less than
    // the dim that has most splits, and return it if there is such a dimension and it
    // does not only have equals values. This helps ensure all dimensions are indexed.
    // 找到已经被分割的次数最多的维度,是多少次
    int maxNumSplits = 0;
    for (int numSplits : parentSplits) {
      maxNumSplits = Math.max(maxNumSplits, numSplits);
    }

    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      final int offset = dim * config.bytesPerDim;
      // 从低1个维度开始找, 如果有一个维度满足: 他的分割次数是最大次数的一半不到,并且这个维度上的值不是全部一样的, 那就是选他了.
      if (parentSplits[dim] < maxNumSplits / 2 &&
          FutureArrays.compareUnsigned(minPackedValue, offset, offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) != 0) {
        return dim;
      }
    }

    // Find which dim has the largest span so we can split on it:
    // 找到跨度最大的那个维度, 就它了
    int splitDim = -1;
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      NumericUtils.subtract(config.bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      if (splitDim == -1 || FutureArrays.compareUnsigned(scratchDiff, 0, config.bytesPerDim, scratch1, 0, config.bytesPerDim) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, config.bytesPerDim);
        splitDim = dim;
      }
    }

    //System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  /**
   * Pull a partition back into heap once the point count is low enough while recursing.
   */
  private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
    int count = Math.toIntExact(source.count());
    try (PointReader reader = source.getReader(0, source.count());
         HeapPointWriter writer = new HeapPointWriter(config, count)) {
      for (int i = 0; i < count; i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.pointValue());
      }
      source.destroy();
      return writer;
    } catch (Throwable t) {
      throw verifyChecksum(t, source);
    }
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  // 递归的排序，写入bkd树.
  // 这个方法用来直接写入一个新的segment
  private void build(int leavesOffset, int numLeaves,
                     MutablePointValues reader, int from, int to,
                     IndexOutput out,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     byte[] splitDimensionValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) {
      // 只有一个叶子，要结束了
      // leaf node
      final int count = to - from;
      assert count <= config.maxPointsInLeafNode;

      // Compute common prefixes
      Arrays.fill(commonPrefixLengths, config.bytesPerDim);
      reader.getValue(from, scratchBytesRef1);
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, scratchBytesRef2);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int offset = dim * config.bytesPerDim;
          int dimensionPrefixLength = commonPrefixLengths[dim];
          commonPrefixLengths[dim] = FutureArrays.mismatch(scratchBytesRef1.bytes, scratchBytesRef1.offset + offset,
              scratchBytesRef1.offset + offset + dimensionPrefixLength,
              scratchBytesRef2.bytes, scratchBytesRef2.offset + offset,
              scratchBytesRef2.offset + offset + dimensionPrefixLength);
          if (commonPrefixLengths[dim] == -1) {
            commonPrefixLengths[dim] = dimensionPrefixLength;
          }
        }
      }

      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      for (int i = from + 1; i < to; ++i) {
        for (int dim = 0; dim < config.numDims; dim++) {
          if (usedBytes[dim] != null) {
            byte b = reader.getByteAt(i, dim * config.bytesPerDim + commonPrefixLengths[dim]);
            usedBytes[dim].set(Byte.toUnsignedInt(b));
          }
        }
      }
      // 挑选本次的排序维度
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (usedBytes[dim] != null) {
          final int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort by sortedDim
      // 根据维度进行排序
      MutablePointsReaderUtils.sortByDim(config, sortedDim, commonPrefixLengths,
          reader, from, to, scratchBytesRef1, scratchBytesRef2);

      BytesRef comparator = scratchBytesRef1;
      BytesRef collector = scratchBytesRef2;
      reader.getValue(from, comparator);
      int leafCardinality = 1;
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, collector);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int start = dim * config.bytesPerDim + commonPrefixLengths[dim];
          final int end = dim * config.bytesPerDim + config.bytesPerDim;
          if (FutureArrays.mismatch(collector.bytes, collector.offset + start, collector.offset + end,
              comparator.bytes, comparator.offset + start, comparator.offset + end) != -1) {
            leafCardinality++;
            BytesRef scratch = collector;
            collector = comparator;
            comparator = scratch;
            break;
          }
        }
      }
      // Save the block file pointer:
      // 记录下当前叶子节点，在data文件中的指针
      leafBlockFPs[leavesOffset] = out.getFilePointer();

      assert scratchOut.size() == 0;

      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i);
      }
      //System.out.println("writeLeafBlock pos=" + out.getFilePointer());
      // 把docId全部写入到草稿里，写入的方式还挺花里胡哨，我看了一些
      writeLeafBlockDocs(scratchOut, docIDs, 0, count);

      // Write the common prefixes:
      // 公共前缀写入
      reader.getValue(from, scratchBytesRef1);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, config.packedBytesLength);
      writeCommonPrefixes(scratchOut, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        @Override
        public BytesRef apply(int i) {
          reader.getValue(from + i, scratchBytesRef1);
          return scratchBytesRef1;
        }
      };
      assert valuesInOrderAndBounds(config, count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      // 写入叶子节点上的所有数据点的实际的值
      writeLeafBlockPackedValues(scratchOut, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);
      // 从草稿写入到真实的data文件，然后清空草稿
      scratchOut.copyTo(out);
      scratchOut.reset();
    } else {
      // inner node

      // 切割维度
      final int splitDim;
      // compute the split dimension and partition around it
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (numLeaves != leafBlockFPs.length && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      // How many leaves will be in the left tree:
      // 左子树的叶子节点个数
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      // 多少个数据点在左子树里
      final int mid = from + numLeftLeafNodes * config.maxPointsInLeafNode;

      // 计算公共前缀的长度
      // 所以就是最小的值和最大的值的公共前缀的长度
      int commonPrefixLen = FutureArrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      // 分区吧
      MutablePointsReaderUtils.partition(config, maxDoc, splitDim, commonPrefixLen,
          reader, from, to, mid, scratchBytesRef1, scratchBytesRef2);

      // 右子树的开始偏移量
      final int rightOffset = leavesOffset + numLeftLeafNodes;
      // 分割节点的偏移量是右子树的-1
      final int splitOffset = rightOffset - 1;
      // set the split value
      // 什么万一的地址
      final int address = splitOffset * config.bytesPerDim;
      // 这个位置的切割点,用的是哪个维度的,记录一下
      splitDimensionValues[splitOffset] = (byte) splitDim;
      reader.getValue(mid, scratchBytesRef1);
      // 切割点的数据找到了
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim, splitPackedValues, address, config.bytesPerDim);

      // 最小的切割点
      byte[] minSplitPackedValue = ArrayUtil.copyOfSubArray(minPackedValue, 0, config.packedIndexBytesLength);
      // 最大的切割点
      byte[] maxSplitPackedValue = ArrayUtil.copyOfSubArray(maxPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(scratchBytesRef1.bytes, scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      // recurse
      // 所有的切割点在各个维度上的计数+1
      parentSplits[splitDim]++;
      // 递归构造左子树
      build(leavesOffset, numLeftLeafNodes, reader, from, mid, out,
          minPackedValue, maxSplitPackedValue, parentSplits,
          splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      // 递归的构建右子树
      build(rightOffset, numLeaves - numLeftLeafNodes, reader, mid, to, out,
          minSplitPackedValue, maxPackedValue, parentSplits,
          splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);
      // 两边都构建完,就减去对应维度的切割次数吧.
      parentSplits[splitDim]--;
    }
  }


  // 这个看过了吧
  private void computePackedValueBounds(BKDRadixSelector.PathSlice slice, byte[] minPackedValue, byte[] maxPackedValue) throws IOException {
    try (PointReader reader = slice.writer.getReader(slice.start, slice.count)) {
      if (reader.next() == false) {
        return;
      }
      BytesRef value = reader.pointValue().packedValue();
      System.arraycopy(value.bytes, value.offset, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(value.bytes, value.offset, maxPackedValue, 0, config.packedIndexBytesLength);
      while (reader.next()) {
        value = reader.pointValue().packedValue();
        for (int dim = 0; dim < config.numIndexDims; dim++) {
          final int startOffset = dim * config.bytesPerDim;
          final int endOffset = startOffset + config.bytesPerDim;
          if (FutureArrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, minPackedValue, startOffset, endOffset) < 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, minPackedValue, startOffset, config.bytesPerDim);
          } else if (FutureArrays.compareUnsigned(value.bytes, value.offset + startOffset, value.offset + endOffset, maxPackedValue, startOffset, endOffset) > 0) {
            System.arraycopy(value.bytes, value.offset + startOffset, maxPackedValue, startOffset, config.bytesPerDim);
          }
        }
      }
    }
  }

  /**
   * The point writer contains the data that is going to be splitted using radix selection.
   * /*  This method is used when we are merging previously written segments, in the numDims > 1 case.
   */
  // point的写入这包含了所有的数据
  // 这个方法用在合并之前写好的分片时, 并且维度要大于1,如果=1的话,用OneDim那个写入者.
  private void build(int leavesOffset, int numLeaves,
                     BKDRadixSelector.PathSlice points,
                     IndexOutput out,
                     BKDRadixSelector radixSelector,
                     byte[] minPackedValue, byte[] maxPackedValue,
                     int[] parentSplits,
                     byte[] splitPackedValues,
                     byte[] splitDimensionValues,
                     long[] leafBlockFPs,
                     int[] spareDocIds) throws IOException {

    if (numLeaves == 1) {
      // 只有一个叶子节点, 玩啥啊

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more efficient
      // 强转一个类型呗. 这里面有当前叶子的所有实际数据哦,所以之后得看这个呢
      HeapPointWriter heapSource;
      if (points.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. merging big segments with most of the points deleted
        heapSource = switchToHeap(points.writer);
      } else {
        heapSource = (HeapPointWriter) points.writer;
      }

      int from = Math.toIntExact(points.start);
      int to = Math.toIntExact(points.start + points.count);
      //we store common prefix on scratch1
      // 计算一下公共前缀的长度
      computeCommonPrefixLength(heapSource, scratch1, from, to);

      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;

      // 每一个维度的使用过的字节??
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }

      //Find the dimension to compress
      for (int dim = 0; dim < config.numDims; dim++) {
        // 当前维度的公共前缀长度
        int prefix = commonPrefixLengths[dim];
        if (prefix < config.bytesPerDim) {
          int offset = dim * config.bytesPerDim;
          for (int i = from; i < to; ++i) {
            // 拿到每一个点的值
            PointValue value = heapSource.getPackedValueSlice(i);
            BytesRef packedValue = value.packedValue();
            // 算一下在每隔维度上, 每一个值的后缀.是多少
            int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
            usedBytes[dim].set(bucket);
          }
          // 当前维度上, 不一样的值有多少
          int cardinality = usedBytes[dim].cardinality();
          // 找到最小的不同点的那个维度
          // 每个维度上的点可能有不同的值,可能有重复的值,找到重复值最多的那个
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort the chosen dimension
      // 对这个维度进行排序
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
      // compute cardinality
      int leafCardinality = heapSource.computeCardinality(from, to, commonPrefixLengths);

      // Save the block file pointer:
      // 记录叶子节点的data文件偏移位置
      leafBlockFPs[leavesOffset] = out.getFilePointer();
      //System.out.println("  write leaf block @ fp=" + out.getFilePointer());

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      int count = to - from;
      assert count > 0 : "numLeaves=" + numLeaves + " leavesOffset=" + leavesOffset;
      assert count <= spareDocIds.length : "count=" + count + " > length=" + spareDocIds.length;
      // Write doc IDs
      // 一个叶子上的所有点对应的docIds.
      int[] docIDs = spareDocIds;
      for (int i = 0; i < count; i++) {
        docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
      }
      // 把这个叶子上的所有的点的docIds存起来
      writeLeafBlockDocs(out, docIDs, 0, count);

      // TODO: minor opto: we don't really have to write the actual common prefixes, because BKDReader on recursing can regenerate it for us
      // from the index, much like how terms dict does so from the FST:

      // Write the common prefixes:
      // 写入公共前缀
      writeCommonPrefixes(out, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues = new IntFunction<BytesRef>() {
        final BytesRef scratch = new BytesRef();

        {
          scratch.length = config.packedBytesLength;
        }

        @Override
        public BytesRef apply(int i) {
          PointValue value = heapSource.getPackedValueSlice(from + i);
          return value.packedValue();
        }
      };
      assert valuesInOrderAndBounds(config, count, sortedDim, minPackedValue, maxPackedValue, packedValues,
          docIDs, 0);
      // 叶子节点的实际的值, 写入,方法分析过了
      writeLeafBlockPackedValues(out, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);

    } else {
      // Inner node: partition/recurse
      // 内部节点, 分区和递归

      final int splitDim;
      // 只有一维的话
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        // 控制一下计算量, 有点累. 这里就是计算应该用哪个维度分割
        if (numLeaves != leafBlockFPs.length && config.numIndexDims > 2 && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(points, minPackedValue, maxPackedValue);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      assert numLeaves <= leafBlockFPs.length : "numLeaves=" + numLeaves + " leafBlockFPs.length=" + leafBlockFPs.length;

      // How many leaves will be in the left tree:
      final int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final long leftCount = numLeftLeafNodes * config.maxPointsInLeafNode;

      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

      int commonPrefixLen = FutureArrays.mismatch(minPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim, maxPackedValue, splitDim * config.bytesPerDim,
          splitDim * config.bytesPerDim + config.bytesPerDim);
      if (commonPrefixLen == -1) {
        commonPrefixLen = config.bytesPerDim;
      }

      byte[] splitValue = radixSelector.select(points, slices, points.start, points.start + points.count, points.start + leftCount, splitDim, commonPrefixLen);

      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitValueOffset = rightOffset - 1;

      splitDimensionValues[splitValueOffset] = (byte) splitDim;
      int address = splitValueOffset * config.bytesPerDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address, config.bytesPerDim);

      byte[] minSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, config.packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, config.packedIndexBytesLength);

      System.arraycopy(splitValue, 0, minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(splitValue, 0, maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(leavesOffset, numLeftLeafNodes, slices[0],
          out, radixSelector, minPackedValue, maxSplitPackedValue,
          parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      // Recurse on right tree:
      build(rightOffset, numLeaves - numLeftLeafNodes, slices[1],
          out, radixSelector, minSplitPackedValue, maxPackedValue,
          parentSplits, splitPackedValues, splitDimensionValues, leafBlockFPs, spareDocIds);

      parentSplits[splitDim]--;
    }
  }

  // 给多维数据,在每个维度上计算一个公共前缀的长度,用的都是mismatch
  private void computeCommonPrefixLength(HeapPointWriter heapPointWriter, byte[] commonPrefix, int from, int to) {
    // 填充
    Arrays.fill(commonPrefixLengths, config.bytesPerDim);
    // 一个多维的数据点
    PointValue value = heapPointWriter.getPackedValueSlice(from);
    // 获取多维数据点的实际值
    BytesRef packedValue = value.packedValue();
    // 第一个点的多维数据都拷贝过去,拷贝到公共前缀的字节上
    for (int dim = 0; dim < config.numDims; dim++) {
      System.arraycopy(packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, commonPrefix, dim * config.bytesPerDim, config.bytesPerDim);
    }
    for (int i = from + 1; i < to; i++) {
      // 相当于前面的那部操作的迭代版本, 1+后面的
      value = heapPointWriter.getPackedValueSlice(i);
      packedValue = value.packedValue();
      for (int dim = 0; dim < config.numDims; dim++) {
        if (commonPrefixLengths[dim] != 0) {
          int j = FutureArrays.mismatch(commonPrefix, dim * config.bytesPerDim, dim * config.bytesPerDim + commonPrefixLengths[dim], packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, packedValue.offset + dim * config.bytesPerDim + commonPrefixLengths[dim]);
          if (j != -1) {
            commonPrefixLengths[dim] = j;
          }
        }
      }
    }
  }

  // only called from assert
  private static boolean valuesInOrderAndBounds(BKDConfig config, int count, int sortedDim, byte[] minPackedValue, byte[] maxPackedValue,
                                                IntFunction<BytesRef> values, int[] docs, int docsOffset) {
    byte[] lastPackedValue = new byte[config.packedBytesLength];
    int lastDoc = -1;
    for (int i = 0; i < count; i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == config.packedBytesLength;
      assert valueInOrder(config, i, sortedDim, lastPackedValue, packedValue.bytes, packedValue.offset,
          docs[docsOffset + i], lastDoc);
      lastDoc = docs[docsOffset + i];

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(config, packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private static boolean valueInOrder(BKDConfig config, long ord, int sortedDim, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset,
                                      int doc, int lastDoc) {
    // 根据每一维度的大小，　能算出来这个维度存的位置
    int dimOffset = sortedDim * config.bytesPerDim;
    if (ord > 0) {
      // 比较上一个值
      int cmp = FutureArrays.compareUnsigned(lastPackedValue, dimOffset, dimOffset + config.bytesPerDim, packedValue, packedValueOffset + dimOffset, packedValueOffset + dimOffset + config.bytesPerDim);
      // 值也必须是有序的
      if (cmp > 0) {
        throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength) + " ord=" + ord);
      }
      // 如果值一样，就比较啥
      if (cmp == 0 && config.numDims > config.numIndexDims) {
        cmp = FutureArrays.compareUnsigned(lastPackedValue, config.packedIndexBytesLength, config.packedBytesLength, packedValue, packedValueOffset + config.packedIndexBytesLength, packedValueOffset + config.packedBytesLength);
        // 也必须有序
        if (cmp > 0) {
          throw new AssertionError("data values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength) + " ord=" + ord);
        }
      }
      // 如果值必须一样，那么doc必须有序
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, config.packedBytesLength);
    return true;
  }

  // only called from assert
  private static boolean valueInBounds(BKDConfig config, BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      int offset = config.bytesPerDim * dim;
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, minPackedValue, offset, offset + config.bytesPerDim) < 0) {
        return false;
      }
      if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset + offset, packedValue.offset + offset + config.bytesPerDim, maxPackedValue, offset, offset + config.bytesPerDim) > 0) {
        return false;
      }
    }

    return true;
  }
}
