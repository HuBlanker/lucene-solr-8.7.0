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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.RadixSelector;
import org.apache.lucene.util.Selector;
import org.apache.lucene.util.Sorter;

/**
 *
 * Offline Radix selector for BKD tree.
 *
 * 离线的基数选择???
 *
 *  @lucene.internal
 * */
public final class BKDRadixSelector {
  // size of the histogram
  // 直方图是特么的什么意思啊
  private static final int HISTOGRAM_SIZE = 256;
  // size of the online buffer: 8 KB
  // 8kb的缓冲区
  private static final int MAX_SIZE_OFFLINE_BUFFER = 1024 * 8;
  // histogram array
  // 直方图的array
  private final long[] histogram;
  // number of bytes to be sorted: config.bytesPerDim + Integer.BYTES
  // 要排序的是多少个字节，一般是每个维度的数字的大小+int
  private final int bytesSorted;
  // flag to when we are moving to sort on heap
  // 最大有多少个point在堆上进行排序
  // 最多是16M的内存，除以每个point的大小，就能拿到具体的数字了
  private final int maxPointsSortInHeap;
  // reusable buffer
  // 离线的缓冲区
  private final byte[] offlineBuffer;
  // holder for partition points
  // 分区
  private final int[] partitionBucket;
  // scratch array to hold temporary data
  // 临时变量的草稿本
  private final byte[] scratch;
  // Directory to create new Offline writer
  // 临时目录
  private final Directory tempDir;
  // prefix for temp files
  // 临时文件的前缀
  private final String tempFileNamePrefix;
  // BKD tree configuration
  private final BKDConfig config;

  /**
   * Sole constructor.
   */
  public BKDRadixSelector(BKDConfig config, int maxPointsSortInHeap, Directory tempDir, String tempFileNamePrefix) {
    this.config = config;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    this.tempDir = tempDir;
    this.tempFileNamePrefix = tempFileNamePrefix;
    // Selection and sorting is done in a given dimension. In case the value of the dimension are equal
    // between two points we tie break first using the data-only dimensions and if those are still equal
    // we tie-break on the docID. Here we account for all bytes used in the process.
    // 比的最大的多少字节，　具体也不懂
    this.bytesSorted = config.bytesPerDim + (config.numDims - config.numIndexDims) * config.bytesPerDim + Integer.BYTES;
    // 缓冲区的大小制定了，除以每个值的大小，就是缓冲区的字节长度
    final int numberOfPointsOffline = MAX_SIZE_OFFLINE_BUFFER / config.bytesPerDoc;
    // 缓冲区初始化
    this.offlineBuffer = new byte[numberOfPointsOffline * config.bytesPerDoc];
    // 排序的多少byte,这里有多少个分区桶???
    this.partitionBucket = new int[bytesSorted];
    // 直方图, 直接就是指定的,256个long
    this.histogram = new long[HISTOGRAM_SIZE];
    // 草稿
    this.scratch = new byte[bytesSorted];
  }

  /**
   *  It uses the provided {@code points} from the given {@code from} to the given {@code to}
   *  to populate the {@code partitionSlices} array holder (length &gt; 1) with two path slices
   *  so the path slice at position 0 contains {@code partition - from} points
   *  where the value of the {@code dim} is lower or equal to the {@code to -from}
   *  points on the slice at position 1.
   *
   *  The {@code dimCommonPrefix} provides a hint for the length of the common prefix length for
   *  the {@code dim} where are partitioning the points.
   *
   *  It return the value of the {@code dim} at the partition point.
   *
   *  If the provided {@code points} is wrapping an {@link OfflinePointWriter}, the
   *  writer is destroyed in the process to save disk space.
   *
   *  // 它使用从给定的给定点到给定的点，用两个路径切片填充partitionSlices数组持有人（长度> 1），因此位置0处的路径切片包含分区-从dim值小于或等于的点开始到位置1上切片的to-from点。dimCommonPrefix提供了一个提示，用于在分区点的dim的公共前缀长度的长度。它返回分区点处的暗淡值。如果提供的点包装了OfflinePointWriter，则写入器在进程中被销毁以节省磁盘空间。
   *
   *  // 感觉就是给定的挑选一个分割点
   * @param partitionSlices 这个参数是个空的，长度为２的数组，就是为了放左右的值而已
   * @param partitionPoint 算出来的切割点，十个类似与index的东西
   */
  public byte[] select(PathSlice points, PathSlice[] partitionSlices, long from, long to, long partitionPoint, int dim, int dimCommonPrefix) throws IOException {
    // some check
    checkArgs(from, to, partitionPoint);

    assert partitionSlices.length > 1 : "[partition alices] must be > 1, got " + partitionSlices.length;

    // If we are on heap then we just select on heap
    // 如果是内存里面玩
    if (points.writer instanceof HeapPointWriter) {
      // 把给定的这个writer，切成两半呗
      byte[] partition = heapRadixSelect((HeapPointWriter) points.writer, dim, Math.toIntExact(from), Math.toIntExact(to),  Math.toIntExact(partitionPoint), dimCommonPrefix);
      partitionSlices[0] = new PathSlice(points.writer, from, partitionPoint - from);
      partitionSlices[1] = new PathSlice(points.writer, partitionPoint, to - partitionPoint);
      return partition;
    }

    // 如果是磁盘的话
    OfflinePointWriter offlinePointWriter = (OfflinePointWriter) points.writer;

    try (PointWriter left = getPointWriter(partitionPoint - from, "left" + dim);
         PointWriter right = getPointWriter(to - partitionPoint, "right" + dim)) {
      partitionSlices[0] = new PathSlice(left, 0, partitionPoint - from);
      partitionSlices[1] = new PathSlice(right, 0, to - partitionPoint);
      return buildHistogramAndPartition(offlinePointWriter, left, right, from, to, partitionPoint, 0, dimCommonPrefix, dim);
    }
  }

  void checkArgs(long from, long to, long partitionPoint) {
    if (partitionPoint < from) {
      throw new IllegalArgumentException("partitionPoint must be >= from");
    }
    if (partitionPoint >= to) {
      throw new IllegalArgumentException("partitionPoint must be < to");
    }
  }

  private int findCommonPrefixAndHistogram(OfflinePointWriter points, long from, long to, int dim, int dimCommonPrefix) throws IOException{
    // find common prefix
    int commonPrefixPosition = bytesSorted;
    final int offset = dim * config.bytesPerDim;
    try (OfflinePointReader reader = points.getReader(from, to - from, offlineBuffer)) {
      assert commonPrefixPosition > dimCommonPrefix;
      reader.next();
      PointValue pointValue = reader.pointValue();
      BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
      // copy dimension
      System.arraycopy(packedValueDocID.bytes, packedValueDocID.offset + offset, scratch, 0, config.bytesPerDim);
      // copy data dimensions and docID
      System.arraycopy(packedValueDocID.bytes, packedValueDocID.offset + config.packedIndexBytesLength, scratch, config.bytesPerDim, (config.numDims - config.numIndexDims) * config.bytesPerDim + Integer.BYTES);

      for (long i = from + 1; i < to; i++) {
        reader.next();
        pointValue = reader.pointValue();
        if (commonPrefixPosition == dimCommonPrefix) {
          histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          // we do not need to check for common prefix anymore,
          // just finish the histogram and break
          for (long j = i + 1; j < to; j++) {
            reader.next();
            pointValue = reader.pointValue();
            histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          }
          break;
        } else {
          // Check common prefix and adjust histogram
          final int startIndex = (dimCommonPrefix > config.bytesPerDim) ? config.bytesPerDim : dimCommonPrefix;
          final int endIndex = (commonPrefixPosition > config.bytesPerDim) ? config.bytesPerDim : commonPrefixPosition;
          packedValueDocID = pointValue.packedValueDocIDBytes();
          int j = FutureArrays.mismatch(scratch, startIndex, endIndex, packedValueDocID.bytes, packedValueDocID.offset + offset + startIndex, packedValueDocID.offset + offset + endIndex);
          if (j == -1) {
            if (commonPrefixPosition > config.bytesPerDim) {
              // Tie-break on data dimensions + docID
              final int startTieBreak = config.packedIndexBytesLength;
              final int endTieBreak = startTieBreak + commonPrefixPosition - config.bytesPerDim;
              int k = FutureArrays.mismatch(scratch, config.bytesPerDim, commonPrefixPosition,
                      packedValueDocID.bytes, packedValueDocID.offset + startTieBreak, packedValueDocID.offset + endTieBreak);
              if (k != -1) {
                commonPrefixPosition = config.bytesPerDim + k;
                Arrays.fill(histogram, 0);
                histogram[scratch[commonPrefixPosition] & 0xff] = i - from;
              }
            }
          } else {
            commonPrefixPosition = dimCommonPrefix + j;
            Arrays.fill(histogram, 0);
            histogram[scratch[commonPrefixPosition] & 0xff] = i - from;
          }
          if (commonPrefixPosition != bytesSorted) {
            histogram[getBucket(offset, commonPrefixPosition, pointValue)]++;
          }
        }
      }
    }

    // Build partition buckets up to commonPrefix
    for (int i = 0; i < commonPrefixPosition; i++) {
      partitionBucket[i] = scratch[i] & 0xff;
    }
    return commonPrefixPosition;
  }

  private int getBucket(int offset, int commonPrefixPosition, PointValue pointValue) {
    int bucket;
    if (commonPrefixPosition < config.bytesPerDim) {
      BytesRef packedValue = pointValue.packedValue();
      bucket = packedValue.bytes[packedValue.offset + offset + commonPrefixPosition] & 0xff;
    } else {
      BytesRef packedValueDocID = pointValue.packedValueDocIDBytes();
      bucket = packedValueDocID.bytes[packedValueDocID.offset + config.packedIndexBytesLength + commonPrefixPosition - config.bytesPerDim] & 0xff;
    }
    return bucket;
  }

  private byte[] buildHistogramAndPartition(OfflinePointWriter points, PointWriter left, PointWriter right,
                                            long from, long to, long partitionPoint, int iteration,  int baseCommonPrefix, int dim) throws IOException {
    // Find common prefix from baseCommonPrefix and build histogram
    int commonPrefix = findCommonPrefixAndHistogram(points, from, to, dim, baseCommonPrefix);

    // If all equals we just partition the points
    if (commonPrefix == bytesSorted) {
      offlinePartition(points, left, right, null, from, to, dim, commonPrefix - 1, partitionPoint);
      return partitionPointFromCommonPrefix();
    }

    long leftCount = 0;
    long rightCount = 0;

    // Count left points and record the partition point
    for(int i = 0; i < HISTOGRAM_SIZE; i++) {
      long size = histogram[i];
      if (leftCount + size > partitionPoint - from) {
        partitionBucket[commonPrefix] = i;
        break;
      }
      leftCount += size;
    }
    // Count right points
    for(int i = partitionBucket[commonPrefix] + 1; i < HISTOGRAM_SIZE; i++) {
      rightCount += histogram[i];
    }

    long delta = histogram[partitionBucket[commonPrefix]];
    assert leftCount + rightCount + delta == to - from : (leftCount + rightCount + delta) + " / " + (to - from);

    // Special case when points are equal except last byte, we can just tie-break
    if (commonPrefix == bytesSorted - 1) {
      long tieBreakCount =(partitionPoint - from - leftCount);
      offlinePartition(points, left,  right, null, from, to, dim, commonPrefix, tieBreakCount);
      return partitionPointFromCommonPrefix();
    }

    // Create the delta points writer
    PointWriter deltaPoints;
    try (PointWriter tempDeltaPoints = getDeltaPointWriter(left, right, delta, iteration)) {
      // Divide the points. This actually destroys the current writer
      offlinePartition(points, left, right, tempDeltaPoints, from, to, dim, commonPrefix, 0);
      deltaPoints = tempDeltaPoints;
    }

    long newPartitionPoint = partitionPoint - from - leftCount;

    if (deltaPoints instanceof HeapPointWriter) {
      return heapPartition((HeapPointWriter) deltaPoints, left, right, dim, 0, (int) deltaPoints.count(), Math.toIntExact(newPartitionPoint), ++commonPrefix);
    } else {
      return buildHistogramAndPartition((OfflinePointWriter) deltaPoints, left, right, 0, deltaPoints.count(), newPartitionPoint, ++iteration, ++commonPrefix, dim);
    }
  }

  private void offlinePartition(OfflinePointWriter points, PointWriter left, PointWriter right, PointWriter deltaPoints,
                                long from, long to, int dim, int bytePosition, long numDocsTiebreak) throws IOException {
    assert bytePosition == bytesSorted -1 || deltaPoints != null;
    int offset =  dim * config.bytesPerDim;
    long tiebreakCounter = 0;
    try (OfflinePointReader reader = points.getReader(from, to - from, offlineBuffer)) {
      while (reader.next()) {
        PointValue pointValue = reader.pointValue();
        int bucket = getBucket(offset, bytePosition, pointValue);
        if (bucket < this.partitionBucket[bytePosition]) {
          // to the left side
          left.append(pointValue);
        } else if (bucket > this.partitionBucket[bytePosition]) {
          // to the right side
          right.append(pointValue);
        } else {
          if (bytePosition == bytesSorted - 1) {
            if (tiebreakCounter < numDocsTiebreak) {
              left.append(pointValue);
              tiebreakCounter++;
            } else {
              right.append(pointValue);
            }
          } else {
            deltaPoints.append(pointValue);
          }
        }
      }
    }
    // Delete original file
    points.destroy();
  }

  private byte[] partitionPointFromCommonPrefix() {
    byte[] partition = new byte[config.bytesPerDim];
    for (int i = 0; i < config.bytesPerDim; i++) {
      partition[i] = (byte)partitionBucket[i];
    }
    return partition;
  }

  private byte[] heapPartition(HeapPointWriter points, PointWriter left, PointWriter right, int dim, int from, int to, int partitionPoint, int commonPrefix) throws IOException {
    byte[] partition = heapRadixSelect(points, dim, from, to, partitionPoint, commonPrefix);
    for (int i = from; i < to; i++) {
      PointValue value = points.getPackedValueSlice(i);
      if (i < partitionPoint) {
        left.append(value);
      } else {
        right.append(value);
      }
    }
    return partition;
  }

  // 内存里的基数选择, 拿到的是切割点的值
  private byte[] heapRadixSelect(HeapPointWriter points, int dim, int from, int to, int partitionPoint, int commonPrefixLength) {
    // 当前维度的偏移量
    final int dimOffset = dim * config.bytesPerDim + commonPrefixLength;
    // 这个维度，除了公共前缀，还要比较几个字节呢
    final int dimCmpBytes = config.bytesPerDim - commonPrefixLength;
    // 比较的这些字节的位置
    final int dataOffset = config.packedIndexBytesLength - dimCmpBytes;
    // maxLength: 要排序的最大的字节长度
    new RadixSelector(bytesSorted - commonPrefixLength) {

      @Override
      // 交换
      protected void swap(int i, int j) {
        points.swap(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        assert k >= 0 : "negative prefix " + k;
        if (k  < dimCmpBytes) {
          // dim bytes
          return points.block[i * config.bytesPerDoc + dimOffset + k] & 0xff;
        } else {
          // data bytes
          return points.block[i * config.bytesPerDoc + dataOffset + k] & 0xff;
        }
      }

      @Override
      protected Selector getFallbackSelector(int d) {
        final int skypedBytes = d + commonPrefixLength;
        final int dimStart = dim * config.bytesPerDim + skypedBytes;
        final int dimEnd =  dim * config.bytesPerDim + config.bytesPerDim;
        // data length is composed by the data dimensions plus the docID
        final int dataLength = (config.numDims - config.numIndexDims) * config.bytesPerDim + Integer.BYTES;
        return new IntroSelector() {

          @Override
          protected void swap(int i, int j) {
            points.swap(i, j);
          }

          @Override
          protected void setPivot(int i) {
            if (skypedBytes < config.bytesPerDim) {
              System.arraycopy(points.block, i * config.bytesPerDoc + dim * config.bytesPerDim, scratch, 0, config.bytesPerDim);
            }
            System.arraycopy(points.block, i * config.bytesPerDoc + config.packedIndexBytesLength, scratch, config.bytesPerDim, dataLength);
          }

          @Override
          protected int compare(int i, int j) {
            if (skypedBytes < config.bytesPerDim) {
              int iOffset = i * config.bytesPerDoc;
              int jOffset = j * config.bytesPerDoc;
              int cmp = FutureArrays.compareUnsigned(points.block, iOffset + dimStart, iOffset + dimEnd, points.block, jOffset + dimStart, jOffset + dimEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            int iOffset = i * config.bytesPerDoc + config.packedIndexBytesLength;
            int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
            return FutureArrays.compareUnsigned(points.block, iOffset, iOffset + dataLength, points.block, jOffset, jOffset + dataLength);
          }

          @Override
          protected int comparePivot(int j) {
            if (skypedBytes < config.bytesPerDim) {
              int jOffset = j * config.bytesPerDoc;
              int cmp = FutureArrays.compareUnsigned(scratch, skypedBytes, config.bytesPerDim, points.block, jOffset + dimStart, jOffset + dimEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
            return FutureArrays.compareUnsigned(scratch, config.bytesPerDim, config.bytesPerDim + dataLength, points.block, jOffset, jOffset + dataLength);
          }
        };
      }
    }.select(from, to, partitionPoint);

    byte[] partition = new byte[config.bytesPerDim];
    PointValue pointValue = points.getPackedValueSlice(partitionPoint);
    BytesRef packedValue = pointValue.packedValue();
    System.arraycopy(packedValue.bytes, packedValue.offset + dim * config.bytesPerDim, partition, 0, config.bytesPerDim);
    return partition;
  }

  /** Sort the heap writer by the specified dim. It is used to sort the leaves of the tree */
  // points进行排序
  public void heapRadixSort(final HeapPointWriter points, int from, int to, int dim, int commonPrefixLength) {
    final int dimOffset = dim * config.bytesPerDim + commonPrefixLength;
    final int dimCmpBytes = config.bytesPerDim - commonPrefixLength;
    final int dataOffset = config.packedIndexBytesLength - dimCmpBytes;
    new MSBRadixSorter(bytesSorted - commonPrefixLength) {

      @Override
      protected int byteAt(int i, int k) {
        assert k >= 0 : "negative prefix " + k;
        if (k  < dimCmpBytes) {
          // dim bytes
          return points.block[i * config.bytesPerDoc + dimOffset + k] & 0xff;
        } else {
          // data bytes
          return points.block[i * config.bytesPerDoc + dataOffset + k] & 0xff;
        }
      }

      @Override
      protected void swap(int i, int j) {
        points.swap(i, j);
      }

      @Override
      protected Sorter getFallbackSorter(int k) {
        final int skypedBytes = k + commonPrefixLength;
        final int dimStart = dim * config.bytesPerDim + skypedBytes;
        final int dimEnd =  dim * config.bytesPerDim + config.bytesPerDim;
        // data length is composed by the data dimensions plus the docID
        final int dataLength = (config.numDims - config.numIndexDims) * config.bytesPerDim + Integer.BYTES;
        return new IntroSorter() {

          @Override
          protected void swap(int i, int j) {
            points.swap(i, j);
          }

          @Override
          protected void setPivot(int i) {
            if (skypedBytes < config.bytesPerDim) {
              System.arraycopy(points.block, i * config.bytesPerDoc + dim * config.bytesPerDim, scratch, 0, config.bytesPerDim);
            }
            System.arraycopy(points.block, i * config.bytesPerDoc + config.packedIndexBytesLength, scratch, config.bytesPerDim, dataLength);
          }

          @Override
          protected int compare(int i, int j) {
            if (skypedBytes < config.bytesPerDim) {
              int iOffset = i * config.bytesPerDoc;
              int jOffset = j * config.bytesPerDoc;
              int cmp = FutureArrays.compareUnsigned(points.block, iOffset + dimStart, iOffset + dimEnd, points.block, jOffset + dimStart, jOffset + dimEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            int iOffset = i * config.bytesPerDoc + config.packedIndexBytesLength;
            int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
            return FutureArrays.compareUnsigned(points.block, iOffset, iOffset + dataLength, points.block, jOffset, jOffset + dataLength);
          }

          @Override
          protected int comparePivot(int j) {
            if (skypedBytes < config.bytesPerDim) {
              int jOffset = j * config.bytesPerDoc;
              int cmp = FutureArrays.compareUnsigned(scratch, skypedBytes, config.bytesPerDim, points.block, jOffset + dimStart, jOffset + dimEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            int jOffset = j * config.bytesPerDoc + config.packedIndexBytesLength;
            return FutureArrays.compareUnsigned(scratch, config.bytesPerDim, config.bytesPerDim + dataLength, points.block, jOffset, jOffset + dataLength);
          }
        };
      }
    }.sort(from, to);
  }

  private PointWriter getDeltaPointWriter(PointWriter left, PointWriter right, long delta, int iteration) throws IOException {
    if (delta <= getMaxPointsSortInHeap(left, right)) {
      return  new HeapPointWriter(config, Math.toIntExact(delta));
    } else {
      return new OfflinePointWriter(config, tempDir, tempFileNamePrefix, "delta" + iteration, delta);
    }
  }

  private int getMaxPointsSortInHeap(PointWriter left, PointWriter right) {
    int pointsUsed = 0;
    if (left instanceof HeapPointWriter) {
      pointsUsed += ((HeapPointWriter) left).size;
    }
    if (right instanceof HeapPointWriter) {
      pointsUsed += ((HeapPointWriter) right).size;
    }
    assert maxPointsSortInHeap >= pointsUsed;
    return maxPointsSortInHeap - pointsUsed;
  }

  PointWriter getPointWriter(long count, String desc) throws IOException {
    // As we recurse, we hold two on-heap point writers at any point. Therefore the
    // max size for these objects is half of the total points we can have on-heap.
    // 小的话就返回一个堆上的，　大了的话就是磁盘的.
    if (count <= maxPointsSortInHeap / 2) {
      int size = Math.toIntExact(count);
      return new HeapPointWriter(config, size);
    } else {
      return new OfflinePointWriter(config, tempDir, tempFileNamePrefix, desc, count);
    }
  }

  /** Sliced reference to points in an PointWriter. */
  // 数据点的写入者的切片，　从start切count个
  public static final class PathSlice {
    // 一个数据点的写入者
    public final PointWriter writer;
    public final long start;
    public final long count;

    public PathSlice(PointWriter writer, long start, long count) {
      this.writer = writer;
      this.start = start;
      this.count = count;
    }

    @Override
    public String toString() {
      return "PathSlice(start=" + start + " count=" + count + " writer=" + writer + ")";
    }
  }
}
