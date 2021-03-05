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
package org.apache.lucene.codecs;


import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 *
 * <pre>
 *
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1)
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 *
 * d - document
 * x - skip data
 * c - skip data with child pointer
 *
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 *
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 *
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 * <br/>
 * <p>
 * 多层跳表的实现
 * <p>
 * 第i层包含有i-1层中每个第跳跃间隔的元素。　比如第３层就有第二层的第3,6,9,12th的元素.
 * 所以第i层的元素编号是:  floor(df / (skipInterval ^ (i+1));
 * <p>
 * 除了第0层的跳跃节点，　其他每一层的都有一个指针，　指向下一层的对应节点。 这保证了对数时间复杂度内的查询。
 * <p>
 * 因为这个类关心了不同跳跃层数的写入，子类必须定义实际的跳跃数据格式.
 *
 * @lucene.experimental
 */

public abstract class MultiLevelSkipListWriter {
  /**
   * number of levels in this skip list
   */
  // 跳表层数
  protected final int numberOfSkipLevels;

  /**
   * the skip interval in the list with level = 0
   */
  //　0层的跳表间隔，　每隔几个跳跃一次
  private final int skipInterval;

  /**
   * skipInterval used for level &gt; 0
   */
  // 高层的跳跃间隔
  private final int skipMultiplier;

  /**
   * for every skip level a different buffer is used
   */
  // buffer
  private RAMOutputStream[] skipBuffer;

  /**
   * Creates a {@code MultiLevelSkipListWriter}.
   */
  protected MultiLevelSkipListWriter(int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
    this.skipInterval = skipInterval;
    this.skipMultiplier = skipMultiplier;

    int numberOfSkipLevels;
    // calculate the maximum number of skip levels for this document frequency
    // 如果总数小于间隔，那么只需要一层跳跃表
    if (df <= skipInterval) {
      numberOfSkipLevels = 1;
    } else {
      // 否则的话，跳跃表层数是算出来的
      // 算法是, 第0层的个数，　对高层的间隔取对数 + 1进行保底
      numberOfSkipLevels = 1 + MathUtil.log(df / skipInterval, skipMultiplier);
    }

    // make sure it does not exceed maxSkipLevels
    if (numberOfSkipLevels > maxSkipLevels) {
      numberOfSkipLevels = maxSkipLevels;
    }
    this.numberOfSkipLevels = numberOfSkipLevels;
  }

  /**
   * Creates a {@code MultiLevelSkipListWriter}, where
   * {@code skipInterval} and {@code skipMultiplier} are
   * the same.
   */
  protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
    this(skipInterval, skipInterval, maxSkipLevels, df);
  }

  /**
   * Allocates internal skip buffers.
   * 申请一下内部的缓冲区
   */
  protected void init() {
    skipBuffer = new RAMOutputStream[numberOfSkipLevels];
    for (int i = 0; i < numberOfSkipLevels; i++) {
      skipBuffer[i] = new RAMOutputStream();
    }
  }

  /**
   * Creates new buffers or empties the existing ones
   * // 全部缓冲区清零
   */
  protected void resetSkip() {
    if (skipBuffer == null) {
      init();
    } else {
      for (int i = 0; i < skipBuffer.length; i++) {
        skipBuffer[i].reset();
      }
    }
  }

  /**
   * Subclasses must implement the actual skip data encoding in this method.
   *
   * @param level      the level skip data shall be writing for
   * @param skipBuffer the skip buffer to write to
   */
  protected abstract void writeSkipData(int level, IndexOutput skipBuffer) throws IOException;

  /**
   * Writes the current skip data to the buffers. The current document frequency determines
   * the max level is skip data is to be written to.
   *
   * // 把当前的跳跃数据写到缓冲区，　当前词频决定了跳跃数据的最大层数.
   *
   * @param df the current document frequency
   * @throws IOException If an I/O error occurs
   */
  public void bufferSkip(int df) throws IOException {

    assert df % skipInterval == 0;
    int numLevels = 1;
    df /= skipInterval;

    // determine max level
    while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) {
      numLevels++;
      df /= skipMultiplier;
    }

    long childPointer = 0;

    for (int level = 0; level < numLevels; level++) {
      writeSkipData(level, skipBuffer[level]);

      // 链表上的子节点
      long newChildPointer = skipBuffer[level].getFilePointer();

      if (level != 0) {
        // store child pointers for all levels except the lowest
        // 除了最底层的，　其他都要记录一下子节点的　文件中的位置
        skipBuffer[level].writeVLong(childPointer);
      }

      //remember the childPointer for the next level
      // 链表开始走
      childPointer = newChildPointer;
    }
  }

  /**
   * Writes the buffered skip lists to the given output.
   *
   * 缓冲的跳表写到给定的输出流去
   *
   * @param output the IndexOutput the skip lists shall be written to
   * @return the pointer the skip list starts
   */
  public long writeSkip(IndexOutput output) throws IOException {
    long skipPointer = output.getFilePointer();
    // 开始了
    //System.out.println("skipper.writeSkip fp=" + skipPointer);
    if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;

    // 从高层向下写
    for (int level = numberOfSkipLevels - 1; level > 0; level--) {
      long length = skipBuffer[level].getFilePointer();
      // 每一层写一下长度，然后把这层的东西全写进去，　这个跳表就可以再次被读取出来了
      if (length > 0) {
        output.writeVLong(length);
        skipBuffer[level].writeTo(output);
      }
    }
    // 最后写最底层的数据
    skipBuffer[0].writeTo(output);

    return skipPointer;
  }
}
