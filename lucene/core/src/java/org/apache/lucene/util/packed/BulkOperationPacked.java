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



/**
 * Non-specialized {@link BulkOperation} for {@link PackedInts.Format#PACKED}.
 */
class BulkOperationPacked extends BulkOperation {

  // 存储的每个值需要的bit数
  private final int bitsPerValue;
  // 用bitsPerValue来存储数据, 同时数据真正存储在long里面.
  // 需要最少多少个long, 才能正好达到边界.
  // 比如使用1个bit存储, 那么1个long=64位可以正好保存64个数
  // 比如使用4bit存储, 那么使用1个long=64位正好可以保存16个数, bit位不多不少
  // 但是使用12bit存数据, 那么使用1个long, 64无法整除12, 会有多的,少的情况. 此时需要3个long, 3 * 64 能正好
  // 存储 12 * 16, 也就是16个12bit的数字.
  // 所以算法很明显, bitPerValue和64的最小公倍数除以64.
  // 理解为: 一个完整的块, 需要多少个long.
  private final int longBlockCount;
  // 理解为: 一个完整的块, 需要多少个perBit存储的数字.
  private final int longValueCount;

  // 每个数用bitPerValue, 如果用byte存, 每个完整的块, 需要几个byte.
  private final int byteBlockCount;
  // 每个完整的块(用byte存储), 能存几个bitsPerValue.
  private final int byteValueCount;

  // 两个mask
  private final long mask;
  private final int intMask;

  /**
   * 块的批量操作???
   * @param bitsPerValue
   */
  public BulkOperationPacked(int bitsPerValue) {
    this.bitsPerValue = bitsPerValue;
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    // 求最小公倍数就是这么求的
    int blocks = bitsPerValue;
    while ((blocks & 1) == 0) {
      blocks >>>= 1;
    }
    this.longBlockCount = blocks;
    // 最小公倍数相对于bitPerValue的商.
    this.longValueCount = 64 * longBlockCount / bitsPerValue;

    // 为了存储一个完整的快, 需要多少个byte
    int byteBlockCount = 8 * longBlockCount;
    // 完整的块, 需要多少个perBit的数, 之后变了
    int byteValueCount = longValueCount;

    // 求最大公约数之类的
    while ((byteBlockCount & 1) == 0 && (byteValueCount & 1) == 0) {
      byteBlockCount >>>= 1;
      byteValueCount >>>= 1;
    }
    this.byteBlockCount = byteBlockCount;
    this.byteValueCount = byteValueCount;
    if (bitsPerValue == 64) {
      this.mask = ~0L;
    } else {
      this.mask = (1L << bitsPerValue) - 1;
    }
    this.intMask = (int) mask;
    assert longValueCount * bitsPerValue == 64 * longBlockCount;
  }

  @Override
  public int longBlockCount() {
    return longBlockCount;
  }

  @Override
  public int longValueCount() {
    return longValueCount;
  }

  @Override
  public int byteBlockCount() {
    return byteBlockCount;
  }

  @Override
  public int byteValueCount() {
    return byteValueCount;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] =
            ((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (blocks[blocksOffset] >>> bitsLeft) & mask;
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values,
      int valuesOffset, int iterations) {
    long nextValue = 0L;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final long bytes = blocks[blocksOffset++] & 0xFFL;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & mask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1L << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    if (bitsPerValue > 32) {
      throw new UnsupportedOperationException("Cannot decode " + bitsPerValue + "-bits values into an int[]");
    }
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft < 0) {
        values[valuesOffset++] = (int)
            (((blocks[blocksOffset++] & ((1L << (bitsPerValue + bitsLeft)) - 1)) << -bitsLeft)
            | (blocks[blocksOffset] >>> (64 + bitsLeft)));
        bitsLeft += 64;
      } else {
        values[valuesOffset++] = (int) ((blocks[blocksOffset] >>> bitsLeft) & mask);
      }
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values,
      int valuesOffset, int iterations) {
    int nextValue = 0;
    int bitsLeft = bitsPerValue;
    for (int i = 0; i < iterations * byteBlockCount; ++i) {
      final int bytes = blocks[blocksOffset++] & 0xFF;
      if (bitsLeft > 8) {
        // just buffer
        bitsLeft -= 8;
        nextValue |= bytes << bitsLeft;
      } else {
        // flush
        int bits = 8 - bitsLeft;
        values[valuesOffset++] = nextValue | (bytes >>> bits);
        while (bits >= bitsPerValue) {
          bits -= bitsPerValue;
          values[valuesOffset++] = (bytes >>> bits) & intMask;
        }
        // then buffer
        bitsLeft = bitsPerValue - bits;
        nextValue = (bytes & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == bitsPerValue;
  }

  @Override
  public void encode(long[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        nextBlock |= values[valuesOffset++] << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= values[valuesOffset++];
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        nextBlock |= values[valuesOffset] >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks,
      int blocksOffset, int iterations) {
    long nextBlock = 0;
    int bitsLeft = 64;
    for (int i = 0; i < longValueCount * iterations; ++i) {
      bitsLeft -= bitsPerValue;
      if (bitsLeft > 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL) << bitsLeft;
      } else if (bitsLeft == 0) {
        nextBlock |= (values[valuesOffset++] & 0xFFFFFFFFL);
        blocks[blocksOffset++] = nextBlock;
        nextBlock = 0;
        bitsLeft = 64;
      } else { // bitsLeft < 0
        nextBlock |= (values[valuesOffset] & 0xFFFFFFFFL) >>> -bitsLeft;
        blocks[blocksOffset++] = nextBlock;
        nextBlock = (values[valuesOffset++] & ((1L << -bitsLeft) - 1)) << (64 + bitsLeft);
        bitsLeft += 64;
      }
    }
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final long v = values[valuesOffset++];
      assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (int) ((v & ((1L << bits) - 1)) << bitsLeft);
      }
    }
    assert bitsLeft == 8;
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks,
      int blocksOffset, int iterations) {
    int nextBlock = 0;
    int bitsLeft = 8;
    for (int i = 0; i < byteValueCount * iterations; ++i) {
      final int v = values[valuesOffset++];
      assert PackedInts.bitsRequired(v & 0xFFFFFFFFL) <= bitsPerValue;
      if (bitsPerValue < bitsLeft) {
        // just buffer
        nextBlock |= v << (bitsLeft - bitsPerValue);
        bitsLeft -= bitsPerValue;
      } else {
        // flush as many blocks as possible
        int bits = bitsPerValue - bitsLeft;
        blocks[blocksOffset++] = (byte) (nextBlock | (v >>> bits));
        while (bits >= 8) {
          bits -= 8;
          blocks[blocksOffset++] = (byte) (v >>> bits);
        }
        // then buffer
        bitsLeft = 8 - bits;
        nextBlock = (v & ((1 << bits) - 1)) << bitsLeft;
      }
    }
    assert bitsLeft == 8;
  }

}
