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
package org.apache.lucene.search;


import java.io.IOException;

/**
 * This abstract class defines methods to iterate over a set of non-decreasing
 * doc ids. Note that this class assumes it iterates on doc Ids, and therefore
 * {@link #NO_MORE_DOCS} is set to {@value #NO_MORE_DOCS} in order to be used as
 * a sentinel object. Implementations of this class are expected to consider
 * {@link Integer#MAX_VALUE} as an invalid value.
 *
 * 这个抽象类，　定义了遍历一个(不会变小的docId 集合）的方法.
 *
 * 注意这个类假设他遍历的是docId。　因此用2147483647来作为没有更多doc的标识.
 *
 * 这个类的实现类被期望使用int的最大值作为非法值.
 *
 */
public abstract class DocIdSetIterator {
  
  /** An empty {@code DocIdSetIterator} instance */
  public static final DocIdSetIterator empty() {
    return new DocIdSetIterator() {
      boolean exhausted = false;
      
      @Override
      public int advance(int target) {
        assert !exhausted;
        assert target >= 0;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public int docID() {
        return exhausted ? NO_MORE_DOCS : -1;
      }
      @Override
      public int nextDoc() {
        assert !exhausted;
        exhausted = true;
        return NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 0;
      }
    };
  }

  /** A {@link DocIdSetIterator} that matches all documents up to
   *  {@code maxDoc - 1}. */
  public static final DocIdSetIterator all(int maxDoc) {
    return new DocIdSetIterator() {
      int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        doc = target;
        if (doc >= maxDoc) {
          doc = NO_MORE_DOCS;
        }
        return doc;
      }

      @Override
      public long cost() {
        return maxDoc;
      }
    };
  }

  /** A {@link DocIdSetIterator} that matches a range documents from
   *  minDocID (inclusive) to maxDocID (exclusive). */
  public static final DocIdSetIterator range(int minDoc, int maxDoc) {
    if (minDoc >= maxDoc) {
        throw new IllegalArgumentException("minDoc must be < maxDoc but got minDoc=" + minDoc + " maxDoc=" + maxDoc);
    }
    if (minDoc < 0) {
      throw new IllegalArgumentException("minDoc must be >= 0 but got minDoc=" + minDoc);
    }
    return new DocIdSetIterator() {
      private int doc = -1;

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        if (target < minDoc) {
            doc = minDoc;
        } else if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
        } else {
            doc = target;
        }
        return doc;
      }

      @Override
      public long cost() {
        return maxDoc - minDoc;
      }
    };
  }

  /**
   * When returned by {@link #nextDoc()}, {@link #advance(int)} and
   * {@link #docID()} it means there are no more docs in the iterator.
   * // 不管被谁返回，　都代表返回完了，没有多的doc了
   */
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

  /**
   * Returns the following:
   * <ul>
   * <li><code>-1</code> if {@link #nextDoc()} or
   * {@link #advance(int)} were not called yet.
   * <li>{@link #NO_MORE_DOCS} if the iterator has exhausted.
   * <li>Otherwise it should return the doc ID it is currently on.
   * </ul>
   * <p>
   *
   * <br/>
   * <ul>
   *   <li>
   *     如果nextDoc/advance没有被调用过，那么是-1.
   *   </li>
   *   <li>
   *     如果没了，　就是最大值
   *   </li>
   *   <li>
   *     其他情况下，返回当前的docId.
   *   </li>
   * </ul>
   * 
   * @since 2.9
   */
  public abstract int docID();

  /**
   * Advances to the next document in the set and returns the doc it is
   * currently on, or {@link #NO_MORE_DOCS} if there are no more docs in the
   * set.<br>
   * 
   * <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   * 
   * @since 2.9
   *
   * 前进到当前集合的下一个id. 返回当前的哪个doc. 如果没了就返回最大值.
   *
   * 注意: 如果没了，不能再调用这个方法，　会有不可预知的错误.
   */
  public abstract int nextDoc() throws IOException;

 /**
   * Advances to the first beyond the current whose document number is greater 
   * than or equal to <i>target</i>, and returns the document number itself. 
   * Exhausts the iterator and returns {@link #NO_MORE_DOCS} if <i>target</i> 
   * is greater than the highest document number in the set.
   * <p>
   * The behavior of this method is <b>undefined</b> when called with
   * <code> target &le; current</code>, or after the iterator has exhausted.
   * Both cases may result in unpredicted behavior.
   * <p>
   * When <code> target &gt; current</code> it behaves as if written:
   * 
   * <pre class="prettyprint">
   * int advance(int target) {
   *   int doc;
   *   while ((doc = nextDoc()) &lt; target) {
   *   }
   *   return doc;
   * }
   * </pre>
   * 
   * Some implementations are considerably more efficient than that.
   * <p>
   * <b>NOTE:</b> this method may be called with {@link #NO_MORE_DOCS} for
   * efficiency by some Scorers. If your implementation cannot efficiently
   * determine that it should exhaust, it is recommended that you check for that
   * value in each call to this method.
   * <p>
   *
   * @since 2.9
  *
  * // 前进到大于等于给定id的那个文档. 返回他的文档编号. 如果给定值太大了，就返回没有.
  *
  * // 如果给定的目标值小于当前值，或者迭代器没了，　这个方法没有定义怎么做，　可能导致奇怪的问题.
  *
  * <br/>
  * 如果给定的目标值大于当前值，那么要按照下面写的做.
  *
  * 就是一直找到目标值然后返回. 很多实现比这个高效很多.
  *
  * <br/>
  * 这个方法可能会被某些打分其用最大值来调用，用以提高效率. 如果你的实现不能搞笑的判断是否遍历完成，　建议你在每次调用这个方法都check一下.
  *
   */
  public abstract int advance(int target) throws IOException;

  /** Slow (linear) implementation of {@link #advance} relying on
   *  {@link #nextDoc()} to advance beyond the target position. */
  // 一个慢的前进，　线性的实现，依靠与nextDoc来一步一步走到目标值.
  protected final int slowAdvance(int target) throws IOException {
    assert docID() < target;
    int doc;
    do {
      doc = nextDoc();
    } while (doc < target);
    return doc;
  }

  /**
   * Returns the estimated cost of this {@link DocIdSetIterator}.
   * <p>
   * This is generally an upper bound of the number of documents this iterator
   * might match, but may be a rough heuristic, hardcoded value, or otherwise
   * completely inaccurate.
   *
   * 返回当前迭代器的大概的cost数值
   * 这通常是这个迭代器可能匹配的文档数量的上线，　但是可能是一个粗略的值，　硬编码的值，　或是完全不准确的一个值.
   * <br/>
   *
   *
   */
  public abstract long cost();
  
}
