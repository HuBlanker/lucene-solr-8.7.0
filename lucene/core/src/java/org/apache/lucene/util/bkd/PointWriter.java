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

/** Appends many points, and then at the end provides a {@link PointReader} to iterate
 *  those points.  This abstracts away whether we write to disk, or use simple arrays
 *  in heap.
 *  // 写入很多点，　然后提供一个Reader来遍历这些点.
 *
 *  这个类抽象了接口。　不管我们使用的是写入磁盘，还是写入堆上的简单数组，都可以使用这个抽象接口.
 *
 *  @lucene.internal
 *  */
public interface PointWriter extends Closeable {

  /** Add a new point from the packed value and docId */
  // 追加一个点，有点的值和对应的docId
  void append(byte[] packedValue, int docID) throws IOException;

  /** Add a new point from a {@link PointValue} */
  // 追加一个点，
  void append(PointValue pointValue) throws IOException;

  /** Returns a {@link PointReader} iterator to step through all previously added points */
  // 返回一个之前所有的点的遍历器
  PointReader getReader(long startPoint, long length) throws IOException;

  /** Return the number of points in this writer */
  // 总数
  long count();

  /** Removes any temp files behind this writer */
  // 销毁
  void destroy() throws IOException;

}

