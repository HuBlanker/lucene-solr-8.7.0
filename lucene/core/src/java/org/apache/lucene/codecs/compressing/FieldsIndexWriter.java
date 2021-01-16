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
package org.apache.lucene.codecs.compressing;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Efficient index format for block-based {@link Codec}s.
 * <p>For each block of compressed stored fields, this stores the first document
 * of the block and the start pointer of the block in a
 * {@link DirectMonotonicWriter}. At read time, the docID is binary-searched in
 * the {@link DirectMonotonicReader} that records doc IDS, and the returned
 * index is used to look up the start pointer in the
 * {@link DirectMonotonicReader} that records start pointers.
 *
 * @lucene.internal
 */
public final class FieldsIndexWriter implements Closeable {

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = 0;

  private final Directory dir;
  private final String name;
  private final String suffix;
  private final String extension;
  private final String codecName;
  private final byte[] id;
  private final int blockShift;
  private final IOContext ioContext;
  private IndexOutput docsOut;
  private IndexOutput filePointersOut;
  private int totalDocs;
  private int totalChunks;
  private long previousFP;

  FieldsIndexWriter(Directory dir, String name, String suffix, String extension,
                    String codecName, byte[] id, int blockShift, IOContext ioContext) throws IOException {
    this.dir = dir;
    this.name = name;
    this.suffix = suffix;
    this.extension = extension;
    this.codecName = codecName;
    this.id = id;
    this.blockShift = blockShift;
    this.ioContext = ioContext;
    this.docsOut = dir.createTempOutput(name, codecName + "-doc_ids", ioContext);
    boolean success = false;
    try {
      CodecUtil.writeHeader(docsOut, codecName + "Docs", VERSION_CURRENT);
      filePointersOut = dir.createTempOutput(name, codecName + "file_pointers", ioContext);
      CodecUtil.writeHeader(filePointersOut, codecName + "FilePointers", VERSION_CURRENT);
      success = true;
    } finally {
      if (success == false) {
        close();
      }
    }
  }

  void writeIndex(int numDocs, long startPointer) throws IOException {
    assert startPointer >= previousFP;
    docsOut.writeVInt(numDocs);
    filePointersOut.writeVLong(startPointer - previousFP);
    previousFP = startPointer;
    totalDocs += numDocs;
    totalChunks++;
  }

  // flush 时候调用，这次在看metaOut都写了啥

  /**
   * 在这里生成的fdx文件，从两个tmp文件里面找到每个chunk的doc数量，fdt文件中存储的字节数，
   * 这两个内容，写到meta文件和fdx文件中，配合起来存储的
   * <p>
   * 这个类本身就是为了fdx文件搞的，就是为了写fdt的索引，写得少很正常
   */
  void finish(int numDocs, long maxPointer, IndexOutput metaOut) throws IOException {
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    CodecUtil.writeFooter(docsOut);
    CodecUtil.writeFooter(filePointersOut);
    IOUtils.close(docsOut, filePointersOut);

    // dataOut　是fdx文件，是用来对fdt文件做索引的文件，所以fdt文件写入内容，我这里记录每个chunk的doc数量，占用字节数即可
    // 所以这里只能调用一次么，无论是多少个多大的field，都只能调用一次这里么
    try (IndexOutput dataOut = dir.createOutput(IndexFileNames.segmentFileName(name, suffix, extension), ioContext)) {
      // 这个header，48个字节.
      CodecUtil.writeIndexHeader(dataOut, codecName + "Idx", VERSION_CURRENT, id, suffix);

      metaOut.writeInt(numDocs);
      metaOut.writeInt(blockShift);
      metaOut.writeInt(totalChunks + 1);
      // 这个filePointer,此时只写了一个header的长度，48
      long filePointer = dataOut.getFilePointer();
      metaOut.writeLong(filePointer);

      try (ChecksumIndexInput docsIn = dir.openChecksumInput(docsOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(docsIn, codecName + "Docs", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          // 这里做的配合是，　meta里面存了min/斜率等，真实的数组偏移量在dataOut里面存储
          final DirectMonotonicWriter docs = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long doc = 0;
          docs.add(doc);
          // 注意，这里是每一chunk, 而不是per document
          for (int i = 0; i < totalChunks; ++i) {
            // 每个chunk的doc数量
            doc += docsIn.readVInt();
            docs.add(doc);
          }
          docs.finish();
          if (doc != totalDocs) {
            throw new CorruptIndexException("Docs don't add up", docsIn);
          }
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(docsIn, priorE);
        }
      }
      dir.deleteFile(docsOut.getName());
      docsOut = null;

      long filePointer1 = dataOut.getFilePointer();
      metaOut.writeLong(filePointer1);
      try (ChecksumIndexInput filePointersIn = dir.openChecksumInput(filePointersOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(filePointersIn, codecName + "FilePointers", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          // 其实由于我测试的时候只有一两个doc，肯定在一个chunk,所以dataOut里面都没写入啥东西
          final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long fp = 0;
          // 这里存储的是每一个chunk的实际数据的字节长度
          for (int i = 0; i < totalChunks; ++i) {
            fp += filePointersIn.readVLong();
            filePointers.add(fp);
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up", filePointersIn);
          }
          filePointers.add(maxPointer);
          filePointers.finish();
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      dir.deleteFile(filePointersOut.getName());
      filePointersOut = null;

      // meta里面再搞个索引
      long filePointer2 = dataOut.getFilePointer();
      metaOut.writeLong(filePointer2);
      metaOut.writeLong(maxPointer);

      CodecUtil.writeFooter(dataOut);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(docsOut, filePointersOut);
    } finally {
      List<String> fileNames = new ArrayList<>();
      if (docsOut != null) {
        fileNames.add(docsOut.getName());
      }
      if (filePointersOut != null) {
        fileNames.add(filePointersOut.getName());
      }
      try {
        IOUtils.deleteFiles(dir, fileNames);
      } finally {
        docsOut = filePointersOut = null;
      }
    }
  }
}
