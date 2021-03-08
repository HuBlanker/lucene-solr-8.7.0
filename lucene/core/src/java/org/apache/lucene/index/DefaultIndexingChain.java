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
package org.apache.lucene.index;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.RamUsageEstimator;

/** Default general purpose indexing chain, which handles
 *  indexing all types of fields. */
final class DefaultIndexingChain extends DocConsumer {


  final Counter bytesUsed = Counter.newCounter();

  // FieldInfos　的构造器，应该就是在这个类里面添加信息的
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash;
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;
  final TermVectorsConsumer termVectorsWriter;


  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:
  // 用来保存所有field的手动hash表及它的hashmask
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  // 全局的Field总数，比如每个doc都是相同的name/desc. 那么这里就是2.
  private int totalFieldCount;
  // 其实约等于当前doc编号，因为每次处理一个文档，就+1,然后对于当前文档的所有field,都是这个值了
  private long nextFieldGen;

  // Holds fields seen in each document
  private PerField[] fields = new PerField[1];
  private final InfoStream infoStream;
  private final ByteBlockPool.Allocator byteBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final int indexCreatedVersionMajor;
  private final Consumer<Throwable> abortingExceptionConsumer;
  private boolean hasHitAbortingException;
  private DocValuesType dvType;

  DefaultIndexingChain(int indexCreatedVersionMajor, SegmentInfo segmentInfo, Directory directory, FieldInfos.Builder fieldInfos, LiveIndexWriterConfig indexWriterConfig,
                       Consumer<Throwable> abortingExceptionConsumer) {
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(bytesUsed);
    IntBlockPool.Allocator intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.indexWriterConfig = indexWriterConfig;
    assert segmentInfo.getIndexSort() == indexWriterConfig.getIndexSort();
    this.fieldInfos = fieldInfos;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.abortingExceptionConsumer = abortingExceptionConsumer;

    if (segmentInfo.getIndexSort() == null) {
      storedFieldsConsumer = new StoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter = new TermVectorsConsumer(intBlockAllocator, byteBlockAllocator, directory, segmentInfo, indexWriterConfig.getCodec());
    } else {
      storedFieldsConsumer = new SortingStoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter = new SortingTermVectorsConsumer(intBlockAllocator, byteBlockAllocator, directory, segmentInfo, indexWriterConfig.getCodec());
    }
    termsHash = new FreqProxTermsWriter(intBlockAllocator, byteBlockAllocator, bytesUsed, termVectorsWriter);
  }

  private void onAbortingException(Throwable th) {
    assert th != null;
    this.hasHitAbortingException = true;
    abortingExceptionConsumer.accept(th);
  }

  private LeafReader getDocValuesLeafReader() {
    return new DocValuesLeafReader() {
      @Override
      public NumericDocValues getNumericDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
          return (NumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
          return (BinaryDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          return (SortedDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
          return (SortedNumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          return (SortedSetDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return fieldInfos.finish();
      }

    };
  }

  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    LeafReader docValuesReader = getDocValuesLeafReader();

    List<IndexSorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new UnsupportedOperationException("Cannot sort index using sort field " + sortField);
      }
      comparators.add(sorter.getDocComparator(docValuesReader, state.segmentInfo.maxDoc()));
    }
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sort(state.segmentInfo.maxDoc(), comparators.toArray(new IndexSorter.DocComparator[0]));
  }

  @Override
  public Sorter.DocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state);
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write norms");
    }
    SegmentReadState readState = new SegmentReadState(state.directory, state.segmentInfo, state.fieldInfos, IOContext.READ, state.segmentSuffix);
    
    t0 = System.nanoTime();
    writeDocValues(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write points");
    }
    
    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);
    storedFieldsConsumer.flush(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String,TermsHashPerField> fieldsToFlush = new HashMap<>();
    for (int i=0;i<fieldHash.length;i++) {
      PerField perField = fieldHash[i];
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    try (NormsProducer norms = readState.fieldInfos.hasNorms()
        ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
        : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);
    }
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();
    indexWriterConfig.getCodec().fieldInfosFormat().write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.pointValuesWriter != null) {
            if (perField.fieldInfo.getPointDimensionCount() == 0) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no points but wrote them");
            }
            if (pointsWriter == null) {
              // lazy init
              PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();
              if (fmt == null) {
                throw new IllegalStateException("field=\"" + perField.fieldInfo.name + "\" was indexed as points but codec does not support points");
              }
              pointsWriter = fmt.fieldsWriter(state);
            }

            perField.pointValuesWriter.flush(state, sortMap, pointsWriter);
            perField.pointValuesWriter = null;
          } else if (perField.fieldInfo.getPointDimensionCount() != 0) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has points but did not write them");
          }
          perField = perField.next;
        }
      }
      if (pointsWriter != null) {
        pointsWriter.finish();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(pointsWriter);
      } else {
        IOUtils.closeWhileHandlingException(pointsWriter);
      }
    }
  }

  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);
            perField.docValuesWriter = null;
          } else if (perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has docValues but did not write them");
          }
          perField = perField.next;
        }
      }

      // TODO: catch missing DV fields here?  else we have
      // null/"" depending on how docs landed in segments?
      // but we can't detect all cases, and we should leave
      // this behavior undefined. dv is not "schemaless": it's column-stride.
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dvConsumer);
      } else {
        IOUtils.closeWhileHandlingException(dvConsumer);
      }
    }

    if (state.fieldInfos.hasDocValues() == false) {
      if (dvConsumer != null) {
        // BUG
        throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
    }
  }

  private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    boolean success = false;
    NormsConsumer normsConsumer = null;
    try {
      if (state.fieldInfos.hasNorms()) {
        NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
        assert normsFormat != null;
        normsConsumer = normsFormat.normsConsumer(state);

        for (FieldInfo fi : state.fieldInfos) {
          PerField perField = getPerField(fi.name);
          assert perField != null;

          // we must check the final value of omitNorms for the fieldinfo: it could have 
          // changed for this field since the first time we added it.
          if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
            assert perField.norms != null: "field=" + fi.name;
            perField.norms.finish(state.segmentInfo.maxDoc());
            perField.norms.flush(state, sortMap, normsConsumer);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsConsumer);
      } else {
        IOUtils.closeWhileHandlingException(normsConsumer);
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void abort() throws IOException{
    // finalizer will e.g. close any open files in the term vectors writer:
    try (Closeable finalizer = termsHash::abort){
      storedFieldsConsumer.abort();
    } finally {
      Arrays.fill(fieldHash, null);
    }
  }

  private void rehash() {
    // 扩容两倍
    int newHashSize = (fieldHash.length*2);
    assert newHashSize > fieldHash.length;

    PerField[] newHashArray = new PerField[newHashSize];

    // Rehash
    // 处理拉链的rehash
    int newHashMask = newHashSize-1;
    for(int j=0;j<fieldHash.length;j++) {
      PerField fp0 = fieldHash[j];
      while(fp0 != null) {
        final int hashPos2 = fp0.fieldInfo.name.hashCode() & newHashMask;
        PerField nextFP0 = fp0.next;
        // 又是一个拉链法的后来居上而已
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the
   *  segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the
   *  segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument();
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  // 处理一个文档
  @Override
  public void processDocument(int docID, Iterable<? extends IndexableField> document) throws IOException {

    // How many indexed field names we've seen (collapses
    // multiple field instances by the same name):
    // 当前doc里的count递增编号
    int fieldCount = 0;

    long fieldGen = nextFieldGen++;

    // NOTE: we need two passes here, in case there are
    // multi-valued fields, because we must process all
    // instances of a given field at once, since the
    // analyzer is free to reuse TokenStream across fields
    // (i.e., we cannot have more than one TokenStream
    // running "at once"):

    // start
    termsHash.startDocument();

    // start
    startStoredFields(docID);
    try {
      // 当前doc里面的所有field
      for (IndexableField field : document) {
        fieldCount = processField(docID, field, fieldGen, fieldCount);
      }
    } finally {
      if (hasHitAbortingException == false) {
        // Finish each indexed field name seen in the document:
        for (int i=0;i<fieldCount;i++) {
          fields[i].finish(docID);
        }
        finishStoredFields();
      }
    }

    try {
      // end
      termsHash.finishDocument(docID);
    } catch (Throwable th) {
      // Must abort, on the possibility that on-disk term
      // vectors are now corrupt:
      abortingExceptionConsumer.accept(th);
      throw th;
    }
  }

  /**
   *  索引写入过程中，处理每一个Field的地方
   *  <il>
   *  <li>1. 是否要索引，处理倒排信息</li>
   *  <li>2. 是否要存储，处理正排信息</li>
   *  <li>3. 是否要咋的，我没看懂</li>
   *  </il>
   *  之后就完事了
   *
   */
  private int processField(int docID, IndexableField field, long fieldGen, int fieldCount) throws IOException {
    // 名字
    String fieldName = field.name();
    // FieldType. 制定了一大堆，是否要存储，是否要分词，是否省略标准化过程等等
    IndexableFieldType fieldType = field.fieldType();

    PerField fp = null;

    // 索引选项，可以指定都要索引哪些内容
    if (fieldType.indexOptions() == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + field.name() + "\")");
    }

    // Invert indexed fields:
    // 处理需要index的东西，那就是搞倒排
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      fp = getOrAddField(fieldName, fieldType, true);
      // 只要fp.fieldGen=-1,就会为真，此时意味着这个field是所有文档中的第一次出现,那肯定是first了。此时fieldGen是文档编号
      // 假设fp.fieldGen=2,从第二个文档开始就没见过了,fieldGen=10, 就会为真，此时意味着这个field是<当前文档>中的第一次出现,那肯定是first了。此时fieldGen是文档编号
      boolean first = fp.fieldGen != fieldGen;
      // 把倒排内容搞起来, 开始看了
      fp.invert(docID, field, first);

      if (first) {
        // 如果当前文档中第一次出现这个field。
        // 就记录下，当前文档中所有field，加一个,把PerField缓存起来的作用么？
        fields[fieldCount++] = fp;
        // 第一次出现的时候，改下fieldGen,这样后面就能认出来不是第一次了
        fp.fieldGen = fieldGen;
      }
    } else {
      verifyUnIndexedFieldType(fieldName, fieldType);
    }

    // Add stored fields:
    // 处理需要stored的内容，那就是保存个正排
    // 如果一个Field要存储，那么在这里处理一些东西
    // 熟悉的地方回来了，　之前学习fdt,fdx,fdm三个文件入口就在这里哦
    if (fieldType.stored()) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      if (fieldType.stored()) {
        String value = field.stringValue();
        if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
          throw new IllegalArgumentException("stored field \"" + field.name() + "\" is too large (" + value.length() + " characters) to store");
        }
        try {
          storedFieldsConsumer.writeField(fp.fieldInfo, field);
        } catch (Throwable th) {
          onAbortingException(th);
          throw th;
        }
      }
    }

    // 处理docValue，第二份正排,准确的说是，一个列存储的正排
    DocValuesType dvType = fieldType.docValuesType();
    if (dvType == null) {
      throw new NullPointerException("docValuesType must not be null (field: \"" + fieldName + "\")");
    }
    if (dvType != DocValuesType.NONE) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexDocValue(docID, fp, dvType, field);
    }
    if (fieldType.pointDimensionCount() != 0) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexPoint(docID, fp, field);
    }
    
    return fieldCount;
  }

  // check下这个field是否真的完全不需要任何索引动作
  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException("cannot store term vectors "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException("cannot store term vector positions "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException("cannot store term vector offsets "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException("cannot store term vector payloads "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
  }

  /** Called from processDocument to index one field's point */
  private void indexPoint(int docID, PerField fp, IndexableField field) {
    int pointDimensionCount = field.fieldType().pointDimensionCount();
    int pointIndexDimensionCount = field.fieldType().pointIndexDimensionCount();

    int dimensionNumBytes = field.fieldType().pointNumBytes();

    // Record dimensions for this field; this setter will throw IllegalArgExc if
    // the dimensions were already set to something different:
    if (fp.fieldInfo.getPointDimensionCount() == 0) {
      // 设置dimensions
      fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);
    }

    fp.fieldInfo.setPointDimensions(pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);

    if (fp.pointValuesWriter == null) {
      fp.pointValuesWriter = new PointValuesWriter(byteBlockAllocator, bytesUsed, fp.fieldInfo);
    }
    fp.pointValuesWriter.addPackedValue(docID, field.binaryValue());
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType) throws IOException {
    for (SortField sortField : indexSort.getSort()) {
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalStateException("Cannot sort index with sort order " + sortField);
      }
      sorter.getDocComparator(new DocValuesLeafReader() {
        @Override
        public NumericDocValues getNumericDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.NUMERIC) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be NUMERIC but it is [" + dvType + "]");
          }
          return DocValues.emptyNumeric();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.BINARY) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be BINARY but it is [" + dvType + "]");
          }
          return DocValues.emptyBinary();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED but it is [" + dvType + "]");
          }
          return DocValues.emptySorted();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_NUMERIC) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED_NUMERIC but it is [" + dvType + "]");
          }
          return DocValues.emptySortedNumeric();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_SET) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED_SET but it is [" + dvType + "]");
          }
          return DocValues.emptySortedSet();
        }

        @Override
        public FieldInfos getFieldInfos() {
          throw new UnsupportedOperationException();
        }
      }, 0);
    }
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

    if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
      // This is the first time we are seeing this field indexed with doc values, so we
      // now record the DV type so that any future attempt to (illegally) change
      // the DV type of this field, will throw an IllegalArgExc:
      if (indexWriterConfig.getIndexSort() != null) {
        final Sort indexSort = indexWriterConfig.getIndexSort();
        validateIndexSortDVType(indexSort, fp.fieldInfo.name, dvType);
      }
      // 设置docValueType
      fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);
    }

    fp.fieldInfo.setDocValuesType(dvType);

    switch(dvType) {

      case NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        if (field.numericValue() == null) {
          throw new IllegalArgumentException("field=\"" + fp.fieldInfo.name + "\": null value not allowed");
        }
        ((NumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new BinaryDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;
        
      case SORTED_NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedNumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedNumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedSetDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }

  /** Returns a previously created {@link PerField}, or null
   *  if this field name wasn't seen yet. */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  /** Returns a previously created {@link PerField},
   *  absorbing the type information from {@link FieldType},
   *  and creates a new {@link PerField} if this field name
   *  wasn't seen yet. */
  // 获取or新建一个PerField
  private PerField getOrAddField(String name, IndexableFieldType fieldType, boolean invert) {

    // Make sure we have a PerField allocated
    // hashPosition , 哈希值取膜, 求在hash表中的位置呢
    final int hashPos = name.hashCode() & hashMask;
    // 初始化size=2
    PerField fp = fieldHash[hashPos];
    // 这里：找对了hash值，然后在翻链表找
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }

    if (fp == null) {
      // First time we are seeing this field in this segment
      // 拿不到, 意味着当前分片中我们第一次见这个Field

      // 一个初始值, 除了名字和number是对的, 其他都是默认值
      // 刚才走远了，重新来，这里从FieldInfos里面创建、获取域信息
      FieldInfo fi = fieldInfos.getOrAdd(name);
      // 给FieldInfo设置索引信息
      initIndexOptions(fi, fieldType.indexOptions());
      Map<String, String> attributes = fieldType.getAttributes();
      // 设置属性信息
      if (attributes != null) {
        attributes.forEach(fi::putAttribute);
      }

      // 新建的一个fp
      fp = new PerField(indexCreatedVersionMajor, fi, invert,
          indexWriterConfig.getSimilarity(), indexWriterConfig.getInfoStream(), indexWriterConfig.getAnalyzer());

      // 拉链法hash-table, 后来者居上，把之前的往next上挪一下
      fp.next = fieldHash[hashPos];
      fieldHash[hashPos] = fp;

      totalFieldCount++;

      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length/2) {
        rehash();
      }
      // 扩容,　拷贝一下
      if (totalFieldCount > fields.length) {
        PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }

    } else if (invert && fp.invertState == null) {
      // 内存里已经有缓存这个fp了。所以我们直接设置下索引选项就好了
      initIndexOptions(fp.fieldInfo, fieldType.indexOptions());
      fp.setInvertState();
    }

    return fp;
  }

  private void initIndexOptions(FieldInfo info, IndexOptions indexOptions) {
    // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
    // IndexOptions to decide what arrays it must create).
    assert info.getIndexOptions() == IndexOptions.NONE;
    // This is the first time we are seeing this field indexed, so we now
    // record the index options so that any future attempt to (illegally)
    // change the index options of this field, will throw an IllegalArgExc:
    // 给这个域设置索引选项
    fieldInfos.globalFieldNumbers.setIndexOptions(info.number, info.name, indexOptions);
    info.setIndexOptions(indexOptions);
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get() + storedFieldsConsumer.accountable.ramBytesUsed()
        + termVectorsWriter.accountable.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Arrays.asList(storedFieldsConsumer.accountable, termVectorsWriter.accountable);
  }

  /** NOTE: not static: accesses at least docState, termsHash. */
  // 意义在哪？　又在FieldInfo上面封装了几个字段呗
  private final class PerField implements Comparable<PerField> {

    // 版本号，lucene的大版本号
    final int indexCreatedVersionMajor;
    // 真实信息
    final FieldInfo fieldInfo;
    // 不认识
    final Similarity similarity;

    // Field 倒排时候的中间状态
    FieldInvertState invertState;
    // 不认识
    TermsHashPerField termsHashPerField;

    // Non-null if this field ever had doc values in this
    // segment:
    // 不认识
    DocValuesWriter<?> docValuesWriter;

    // Non-null if this field ever had points in this segment:
    // 不认识
    PointValuesWriter pointValuesWriter;

    /** We use this to know when a PerField is seen for the
     *  first time in the current document. */
    // 初始值是-1
    long fieldGen = -1;

    // Used by the hash table
    // 链表
    PerField next;

    // Lazy init'd:
    // b标准化写入咯
    NormValuesWriter norms;
    
    // reused
    // 分词
    TokenStream tokenStream;
    // 不认识
    private final InfoStream infoStream;
    // 分词器, 初始化配置的时候需要指定一个
    private final Analyzer analyzer;

    PerField(int indexCreatedVersionMajor, FieldInfo fieldInfo, boolean invert, Similarity similarity, InfoStream infoStream, Analyzer analyzer) {
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.fieldInfo = fieldInfo;
      this.similarity = similarity;
      this.infoStream = infoStream;
      this.analyzer = analyzer;
      if (invert) {
        setInvertState();
      }
    }

    void setInvertState() {
      invertState = new FieldInvertState(indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
      termsHashPerField = termsHash.addField(invertState, fieldInfo);
      // 如果需要标准化，才初始化标准化相关东西
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this segment:
        norms = new NormValuesWriter(fieldInfo, bytesUsed);
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    public void finish(int docID) throws IOException {
      if (fieldInfo.omitsNorms() == false) {
        long normValue;
        if (invertState.length == 0) {
          // the field exists in this document, but it did not have
          // any indexed tokens, so we assign a default value of zero
          // to the norm
          normValue = 0;
        } else {
          normValue = similarity.computeNorm(invertState);
          if (normValue == 0) {
            throw new IllegalStateException("Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docID, normValue);
      }

      termsHashPerField.finish();
    }

    /** Inverts one field for one document; first is true
     *  if this is the first time we are seeing this field
     *  name in this document. */
    // 搞倒排
    // field = Field.class
    public void invert(int docID, IndexableField field, boolean first) throws IOException {
      if (first) {
        // 我们在这个文档中，第一次索引到这个field的时候, 先reset 复位。
        // First time we're seeing this field (indexed) in
        // this document:
        invertState.reset();
      }

      IndexableFieldType fieldType = field.fieldType();

      // 设置下索引选项，之后会check，不符合条件就挂了
      IndexOptions indexOptions = fieldType.indexOptions();
      fieldInfo.setIndexOptions(indexOptions);

      // 是否不需要归一化
      if (fieldType.omitNorms()) {
        fieldInfo.setOmitsNorms();
      }

      // 是否分词
      final boolean analyzed = fieldType.tokenized() && analyzer != null;
        
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
       * but rather a finally that takes note of the problem.
       * // 为了帮助人们追踪分词组件中的问题，　我们希望当失败的时候，　把fieldName写入infostream.
       * 我们希望调用方实际的处理异常，所以我们不写任何的catch语句
       * 但是在最后还是要注意这个问题。
       */
      boolean succeededInProcessingField = false; // 处理field是否成功
      // 从当前field中获取一个流，这个流可以不断的拿东西，所以可以for循环，流最后需要关闭
      try (TokenStream stream = tokenStream = field.tokenStream(analyzer, tokenStream)) {
        // reset the TokenStream to the first token
        stream.reset();
        // 确保inverState 中的是这个stream. 而这个stream刚刚reset过。所以这里是空的,
        // 这里相当与把当前的tokenStream直接引用了一份给到了invertState. 那么当tokenStream进行incr时，　中间状态也就变了，因为他持有的是别人的引用
        invertState.setAttributeSource(stream);
        // FreqProxTermsWriterPerField
        termsHashPerField.start(field, first);

        // 上面说的for循环，其实就是stream的使用方式而已
        // 这里的stream是一个装饰器
        // stream = StopFilter -> LowCase -> StandardTokenizer.
        // 就是还有下一个的意思
        // 判断offset/词频什么的, 就是在这个下一步的操作里.
        while (stream.incrementToken()) {

          // If we hit an exception in stream.next below
          // (which is fairly common, e.g. if analyzer
          // chokes on a given document), then it's
          // non-aborting and (above) this one document
          // will be marked as deleted, but still
          // consume a docID
          // 所以这里是，每一个域的每一个token都会来一次。

          // PackedTokenAttributeImpl
          // term 在文档/field中的位置，那么第一次进来，这个值肯定是1. 因此你分词也要讲究基本法，第一个就是１．
          int posIncr = invertState.posIncrAttribute.getPositionIncrement();
          // 倒排状态的位置
          // 初始化为-1
          invertState.position += posIncr;

          // 这里是对位置的检查
          if (invertState.position < invertState.lastPosition) {
            if (posIncr == 0) {
              throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException("position increment must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
            } else {
              throw new IllegalArgumentException("position overflowed Integer.MAX_VALUE (got posIncr=" + posIncr + " lastPosition=" + invertState.lastPosition + " position=" + invertState.position + ") for field '" + field.name() + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException("position " + invertState.position + " is too large for field '" + field.name() + "': max allowed position is " + IndexWriter.MAX_POSITION);
          }

          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }
              
          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
          // 又检查
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                               + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            invertState.length = Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("too many tokens for field \"" + field.name() + "\"");
          }
          
          //System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          // 那么这里就是唯一的重点咯，刚才把一部分状态算出来，给到了倒排状态，现在把他写入或者怎么样
          // termsHashPerField = FreqProxTermsWriterPerField
          try {
            // 给这个docId,添加这堆byte[]作为信息.
            termsHashPerField.add(invertState.termAttribute.getBytesRef(), docID);
          } catch (MaxBytesLengthExceededException e) {
            // 相当于回滚逻辑，不看
            byte[] prefix = new byte[30];
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
            String msg = "Document contains at least one immense term in field=\"" + fieldInfo.name + "\" (whose UTF8 encoding is longer than the max length " + IndexWriter.MAX_TERM_LENGTH + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + Arrays.toString(prefix) + "...', original message: " + e.getMessage();
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (Throwable th) {
            onAbortingException(th);
            throw th;
          }
        }

        // trigger streams to perform end-of-stream operations
        // 流里的一项完成
        stream.end();

        // TODO: maybe add some safety? then again, it's already checked 
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement();
        invertState.offset += invertState.offsetAttribute.endOffset();

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } finally {
        if (!succeededInProcessingField && infoStream.isEnabled("DW")) {
          infoStream.message("DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) {
        invertState.position += analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += analyzer.getOffsetGap(fieldInfo.name);
      }
    }
  }

  @Override
  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocValues();
      }
    }
    return null;
  }

  private static class IntBlockAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;

    IntBlockAllocator(Counter bytesUsed) {
      super(IntBlockPool.INT_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    /* Allocate another int[] from the shared pool */
    @Override
    public int[] getIntBlock() {
      int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
      bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES);
      return b;
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int offset, int length) {
      bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES)));
    }

  }

}
