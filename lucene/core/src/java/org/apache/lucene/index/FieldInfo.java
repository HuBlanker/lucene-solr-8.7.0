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


import java.util.Map;
import java.util.Objects;

/**
 *  Access to the Field Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Field Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 *
 *  访问fieldInfo文件，该文件描述了field是否要被索引。
 *  每个segment拥有独立的FieldInfo文件。
 *  这个类的实例对于多个reader来说是线程安全的，但是同一时间只能一个线程进行添加document操作，且同时不能有别的reader/writer访问该实例。
 *
 *  其实就是说，是个读写锁呗，可以多线程读,但是写操作是完全互斥的，写和读也互斥。
 *
 *  // 保存了很多的属性，然后提供了对这些属性的getter/setter. 及每次操作后的参数合法性校验
 *  TODO: 每个属性的详细含义
 **/

public final class FieldInfo {
  // field名字
  /** Field's name */
  public final String name;
  // 内部编号，TODO 具体编号值生成方法待查
  /** Internal field number */
  public final int number;

  // 内容的类型
  private DocValuesType docValuesType = DocValuesType.NONE;

  // True if any document indexed term vectors
  // 是否索引termVectors, 暂时忽略
  private boolean storeTermVector;

  // 忽略关联啥玩意的
  private boolean omitNorms; // omit norms associated with indexed fields  

  private IndexOptions indexOptions = IndexOptions.NONE;
  // 不知道
  private boolean storePayloads; // whether this field stores payloads together with term positions

  private final Map<String,String> attributes;

  private long dvGen;

  // 不懂
  /** If both of these are positive it means this field indexed points
   *  (see {@link org.apache.lucene.codecs.PointsFormat}). */
  private int pointDimensionCount;
  private int pointIndexDimensionCount;
  private int pointNumBytes;

  // 不懂
  // whether this field is used as the soft-deletes field
  private final boolean softDeletesField;

  /**
   * Sole constructor.
   *
   * @lucene.experimental
   */
  public FieldInfo(String name, int number, boolean storeTermVector, boolean omitNorms, boolean storePayloads,
                   IndexOptions indexOptions, DocValuesType docValues, long dvGen, Map<String,String> attributes,
                   int pointDimensionCount, int pointIndexDimensionCount, int pointNumBytes, boolean softDeletesField) {
    this.name = Objects.requireNonNull(name);
    this.number = number;
    this.docValuesType = Objects.requireNonNull(docValues, "DocValuesType must not be null (field: \"" + name + "\")");
    this.indexOptions = Objects.requireNonNull(indexOptions, "IndexOptions must not be null (field: \"" + name + "\")");
    if (indexOptions != IndexOptions.NONE) {
      this.storeTermVector = storeTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
    }
    this.dvGen = dvGen;
    this.attributes = Objects.requireNonNull(attributes);
    this.pointDimensionCount = pointDimensionCount;
    this.pointIndexDimensionCount = pointIndexDimensionCount;
    this.pointNumBytes = pointNumBytes;
    this.softDeletesField = softDeletesField;
    this.checkConsistency();
  }

  /** 
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException)
   * 一致性检查
   * 其实就是个参数合理性的校验，那么多bool参数，之间有互斥等因素
   */
  public boolean checkConsistency() {
    if (indexOptions != IndexOptions.NONE) {
      // Cannot store payloads unless positions are indexed:
      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0 && storePayloads) {
        throw new IllegalStateException("indexed field '" + name + "' cannot have payloads without positions");
      }
    } else {
      if (storeTermVector) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store term vectors");
      }
      if (storePayloads) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store payloads");
      }
      if (omitNorms) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot omit norms");
      }
    }

    if (pointDimensionCount < 0) {
      throw new IllegalStateException("pointDimensionCount must be >= 0; got " + pointDimensionCount);
    }

    if (pointIndexDimensionCount < 0) {
      throw new IllegalStateException("pointIndexDimensionCount must be >= 0; got " + pointIndexDimensionCount);
    }

    if (pointNumBytes < 0) {
      throw new IllegalStateException("pointNumBytes must be >= 0; got " + pointNumBytes);
    }

    if (pointDimensionCount != 0 && pointNumBytes == 0) {
      throw new IllegalStateException("pointNumBytes must be > 0 when pointDimensionCount=" + pointDimensionCount);
    }

    if (pointIndexDimensionCount != 0 && pointDimensionCount == 0) {
      throw new IllegalStateException("pointIndexDimensionCount must be 0 when pointDimensionCount=0");
    }

    if (pointNumBytes != 0 && pointDimensionCount == 0) {
      throw new IllegalStateException("pointDimensionCount must be > 0 when pointNumBytes=" + pointNumBytes);
    }
    
    if (dvGen != -1 && docValuesType == DocValuesType.NONE) {
      throw new IllegalStateException("field '" + name + "' cannot have a docvalues update generation without having docvalues");
    }

    return true;
  }

  // should only be called by FieldInfos#addOrUpdate

  /**
   *
   * 赋值操作，　把当前的所有信息改了，改完检查一下参数
   */
  void update(boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions,
              Map<String, String> attributes, int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
    if (indexOptions == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + name + "\")");
    }
    //System.out.println("FI.update field=" + name + " indexed=" + indexed + " omitNorms=" + omitNorms + " this.omitNorms=" + this.omitNorms);
    if (this.indexOptions != indexOptions) {
      if (this.indexOptions == IndexOptions.NONE) {
        this.indexOptions = indexOptions;
      } else if (indexOptions != IndexOptions.NONE) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + this.indexOptions + " to inconsistent index options=" + indexOptions);
      }
    }

    if (this.pointDimensionCount == 0 && dimensionCount != 0) {
      this.pointDimensionCount = dimensionCount;
      this.pointIndexDimensionCount = indexDimensionCount;
      this.pointNumBytes = dimensionNumBytes;
    } else if (dimensionCount != 0 && (this.pointDimensionCount != dimensionCount || this.pointIndexDimensionCount != indexDimensionCount || this.pointNumBytes != dimensionNumBytes)) {
      throw new IllegalArgumentException("cannot change field \"" + name + "\" from points dimensionCount=" + this.pointDimensionCount + ", indexDimensionCount=" + this.pointIndexDimensionCount + ", numBytes=" + this.pointNumBytes + " to inconsistent dimensionCount=" + dimensionCount +", indexDimensionCount=" + indexDimensionCount + ", numBytes=" + dimensionNumBytes);
    }

    if (this.indexOptions != IndexOptions.NONE) { // if updated field data is not for indexing, leave the updates out
      this.storeTermVector |= storeTermVector;                // once vector, always vector
      this.storePayloads |= storePayloads;

      // Awkward: only drop norms if incoming update is indexed:
      if (indexOptions != IndexOptions.NONE && this.omitNorms != omitNorms) {
        this.omitNorms = true;                // if one require omitNorms at least once, it remains off for life
      }
    }
    if (this.indexOptions == IndexOptions.NONE || this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // cannot store payloads if we don't store positions:
      this.storePayloads = false;
    }
    if (attributes != null) {
      this.attributes.putAll(attributes);
    }
    this.checkConsistency();
  }

  /** Record that this field is indexed with points, with the
   *  specified number of dimensions and bytes per dimension. */
  // set方法，设置一下point相关的几个值
  public void setPointDimensions(int dimensionCount, int indexDimensionCount, int numBytes) {
    if (dimensionCount <= 0) {
      throw new IllegalArgumentException("point dimension count must be >= 0; got " + dimensionCount + " for field=\"" + name + "\"");
    }
    if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
      throw new IllegalArgumentException("point index dimension count must be < PointValues.MAX_INDEX_DIMENSIONS (= " + PointValues.MAX_INDEX_DIMENSIONS + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (indexDimensionCount > dimensionCount) {
      throw new IllegalArgumentException("point index dimension count must be <= point dimension count (= " + dimensionCount + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (numBytes <= 0) {
      throw new IllegalArgumentException("point numBytes must be >= 0; got " + numBytes + " for field=\"" + name + "\"");
    }
    if (numBytes > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException("point numBytes must be <= PointValues.MAX_NUM_BYTES (= " + PointValues.MAX_NUM_BYTES + "); got " + numBytes + " for field=\"" + name + "\"");
    }
    if (pointDimensionCount != 0 && pointDimensionCount != dimensionCount) {
      throw new IllegalArgumentException("cannot change point dimension count from " + pointDimensionCount + " to " + dimensionCount + " for field=\"" + name + "\"");
    }
    if (pointIndexDimensionCount != 0 && pointIndexDimensionCount != indexDimensionCount) {
      throw new IllegalArgumentException("cannot change point index dimension count from " + pointIndexDimensionCount + " to " + indexDimensionCount + " for field=\"" + name + "\"");
    }
    if (pointNumBytes != 0 && pointNumBytes != numBytes) {
      throw new IllegalArgumentException("cannot change point numBytes from " + pointNumBytes + " to " + numBytes + " for field=\"" + name + "\"");
    }

    pointDimensionCount = dimensionCount;
    pointIndexDimensionCount = indexDimensionCount;
    pointNumBytes = numBytes;

    this.checkConsistency();
  }

  /** Return point data dimension count */
  public int getPointDimensionCount() {
    return pointDimensionCount;
  }

  /** Return point data dimension count */
  public int getPointIndexDimensionCount() {
    return pointIndexDimensionCount;
  }

  /** Return number of bytes per dimension */
  public int getPointNumBytes() {
    return pointNumBytes;
  }

  /** Record that this field is indexed with docvalues, with the specified type */
  public void setDocValuesType(DocValuesType type) {
    if (type == null) {
      throw new NullPointerException("DocValuesType must not be null (field: \"" + name + "\")");
    }
    if (docValuesType != DocValuesType.NONE && type != DocValuesType.NONE && docValuesType != type) {
      throw new IllegalArgumentException("cannot change DocValues type from " + docValuesType + " to " + type + " for field \"" + name + "\"");
    }
    docValuesType = type;
    this.checkConsistency();
  }
  
  /** Returns IndexOptions for the field, or IndexOptions.NONE if the field is not indexed */
  public IndexOptions getIndexOptions() {
    return indexOptions;
  }

  /** Record the {@link IndexOptions} to use with this field. */
  public void setIndexOptions(IndexOptions newIndexOptions) {
    if (indexOptions != newIndexOptions) {
      if (indexOptions == IndexOptions.NONE) {
        indexOptions = newIndexOptions;
      } else if (newIndexOptions != IndexOptions.NONE) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + indexOptions + " to inconsistent index options=" + newIndexOptions);
      }
    }

    if (indexOptions == IndexOptions.NONE || indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // cannot store payloads if we don't store positions:
      storePayloads = false;
    }
    this.checkConsistency();
  }
  
  /**
   * Returns {@link DocValuesType} of the docValues; this is
   * {@code DocValuesType.NONE} if the field has no docvalues.
   */
  public DocValuesType getDocValuesType() {
    return docValuesType;
  }
  
  /** Sets the docValues generation of this field. */
  void setDocValuesGen(long dvGen) {
    this.dvGen = dvGen;
    this.checkConsistency();
  }
  
  /**
   * Returns the docValues generation of this field, or -1 if no docValues
   * updates exist for it.
   */
  public long getDocValuesGen() {
    return dvGen;
  }
  
  void setStoreTermVectors() {
    storeTermVector = true;
    this.checkConsistency();
  }
  
  void setStorePayloads() {
    if (indexOptions != IndexOptions.NONE && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      storePayloads = true;
    }
    this.checkConsistency();
  }

  /**
   * Returns true if norms are explicitly omitted for this field
   */
  public boolean omitsNorms() {
    return omitNorms;
  }

  /** Omit norms for this field. */
  public void setOmitsNorms() {
    if (indexOptions == IndexOptions.NONE) {
      throw new IllegalStateException("cannot omit norms: this field is not indexed");
    }
    omitNorms = true;
    this.checkConsistency();
  }
  
  /**
   * Returns true if this field actually has any norms.
   */
  public boolean hasNorms() {
    return indexOptions != IndexOptions.NONE && omitNorms == false;
  }
  
  /**
   * Returns true if any payloads exist for this field.
   */
  public boolean hasPayloads() {
    return storePayloads;
  }
  
  /**
   * Returns true if any term vectors exist for this field.
   */
  public boolean hasVectors() {
    return storeTermVector;
  }
  
  /**
   * Get a codec attribute value, or null if it does not exist
   */
  public String getAttribute(String key) {
    return attributes.get(key);
  }
  
  /**
   * Puts a codec attribute value.
   * <p>
   * This is a key-value mapping for the field that the codec can use
   * to store additional metadata, and will be available to the codec
   * when reading the segment via {@link #getAttribute(String)}
   * <p>
   * If a value already exists for the key in the field, it will be replaced with
   * the new value. If the value of the attributes for a same field is changed between
   * the documents, the behaviour after merge is undefined.
   */
  public String putAttribute(String key, String value) {
    return attributes.put(key, value);
  }
  
  /**
   * Returns internal codec attributes map.
   */
  public Map<String,String> attributes() {
    return attributes;
  }

  /**
   * Returns true if this field is configured and used as the soft-deletes field.
   * See {@link IndexWriterConfig#softDeletesField}
   */
  public boolean isSoftDeletesField() {
    return softDeletesField;
  }
}
