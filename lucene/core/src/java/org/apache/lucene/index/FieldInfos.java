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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.lucene.util.ArrayUtil;

/** 
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 * 继承一个迭代器. 来成为一个FieldInfo的集合。可以遍历等
 *  @lucene.experimental
 */
public class FieldInfos implements Iterable<FieldInfo> {

  /** An instance without any fields. */
  public final static FieldInfos EMPTY = new FieldInfos(new FieldInfo[0]);

  // 一堆has
  // 原来这个变量，　仅仅是所有field的对应的变量的聚合
  // 代表着，　所有的Field中，有某一个是HasFreq的，那么这里就是true
  private final boolean hasFreq;
  private final boolean hasProx;
  private final boolean hasPayloads;
  private final boolean hasOffsets;
  private final boolean hasVectors;
  private final boolean hasNorms;
  private final boolean hasDocValues;
  private final boolean hasPointValues;
  private final String softDeletesField;
  
  // used only by fieldInfo(int)
  // 实际保存所有FieldInfo的数组, byNumber
  private final FieldInfo[] byNumber;

  // 实际保存所有FieldInfo的map
  private final HashMap<String,FieldInfo> byName = new HashMap<>();
  private final Collection<FieldInfo> values; // for an unmodifiable iterator, 一个不能改的遍历集合
  
  /**
   * Constructs a new FieldInfos from an array of FieldInfo objects
   */
  public FieldInfos(FieldInfo[] infos) {
    boolean hasVectors = false;
    boolean hasProx = false;
    boolean hasPayloads = false;
    boolean hasOffsets = false;
    boolean hasFreq = false;
    boolean hasNorms = false;
    boolean hasDocValues = false;
    boolean hasPointValues = false;
    String softDeletesField = null;

    int size = 0; // number of elements in byNumberTemp, number of used array slots
    FieldInfo[] byNumberTemp = new FieldInfo[10]; // initial array capacity of 10
    // 传入的所有FieldInfos遍历
    for (FieldInfo info : infos) {
      // 域编号不能小于0
      if (info.number < 0) {
        throw new IllegalArgumentException("illegal field number: " + info.number + " for field " + info.name);
      }
      // size 应该永远等于number+1, 应该number可能就是个顺序编码而已。(0,1,2,3) 那么size=4
      size = info.number >= size ? info.number+1 : size;
      if (info.number >= byNumberTemp.length){ //grow array
        byNumberTemp = ArrayUtil.grow(byNumberTemp, info.number + 1);
      }
      // 如果数组中对应的位置，不为空，说明编号重复了，出现bug了
      FieldInfo previous = byNumberTemp[info.number];
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field numbers: " + previous.name + " and " + info.name + " have: " + info.number);
      }
      // 赋值
      byNumberTemp[info.number] = info;

      // 给byName的map里面放进去
      previous = byName.put(info.name, info);
      // map中也不能重复，嘻嘻
      if (previous != null) {
        throw new IllegalArgumentException("duplicate field names: " + previous.number + " and " + info.number + " have: " + info.name);
      }

      // 遍历取或，就是随便有一个就是真咯
      hasVectors |= info.hasVectors();
      hasProx |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
      hasFreq |= info.getIndexOptions() != IndexOptions.DOCS;
      hasOffsets |= info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
      hasNorms |= info.hasNorms();
      hasDocValues |= info.getDocValuesType() != DocValuesType.NONE;
      hasPayloads |= info.hasPayloads();
      hasPointValues |= (info.getPointDimensionCount() != 0);
      if (info.isSoftDeletesField()) {
        if (softDeletesField != null && softDeletesField.equals(info.name) == false) {
          throw new IllegalArgumentException("multiple soft-deletes fields [" + info.name + ", " + softDeletesField + "]");
        }
        softDeletesField = info.name;
      }
    }
    
    this.hasVectors = hasVectors;
    this.hasProx = hasProx;
    this.hasPayloads = hasPayloads;
    this.hasOffsets = hasOffsets;
    this.hasFreq = hasFreq;
    this.hasNorms = hasNorms;
    this.hasDocValues = hasDocValues;
    this.hasPointValues = hasPointValues;
    this.softDeletesField = softDeletesField;

    // 最终赋值咯
    List<FieldInfo> valuesTemp = new ArrayList<>();
    byNumber = new FieldInfo[size];
    for(int i=0; i<size; i++){
      byNumber[i] = byNumberTemp[i];
      if (byNumberTemp[i] != null) {
        valuesTemp.add(byNumberTemp[i]);
      }
    }
    values = Collections.unmodifiableCollection(Arrays.asList(valuesTemp.toArray(new FieldInfo[0])));
  }

  /** Call this to get the (merged) FieldInfos for a
   *  composite reader.
   *  <p>
   *  NOTE: the returned field numbers will likely not
   *  correspond to the actual field numbers in the underlying
   *  readers, and codec metadata ({@link FieldInfo#getAttribute(String)}
   *  will be unavailable.
   */
  public static FieldInfos getMergedFieldInfos(IndexReader reader) {
    final List<LeafReaderContext> leaves = reader.leaves();
    if (leaves.isEmpty()) {
      return FieldInfos.EMPTY;
    } else if (leaves.size() == 1) {
      return leaves.get(0).reader().getFieldInfos();
    } else {
      final String softDeletesField = leaves.stream()
          .map(l -> l.reader().getFieldInfos().getSoftDeletesField())
          .filter(Objects::nonNull)
          .findAny().orElse(null);
      final Builder builder = new Builder(new FieldNumbers(softDeletesField));
      for (final LeafReaderContext ctx : leaves) {
        builder.add(ctx.reader().getFieldInfos());
      }
      return builder.finish();
    }
  }

  /** Returns a set of names of fields that have a terms index.  The order is undefined. */
  public static Collection<String> getIndexedFields(IndexReader reader) {
    return reader.leaves().stream()
        .flatMap(l -> StreamSupport.stream(l.reader().getFieldInfos().spliterator(), false)
        .filter(fi -> fi.getIndexOptions() != IndexOptions.NONE))
        .map(fi -> fi.name)
        .collect(Collectors.toSet());
  }

  /** Returns true if any fields have freqs */
  public boolean hasFreq() {
    return hasFreq;
  }
  
  /** Returns true if any fields have positions */
  public boolean hasProx() {
    return hasProx;
  }

  /** Returns true if any fields have payloads */
  public boolean hasPayloads() {
    return hasPayloads;
  }

  /** Returns true if any fields have offsets */
  public boolean hasOffsets() {
    return hasOffsets;
  }
  
  /** Returns true if any fields have vectors */
  public boolean hasVectors() {
    return hasVectors;
  }
  
  /** Returns true if any fields have norms */
  public boolean hasNorms() {
    return hasNorms;
  }
  
  /** Returns true if any fields have DocValues */
  public boolean hasDocValues() {
    return hasDocValues;
  }

  /** Returns true if any fields have PointValues */
  public boolean hasPointValues() {
    return hasPointValues;
  }

  /** Returns the soft-deletes field name if exists; otherwise returns null */
  public String getSoftDeletesField() {
    return softDeletesField;
  }
  
  /** Returns the number of fields */
  public int size() {
    return byName.size();
  }
  
  /**
   * Returns an iterator over all the fieldinfo objects present,
   * ordered by ascending field number
   */
  // TODO: what happens if in fact a different order is used?
  @Override
  public Iterator<FieldInfo> iterator() {
    return values.iterator();
  }

  /**
   * Return the fieldinfo object referenced by the field name
   * @return the FieldInfo object or null when the given fieldName
   * doesn't exist.
   */  
  public FieldInfo fieldInfo(String fieldName) {
    return byName.get(fieldName);
  }

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   * @param fieldNumber field's number.
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   * @throws IllegalArgumentException if fieldNumber is negative
   */
  public FieldInfo fieldInfo(int fieldNumber) {
    if (fieldNumber < 0) {
      throw new IllegalArgumentException("Illegal field number: " + fieldNumber);
    }
    if (fieldNumber >= byNumber.length) {
      return null;
    }
    return byNumber[fieldNumber];
  }

  static final class FieldDimensions {
    public final int dimensionCount;
    public final int indexDimensionCount;
    public final int dimensionNumBytes;

    public FieldDimensions(int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      this.dimensionCount = dimensionCount;
      this.indexDimensionCount = indexDimensionCount;
      this.dimensionNumBytes = dimensionNumBytes;
    }
  }

  /**
   *　这个类是主管对Field进行编号的么
   */
  static final class FieldNumbers {

    // 编号->名字的映射
    private final Map<Integer,String> numberToName;
    // 名字->编号的映射
    private final Map<String,Integer> nameToNumber;
    // 每个field的索引类型
    private final Map<String, IndexOptions> indexOptions;
    // We use this to enforce that a given field never
    // changes DV type, even across segments / IndexWriter
    // sessions:
    // 每个field的内容类型，用这个保证，即使在不同的分片，Writer，会话中，
    // 同一个Field始终是同一个类型
    private final Map<String,DocValuesType> docValuesType;

    // 每个field的形状...
    private final Map<String,FieldDimensions> dimensions;

    // TODO: we should similarly catch an attempt to turn
    // norms back on after they were already committed; today
    // we silently discard the norm but this is badly trappy
    // 最小的没使用的域编号
    private int lowestUnassignedFieldNumber = -1;

    // The soft-deletes field from IWC to enforce a single soft-deletes field
    private final String softDeletesFieldName;
    
    FieldNumbers(String softDeletesFieldName) {
      this.nameToNumber = new HashMap<>();
      this.numberToName = new HashMap<>();
      this.indexOptions = new HashMap<>();
      this.docValuesType = new HashMap<>();
      this.dimensions = new HashMap<>();
      this.softDeletesFieldName = softDeletesFieldName;
    }
    
    /**
     * Returns the global field number for the given field name. If the name
     * does not exist yet it tries to add it with the given preferred field
     * number assigned if possible otherwise the first unassigned field number
     * is used as the field number.
     *
     * <br/>
     * 根据给定的名字，　拿到全局的number,　如果没有，添加这个名字
     * 尽量使用给定的编号对其添加，　不行的话就是用第一个未使用的编号对其编号
     * <br/>
     * 如果给的名字，　已经存在了。　那就把各种信息检查一遍，替换一遍
     * 如果不存在，那就放进去，然后找个编号给它
     * <br/>
     * @param preferredFieldNumber 如果不存在，　希望使用这个Number么？
     */
    synchronized int addOrGet(String fieldName, int preferredFieldNumber, IndexOptions indexOptions, DocValuesType dvType, int dimensionCount, int indexDimensionCount, int dimensionNumBytes, boolean isSoftDeletesField) {
      if (indexOptions != IndexOptions.NONE) {
        // 检查索引选项对不对并添加
        IndexOptions currentOpts = this.indexOptions.get(fieldName);
        if (currentOpts == null) {
          this.indexOptions.put(fieldName, indexOptions);
        } else if (currentOpts != IndexOptions.NONE && currentOpts != indexOptions) {
          throw new IllegalArgumentException("cannot change field \"" + fieldName + "\" from index options=" + currentOpts + " to inconsistent index options=" + indexOptions);
        }
      }

      // 检查值的类型对不对并添加
      if (dvType != DocValuesType.NONE) {
        DocValuesType currentDVType = docValuesType.get(fieldName);
        if (currentDVType == null) {
          docValuesType.put(fieldName, dvType);
        } else if (currentDVType != DocValuesType.NONE && currentDVType != dvType) {
          throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + fieldName + "\"");
        }
      }
      // 检查FieldDim对不对并添加
      if (dimensionCount != 0) {
        FieldDimensions dims = dimensions.get(fieldName);
        if (dims != null) {
          if (dims.dimensionCount != dimensionCount) {
            throw new IllegalArgumentException("cannot change point dimension count from " + dims.dimensionCount + " to " + dimensionCount + " for field=\"" + fieldName + "\"");
          }
          if (dims.indexDimensionCount != indexDimensionCount) {
            throw new IllegalArgumentException("cannot change point index dimension count from " + dims.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + fieldName + "\"");
          }
          if (dims.dimensionNumBytes != dimensionNumBytes) {
            throw new IllegalArgumentException("cannot change point numBytes from " + dims.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + fieldName + "\"");
          }
        } else {
          dimensions.put(fieldName, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
        }
      }

      // 根据名字拿编号
      Integer fieldNumber = nameToNumber.get(fieldName);
      if (fieldNumber == null) {
        // 没拿到
        final Integer preferredBoxed = preferredFieldNumber;
        // 只要不为-1且之前没用过这个编号，那就同意你使用这个编号啦
        if (preferredFieldNumber != -1 && !numberToName.containsKey(preferredBoxed)) {
          // cool - we can use this number globally
          fieldNumber = preferredBoxed;
        } else {
          // 找一个最小的，没有使用的编号给新的Field
          // find a new FieldNumber
          while (numberToName.containsKey(++lowestUnassignedFieldNumber)) {
            // might not be up to date - lets do the work once needed
          }
          fieldNumber = lowestUnassignedFieldNumber;
        }
        assert fieldNumber >= 0;
        // 添加进去
        numberToName.put(fieldNumber, fieldName);
        nameToNumber.put(fieldName, fieldNumber);
      }

      // 没看懂这块，　什么软删除之类的
      if (isSoftDeletesField) {
        if (softDeletesFieldName == null) {
          throw new IllegalArgumentException("this index has [" + fieldName + "] as soft-deletes already but soft-deletes field is not configured in IWC");
        } else if (!fieldName.equals(softDeletesFieldName)) {
          throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as soft-deletes already");
        }
      } else if (fieldName.equals(softDeletesFieldName)) {
        throw new IllegalArgumentException("cannot configure [" + softDeletesFieldName + "] as soft-deletes; this index uses [" + fieldName + "] as non-soft-deletes already");
      }

      return fieldNumber;
    }

    /**
     * 检查一致性, 思路就是你给参数，　和我内存里保存的要符合规则，　不然报错
     */
    synchronized void verifyConsistent(Integer number, String name, IndexOptions indexOptions) {
      if (!name.equals(numberToName.get(number))) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (!number.equals(nameToNumber.get(name))) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      IndexOptions currentIndexOptions = this.indexOptions.get(name);
      if (indexOptions != IndexOptions.NONE && currentIndexOptions != null && currentIndexOptions != IndexOptions.NONE && indexOptions != currentIndexOptions) {
        throw new IllegalArgumentException("cannot change field \"" + name + "\" from index options=" + currentIndexOptions + " to inconsistent index options=" + indexOptions);
      }
    }

    /**
     * 检查一致性, 思路就是你给参数，　和我内存里保存的要符合规则，　不然报错
     */
    synchronized void verifyConsistent(Integer number, String name, DocValuesType dvType) {
      if (!name.equals(numberToName.get(number))) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (!number.equals(nameToNumber.get(name))) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      DocValuesType currentDVType = docValuesType.get(name);
      if (dvType != DocValuesType.NONE && currentDVType != null && currentDVType != DocValuesType.NONE && dvType != currentDVType) {
        throw new IllegalArgumentException("cannot change DocValues type from " + currentDVType + " to " + dvType + " for field \"" + name + "\"");
      }
    }

    /**
     * 检查一致性, 思路就是你给参数，　和我内存里保存的要符合规则，　不然报错
     */
    synchronized void verifyConsistentDimensions(Integer number, String name, int dataDimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      if (!name.equals(numberToName.get(number))) {
        throw new IllegalArgumentException("field number " + number + " is already mapped to field name \"" + numberToName.get(number) + "\", not \"" + name + "\"");
      }
      if (!number.equals(nameToNumber.get(name))) {
        throw new IllegalArgumentException("field name \"" + name + "\" is already mapped to field number \"" + nameToNumber.get(name) + "\", not \"" + number + "\"");
      }
      FieldDimensions dim = dimensions.get(name);
      if (dim != null) {
        if (dim.dimensionCount != dataDimensionCount) {
          throw new IllegalArgumentException("cannot change point dimension count from " + dim.dimensionCount + " to " + dataDimensionCount + " for field=\"" + name + "\"");
        }
        if (dim.indexDimensionCount != indexDimensionCount) {
          throw new IllegalArgumentException("cannot change point index dimension count from " + dim.indexDimensionCount + " to " + indexDimensionCount + " for field=\"" + name + "\"");
        }
        if (dim.dimensionNumBytes != dimensionNumBytes) {
          throw new IllegalArgumentException("cannot change point numBytes from " + dim.dimensionNumBytes + " to " + dimensionNumBytes + " for field=\"" + name + "\"");
        }
      }
    }

    /**
     * Returns true if the {@code fieldName} exists in the map and is of the
     * same {@code dvType}.
     * 存在不
     */
    synchronized boolean contains(String fieldName, DocValuesType dvType) {
      // used by IndexWriter.updateNumericDocValue
      if (!nameToNumber.containsKey(fieldName)) {
        return false;
      } else {
        // only return true if the field has the same dvType as the requested one
        return dvType == docValuesType.get(fieldName);
      }
    }

    @Deprecated
    synchronized Set<String> getFieldNames() {
      return Collections.unmodifiableSet(new HashSet<>(nameToNumber.keySet()));
    }

    // clear
    synchronized void clear() {
      numberToName.clear();
      nameToNumber.clear();
      indexOptions.clear();
      docValuesType.clear();
      dimensions.clear();
    }
    // ===============几个set方法，要先检查一致性=========================
    synchronized void setIndexOptions(int number, String name, IndexOptions indexOptions) {
      verifyConsistent(number, name, indexOptions);
      this.indexOptions.put(name, indexOptions);
    }

    synchronized void setDocValuesType(int number, String name, DocValuesType dvType) {
      verifyConsistent(number, name, dvType);
      docValuesType.put(name, dvType);
    }

    synchronized void setDimensions(int number, String name, int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
      if (dimensionCount > PointValues.MAX_DIMENSIONS) {
        throw new IllegalArgumentException("dimensionCount must be <= PointValues.MAX_DIMENSIONS (= " + PointValues.MAX_DIMENSIONS + "); got " + dimensionCount + " for field=\"" + name + "\"");
      }
      if (dimensionNumBytes > PointValues.MAX_NUM_BYTES) {
        throw new IllegalArgumentException("dimension numBytes must be <= PointValues.MAX_NUM_BYTES (= " + PointValues.MAX_NUM_BYTES + "); got " + dimensionNumBytes + " for field=\"" + name + "\"");
      }
      if (indexDimensionCount > dimensionCount) {
        throw new IllegalArgumentException("indexDimensionCount must be <= dimensionCount (= " + dimensionCount + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
      }
      if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
        throw new IllegalArgumentException("indexDimensionCount must be <= PointValues.MAX_INDEX_DIMENSIONS (= " + PointValues.MAX_INDEX_DIMENSIONS + "); got " + indexDimensionCount + " for field=\"" + name + "\"");
      }
      verifyConsistentDimensions(number, name, dimensionCount, indexDimensionCount, dimensionNumBytes);
      dimensions.put(name, new FieldDimensions(dimensionCount, indexDimensionCount, dimensionNumBytes));
    }
  }

  /**
   * 一个构造器
   */
  static final class Builder {
    private final HashMap<String,FieldInfo> byName = new HashMap<>();
    final FieldNumbers globalFieldNumbers;
    private boolean finished;

    /**
     * Creates a new instance with the given {@link FieldNumbers}.
     *
     * 以已有的FieldNumbers构造个构造器
     * // 参数为什么叫做全局的？
     * 因此不是这一次segment写入的，而是也要考虑之前写了一年多的索引，　放在那里，　现在读取出来也要先把他们复原
     */
    Builder(FieldNumbers globalFieldNumbers) {
      assert globalFieldNumbers != null;
      this.globalFieldNumbers = globalFieldNumbers;
    }

    // 添加一堆field进来
    public void add(FieldInfos other) {
      assert assertNotFinished();
      for(FieldInfo fieldInfo : other){ 
        add(fieldInfo);
      }
    }

    /** Create a new field, or return existing one. */
    public FieldInfo getOrAdd(String name) {
      FieldInfo fi = fieldInfo(name);
      if (fi == null) {
        assert assertNotFinished();
        // This field wasn't yet added to this in-RAM
        // segment's FieldInfo, so now we get a global
        // number for this field.  If the field was seen
        // before then we'll get the same name and number,
        // else we'll allocate a new one:
        final boolean isSoftDeletesField = name.equals(globalFieldNumbers.softDeletesFieldName);
        // 给新的field分配一个编号
        final int fieldNumber = globalFieldNumbers.addOrGet(name, -1, IndexOptions.NONE, DocValuesType.NONE, 0, 0, 0, isSoftDeletesField);
        // 新建field，　各种参数都给的默认值，false/None等
        fi = new FieldInfo(name, fieldNumber, false, false, false, IndexOptions.NONE, DocValuesType.NONE, -1, new HashMap<>(), 0, 0, 0, isSoftDeletesField);
        assert !byName.containsKey(fi.name);
        // 检查参数
        globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, DocValuesType.NONE);
        // 放起来
        byName.put(fi.name, fi);
      }

      return fi;
    }

    // 添加或更新
    private FieldInfo addOrUpdateInternal(String name, int preferredFieldNumber,
                                          boolean storeTermVector,
                                          boolean omitNorms, boolean storePayloads, IndexOptions indexOptions,
                                          DocValuesType docValues, long dvGen,
                                          Map<String, String> attributes,
                                          int dataDimensionCount, int indexDimensionCount, int dimensionNumBytes,
                                          boolean isSoftDeletesField) {
      assert assertNotFinished();
      // 参数检查
      if (docValues == null) {
        throw new NullPointerException("DocValuesType must not be null");
      }
      if (attributes != null) {
        // original attributes is UnmodifiableMap
        attributes = new HashMap<>(attributes);
      }

      FieldInfo fi = fieldInfo(name);
      if (fi == null) {
        // This field wasn't yet added to this in-RAM
        // segment's FieldInfo, so now we get a global
        // number for this field.  If the field was seen
        // before then we'll get the same name and number,
        // else we'll allocate a new one:
        // 分配编号
        final int fieldNumber = globalFieldNumbers.addOrGet(name, preferredFieldNumber, indexOptions, docValues, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
        // 新建field，注意这里不是默认值哦
        fi = new FieldInfo(name, fieldNumber, storeTermVector, omitNorms, storePayloads, indexOptions, docValues, dvGen, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes, isSoftDeletesField);
        assert !byName.containsKey(fi.name);
        // 检查
        globalFieldNumbers.verifyConsistent(Integer.valueOf(fi.number), fi.name, fi.getDocValuesType());
        byName.put(fi.name, fi);
      } else {
        // 如果已经有了，用给的参数，更新一下属性
        fi.update(storeTermVector, omitNorms, storePayloads, indexOptions, attributes, dataDimensionCount, indexDimensionCount, dimensionNumBytes);

        if (docValues != DocValuesType.NONE) {
          // Only pay the synchronization cost if fi does not already have a DVType
          boolean updateGlobal = fi.getDocValuesType() == DocValuesType.NONE;
          if (updateGlobal) {
            // Must also update docValuesType map so it's
            // aware of this field's DocValuesType.  This will throw IllegalArgumentException if
            // an illegal type change was attempted.
            globalFieldNumbers.setDocValuesType(fi.number, name, docValues);
          }

          fi.setDocValuesType(docValues); // this will also perform the consistency check.
          fi.setDocValuesGen(dvGen);
        }
      }
      return fi;
    }

    // add 一个构造好的FieldInfo
    public FieldInfo add(FieldInfo fi) {
      return add(fi, -1);
    }

    // add
    public FieldInfo add(FieldInfo fi, long dvGen) {
      // IMPORTANT - reuse the field number if possible for consistent field numbers across segments
      // 尽量重用编号, 在不同的Segment之间
      return addOrUpdateInternal(fi.name, fi.number, fi.hasVectors(),
                                 fi.omitsNorms(), fi.hasPayloads(),
                                 fi.getIndexOptions(), fi.getDocValuesType(), dvGen,
                                 fi.attributes(),
                                 fi.getPointDimensionCount(), fi.getPointIndexDimensionCount(), fi.getPointNumBytes(),
                                 fi.isSoftDeletesField());
    }

    // 根据名字拿咯
    public FieldInfo fieldInfo(String fieldName) {
      return byName.get(fieldName);
    }

    /** Called only from assert */
    private boolean assertNotFinished() {
      if (finished) {
        throw new IllegalStateException("FieldInfos.Builder was already finished; cannot add new fields");
      }
      return true;
    }

    // 构造完成，　返回一个完整的FieldInfos，那么此时必然是所有doc已经写入了，　
    // 生成一个这个，　按照格式写入磁盘，等着用
    FieldInfos finish() {
      finished = true;
      return new FieldInfos(byName.values().toArray(new FieldInfo[byName.size()]));
    }
  }
}
