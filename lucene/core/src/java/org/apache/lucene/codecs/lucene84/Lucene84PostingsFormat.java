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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 5.0 postings format, which encodes postings in packed integer blocks
 * for fast decode.
 *
 * <p>
 * Basic idea:
 * <ul>
 *   <li>
 *   <b>Packed Blocks and VInt Blocks</b>:
 *   <p>In packed blocks, integers are encoded with the same bit width ({@link PackedInts packed format}):
 *      the block size (i.e. number of integers inside block) is fixed (currently 128). Additionally blocks
 *      that are all the same value are encoded in an optimized way.</p>
 *   <p>In VInt blocks, integers are encoded as {@link DataOutput#writeVInt VInt}:
 *      the block size is variable.</p>
 *   </li>
 *
 *   <li>
 *   <b>Block structure</b>:
 *   <p>When the postings are long enough, Lucene84PostingsFormat will try to encode most integer data
 *      as a packed block.</p>
 *   <p>Take a term with 259 documents as an example, the first 256 document ids are encoded as two packed
 *      blocks, while the remaining 3 are encoded as one VInt block. </p>
 *   <p>Different kinds of data are always encoded separately into different packed blocks, but may
 *      possibly be interleaved into the same VInt block. </p>
 *   <p>This strategy is applied to pairs:
 *      &lt;document number, frequency&gt;,
 *      &lt;position, payload length&gt;,
 *      &lt;position, offset start, offset length&gt;, and
 *      &lt;position, payload length, offsetstart, offset length&gt;.</p>
 *   </li>
 *
 *   <li>
 *   <b>Skipdata settings</b>:
 *   <p>The structure of skip table is quite similar to previous version of Lucene. Skip interval is the
 *      same as block size, and each skip entry points to the beginning of each block. However, for
 *      the first block, skip data is omitted.</p>
 *   </li>
 *
 *   <li>
 *   <b>Positions, Payloads, and Offsets</b>:
 *   <p>A position is an integer indicating where the term occurs within one document.
 *      A payload is a blob of metadata associated with current position.
 *      An offset is a pair of integers indicating the tokenized start/end offsets for given term
 *      in current position: it is essentially a specialized payload. </p>
 *   <p>When payloads and offsets are not omitted, numPositions==numPayloads==numOffsets (assuming a
 *      null payload contributes one count). As mentioned in block structure, it is possible to encode
 *      these three either combined or separately.
 *   <p>In all cases, payloads and offsets are stored together. When encoded as a packed block,
 *      position data is separated out as .pos, while payloads and offsets are encoded in .pay (payload
 *      metadata will also be stored directly in .pay). When encoded as VInt blocks, all these three are
 *      stored interleaved into the .pos (so is payload metadata).</p>
 *   <p>With this strategy, the majority of payload and offset data will be outside .pos file.
 *      So for queries that require only position data, running on a full index with payloads and offsets,
 *      this reduces disk pre-fetches.</p>
 *   </li>
 * </ul>
 *
 * <p>
 * Files and detailed format:
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 *   <li><tt>.doc</tt>: <a href="#Frequencies">Frequencies and Skip Data</a></li>
 *   <li><tt>.pos</tt>: <a href="#Positions">Positions</a></li>
 *   <li><tt>.pay</tt>: <a href="#Payloads">Payloads and Offsets</a></li>
 * </ul>
 *
 * <a name="Termdictionary"></a>
 * <dl>
 * <dd>
 * <b>Term Dictionary</b>
 *
 * <p>The .tim file contains the list of terms in each
 * field along with per-term statistics (such as docfreq)
 * and pointers to the frequencies, positions, payload and
 * skip data in the .doc, .pos, and .pay files.
 * See {@link BlockTreeTermsWriter} for more details on the format.
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations:
 * the postings writer/reader are actually responsible for encoding
 * and decoding the PostingsHeader and TermMetadata sections described here:
 *
 * <ul>
 *   <li>PostingsHeader --&gt; Header, PackedBlockSize</li>
 *   <li>TermMetadata --&gt; (DocFPDelta|SingletonDocID), PosFPDelta?, PosVIntBlockFPDelta?, PayFPDelta?,
 *                            SkipFPDelta?</li>
 *   <li>Header, --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>PackedBlockSize, SingletonDocID --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>DocFPDelta, PosFPDelta, PayFPDelta, PosVIntBlockFPDelta, SkipFPDelta --&gt; {@link DataOutput#writeVLong VLong}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:
 * <ul>
 *    <li>Header is a {@link CodecUtil#writeIndexHeader IndexHeader} storing the version information
 *        for the postings.</li>
 *    <li>PackedBlockSize is the fixed block size for packed blocks. In packed block, bit width is
 *        determined by the largest integer. Smaller block size result in smaller variance among width
 *        of integers hence smaller indexes. Larger block size result in more efficient bulk i/o hence
 *        better acceleration. This value should always be a multiple of 64, currently fixed as 128 as
 *        a tradeoff. It is also the skip interval used to accelerate {@link org.apache.lucene.index.PostingsEnum#advance(int)}.
 *    <li>DocFPDelta determines the position of this term's TermFreqs within the .doc file.
 *        In particular, it is the difference of file offset between this term's
 *        data and previous term's data (or zero, for the first term in the block).On disk it is
 *        stored as the difference from previous value in sequence. </li>
 *    <li>PosFPDelta determines the position of this term's TermPositions within the .pos file.
 *        While PayFPDelta determines the position of this term's &lt;TermPayloads, TermOffsets?&gt; within
 *        the .pay file. Similar to DocFPDelta, it is the difference between two file positions (or
 *        neglected, for fields that omit payloads and offsets).</li>
 *    <li>PosVIntBlockFPDelta determines the position of this term's last TermPosition in last pos packed
 *        block within the .pos file. It is synonym for PayVIntBlockFPDelta or OffsetVIntBlockFPDelta.
 *        This is actually used to indicate whether it is necessary to load following
 *        payloads and offsets from .pos instead of .pay. Every time a new block of positions are to be
 *        loaded, the PostingsReader will use this value to check whether current block is packed format
 *        or VInt. When packed format, payloads and offsets are fetched from .pay, otherwise from .pos.
 *        (this value is neglected when total number of positions i.e. totalTermFreq is less or equal
 *        to PackedBlockSize).
 *    <li>SkipFPDelta determines the position of this term's SkipData within the .doc
 *        file. In particular, it is the length of the TermFreq data.
 *        SkipDelta is only stored if DocFreq is not smaller than SkipMinimum
 *        (i.e. 128 in Lucene84PostingsFormat).</li>
 *    <li>SingletonDocID is an optimization when a term only appears in one document. In this case, instead
 *        of writing a file pointer to the .doc file (DocFPDelta), and then a VIntBlock at that location, the
 *        single document ID is written to the term dictionary.</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Termindex"></a>
 * <dl>
 * <dd>
 * <b>Term Index</b>
 * <p>The .tip file contains an index into the term dictionary, so that it can be
 * accessed randomly.  See {@link BlockTreeTermsWriter} for more details on the format.
 * </dd>
 * </dl>
 *
 *
 * <a name="Frequencies"></a>
 * <dl>
 * <dd>
 * <b>Frequencies and Skip Data</b>
 *
 * <p>The .doc file contains the lists of documents which contain each term, along
 * with the frequency of the term in that document (except when frequencies are
 * omitted: {@link IndexOptions#DOCS}). It also saves skip data to the beginning of
 * each packed or VInt block, when the length of document list is larger than packed block size.</p>
 *
 * <ul>
 *   <li>docFile(.doc) --&gt; Header, &lt;TermFreqs, SkipData?&gt;<sup>TermCount</sup>, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>TermFreqs --&gt; &lt;PackedBlock&gt; <sup>PackedDocBlockNum</sup>,
 *                        VIntBlock? </li>
 *   <li>PackedBlock --&gt; PackedDocDeltaBlock, PackedFreqBlock?
 *   <li>VIntBlock --&gt; &lt;DocDelta[, Freq?]&gt;<sup>DocFreq-PackedBlockSize*PackedDocBlockNum</sup>
 *   <li>SkipData --&gt; &lt;&lt;SkipLevelLength, SkipLevel&gt;
 *       <sup>NumSkipLevels-1</sup>, SkipLevel&gt;, SkipDatum?</li>
 *   <li>SkipLevel --&gt; &lt;SkipDatum&gt; <sup>TrimmedDocFreq/(PackedBlockSize^(Level + 1))</sup></li>
 *   <li>SkipDatum --&gt; DocSkip, DocFPSkip, &lt;PosFPSkip, PosBlockOffset, PayLength?,
 *                        PayFPSkip?&gt;?, SkipChildLevelPointer?</li>
 *   <li>PackedDocDeltaBlock, PackedFreqBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>DocDelta, Freq, DocSkip, DocFPSkip, PosFPSkip, PosBlockOffset, PayByteUpto, PayFPSkip
 *       --&gt;
 *   {@link DataOutput#writeVInt VInt}</li>
 *   <li>SkipChildLevelPointer --&gt; {@link DataOutput#writeVLong VLong}</li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:
 * <ul>
 *   <li>PackedDocDeltaBlock is theoretically generated from two steps:
 *     <ol>
 *       <li>Calculate the difference between each document number and previous one,
 *           and get a d-gaps list (for the first document, use absolute value); </li>
 *       <li>For those d-gaps from first one to PackedDocBlockNum*PackedBlockSize<sup>th</sup>,
 *           separately encode as packed blocks.</li>
 *     </ol>
 *     If frequencies are not omitted, PackedFreqBlock will be generated without d-gap step.
 *   </li>
 *   <li>VIntBlock stores remaining d-gaps (along with frequencies when possible) with a format
 *       that encodes DocDelta and Freq:
 *       <p>DocDelta: if frequencies are indexed, this determines both the document
 *       number and the frequency. In particular, DocDelta/2 is the difference between
 *       this document number and the previous document number (or zero when this is the
 *       first document in a TermFreqs). When DocDelta is odd, the frequency is one.
 *       When DocDelta is even, the frequency is read as another VInt. If frequencies
 *       are omitted, DocDelta contains the gap (not multiplied by 2) between document
 *       numbers and no frequency information is stored.</p>
 *       <p>For example, the TermFreqs for a term which occurs once in document seven
 *          and three times in document eleven, with frequencies indexed, would be the
 *          following sequence of VInts:</p>
 *       <p>15, 8, 3</p>
 *       <p>If frequencies were omitted ({@link IndexOptions#DOCS}) it would be this
 *          sequence of VInts instead:</p>
 *       <p>7,4</p>
 *   </li>
 *   <li>PackedDocBlockNum is the number of packed blocks for current term's docids or frequencies.
 *       In particular, PackedDocBlockNum = floor(DocFreq/PackedBlockSize) </li>
 *   <li>TrimmedDocFreq = DocFreq % PackedBlockSize == 0 ? DocFreq - 1 : DocFreq.
 *       We use this trick since the definition of skip entry is a little different from base interface.
 *       In {@link MultiLevelSkipListWriter}, skip data is assumed to be saved for
 *       skipInterval<sup>th</sup>, 2*skipInterval<sup>th</sup> ... posting in the list. However,
 *       in Lucene84PostingsFormat, the skip data is saved for skipInterval+1<sup>th</sup>,
 *       2*skipInterval+1<sup>th</sup> ... posting (skipInterval==PackedBlockSize in this case).
 *       When DocFreq is multiple of PackedBlockSize, MultiLevelSkipListWriter will expect one
 *       more skip data than Lucene84SkipWriter. </li>
 *   <li>SkipDatum is the metadata of one skip entry.
 *      For the first block (no matter packed or VInt), it is omitted.</li>
 *   <li>DocSkip records the document number of every PackedBlockSize<sup>th</sup> document number in
 *       the postings (i.e. last document number in each packed block). On disk it is stored as the
 *       difference from previous value in the sequence. </li>
 *   <li>DocFPSkip records the file offsets of each block (excluding )posting at
 *       PackedBlockSize+1<sup>th</sup>, 2*PackedBlockSize+1<sup>th</sup> ... , in DocFile.
 *       The file offsets are relative to the start of current term's TermFreqs.
 *       On disk it is also stored as the difference from previous SkipDatum in the sequence.</li>
 *   <li>Since positions and payloads are also block encoded, the skip should skip to related block first,
 *       then fetch the values according to in-block offset. PosFPSkip and PayFPSkip record the file
 *       offsets of related block in .pos and .pay, respectively. While PosBlockOffset indicates
 *       which value to fetch inside the related block (PayBlockOffset is unnecessary since it is always
 *       equal to PosBlockOffset). Same as DocFPSkip, the file offsets are relative to the start of
 *       current term's TermFreqs, and stored as a difference sequence.</li>
 *   <li>PayByteUpto indicates the start offset of the current payload. It is equivalent to
 *       the sum of the payload lengths in the current block up to PosBlockOffset</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Positions"></a>
 * <dl>
 * <dd>
 * <b>Positions</b>
 * <p>The .pos file contains the lists of positions that each term occurs at within documents. It also
 *    sometimes stores part of payloads and offsets for speedup.</p>
 * <ul>
 *   <li>PosFile(.pos) --&gt; Header, &lt;TermPositions&gt; <sup>TermCount</sup>, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>TermPositions --&gt; &lt;PackedPosDeltaBlock&gt; <sup>PackedPosBlockNum</sup>,
 *                            VIntBlock? </li>
 *   <li>VIntBlock --&gt; &lt;PositionDelta[, PayloadLength?], PayloadData?,
 *                        OffsetDelta?, OffsetLength?&gt;<sup>PosVIntCount</sup>
 *   <li>PackedPosDeltaBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>PositionDelta, OffsetDelta, OffsetLength --&gt;
 *       {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayloadData --&gt; {@link DataOutput#writeByte byte}<sup>PayLength</sup></li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:
 * <ul>
 *   <li>TermPositions are order by term (terms are implicit, from the term dictionary), and position
 *       values for each term document pair are incremental, and ordered by document number.</li>
 *   <li>PackedPosBlockNum is the number of packed blocks for current term's positions, payloads or offsets.
 *       In particular, PackedPosBlockNum = floor(totalTermFreq/PackedBlockSize) </li>
 *   <li>PosVIntCount is the number of positions encoded as VInt format. In particular,
 *       PosVIntCount = totalTermFreq - PackedPosBlockNum*PackedBlockSize</li>
 *   <li>The procedure how PackedPosDeltaBlock is generated is the same as PackedDocDeltaBlock
 *       in chapter <a href="#Frequencies">Frequencies and Skip Data</a>.</li>
 *   <li>PositionDelta is, if payloads are disabled for the term's field, the
 *       difference between the position of the current occurrence in the document and
 *       the previous occurrence (or zero, if this is the first occurrence in this
 *       document). If payloads are enabled for the term's field, then PositionDelta/2
 *       is the difference between the current and the previous position. If payloads
 *       are enabled and PositionDelta is odd, then PayloadLength is stored, indicating
 *       the length of the payload at the current term position.</li>
 *   <li>For example, the TermPositions for a term which occurs as the fourth term in
 *       one document, and as the fifth and ninth term in a subsequent document, would
 *       be the following sequence of VInts (payloads disabled):
 *       <p>4, 5, 4</p></li>
 *   <li>PayloadData is metadata associated with the current term position. If
 *       PayloadLength is stored at the current position, then it indicates the length
 *       of this payload. If PayloadLength is not stored, then this payload has the same
 *       length as the payload at the previous position.</li>
 *   <li>OffsetDelta/2 is the difference between this position's startOffset from the
 *       previous occurrence (or zero, if this is the first occurrence in this document).
 *       If OffsetDelta is odd, then the length (endOffset-startOffset) differs from the
 *       previous occurrence and an OffsetLength follows. Offset data is only written for
 *       {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.</li>
 * </ul>
 * </dd>
 * </dl>
 *
 * <a name="Payloads"></a>
 * <dl>
 * <dd>
 * <b>Payloads and Offsets</b>
 * <p>The .pay file will store payloads and offsets associated with certain term-document positions.
 *    Some payloads and offsets will be separated out into .pos file, for performance reasons.
 * <ul>
 *   <li>PayFile(.pay): --&gt; Header, &lt;TermPayloads, TermOffsets?&gt; <sup>TermCount</sup>, Footer</li>
 *   <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}</li>
 *   <li>TermPayloads --&gt; &lt;PackedPayLengthBlock, SumPayLength, PayData&gt; <sup>PackedPayBlockNum</sup>
 *   <li>TermOffsets --&gt; &lt;PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock&gt; <sup>PackedPayBlockNum</sup>
 *   <li>PackedPayLengthBlock, PackedOffsetStartDeltaBlock, PackedOffsetLengthBlock --&gt; {@link PackedInts PackedInts}</li>
 *   <li>SumPayLength --&gt; {@link DataOutput#writeVInt VInt}</li>
 *   <li>PayData --&gt; {@link DataOutput#writeByte byte}<sup>SumPayLength</sup></li>
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}</li>
 * </ul>
 * <p>Notes:
 * <ul>
 *   <li>The order of TermPayloads/TermOffsets will be the same as TermPositions, note that part of
 *       payload/offsets are stored in .pos.</li>
 *   <li>The procedure how PackedPayLengthBlock and PackedOffsetLengthBlock are generated is the
 *       same as PackedFreqBlock in chapter <a href="#Frequencies">Frequencies and Skip Data</a>.
 *       While PackedStartDeltaBlock follows a same procedure as PackedDocDeltaBlock.</li>
 *   <li>PackedPayBlockNum is always equal to PackedPosBlockNum, for the same term. It is also synonym
 *       for PackedOffsetBlockNum.</li>
 *   <li>SumPayLength is the total length of payloads written within one block, should be the sum
 *       of PayLengths in one packed block.</li>
 *   <li>PayLength in PackedPayLengthBlock is the length of each payload associated with the current
 *       position.</li>
 * </ul>
 * </dd>
 * </dl>
 * <br/>
 * lucene 5.0　的倒排表格式，将倒排表以<B>Packed Int blocks</B>的方式编码，为了更快的解码.
 * <br/>
 * 基本理念：
 * <br/>
 * <ul>
 *   <li>
 *     <b>Packed Blocks (打包的块) 和　Vint Blocks (变长int块）</b>
 *     <p>在Packed Blocks中，int被以相同的bit宽度进行编码，块的大小(比如块内的int数量)是固定的(当前是128).
 *     此外，全是相同值的块以另外一种优化的方法来编码。</p>
 *     <p> 在变长int块中，整数被编码为变长int, 块的大小是可变化的。</p>
 *   </li>
 *   <li>
 *     <b> 块的结构</b>
 *     <p> 当倒排表足够长，　这个类尽量用`Packed block`的方式编码更多的整数数据。</p>
 *     <p> 举个例子，一个词有259个文档，那么前面256个文档ID将被编码成两个`packed block`. 剩下的３个整数被编码成一个`Vint Block`.</p>
 *     <p> 不同类型的数据始终分别编码为不同的打包块，但可能会交错到同一VInt块中。</p>
 *     <p> 此策略适用于以下对：
 * <p>
 *      &lt;文档编号, 频率&gt;,
 *      &lt;位置, 有效负载长度&gt;,
 *      &lt;位置, 偏移起始位置, 偏移长度&gt;, 和
 *      &lt;位置, 有效负载长度, 偏移起始位置, 偏移长度&gt;.</p>
 *   </li>
 *   <li>
 *     <b> 跳表　设置</b>
 *     <p> 跳表的结构　和以前版本的lucene差不多。跳跃的间隔和块的大小一样，　每一个跳跃的节点是每一个块的开始。然而，第一个块，跳跃数据被省略了。</p>
 *   </li>
 *   <li>
 *     <b>位置，有效负载，　偏移量</b>
 *     <p> 位置：　代表着词在文档中的哪里命中了文档.
 *         有效负载，　是一块元数据，关联到当前的位置。
 *         偏移量：是一对整数，代表了给定词在当前位置的开始及结束偏移量。实际上是一个专用的有效负载。</p>
 *     <p> 当有效负载和偏移量没有被省略掉，　位置数量＝有效负载数量＝偏移量数量（假设有限负载为空，则计数为１）,
 *     就像在`块结构`中说的，这三个数据可以被分开或者合并起来编码。</p>
 *     <p> 在所有的情况中，有效负载和偏移量存储在一起。
 *     当编码为一个打包块时，位置数据使用.pos文件分开存储，而有效负载和偏移量存储在.pay.
 *     当使用变长int编码，这三个数据存储在一起，在.pos文件中(有效负载数据也在)</p>
 *     <p> 在这个策略下，大部分有效负载和偏移数据都在.pos文件外面。因此，对于只需要位置数据的查询，该查询在带有有效负载和偏移量的完整索引上运行，
 *     这会减少磁盘的预读取。</p>
 *   </li>
 * </ul>
 * <br/>
 * 文件和详细的格式
 * <ul>
 *   <li><tt>.tim</tt>: <a href="#Termdictionary">Term Dictionary</a></li>
 *   <li><tt>.tip</tt>: <a href="#Termindex">Term Index</a></li>
 *   <li><tt>.doc</tt>: <a href="#Frequencies">Frequencies and Skip Data</a></li>
 *   <li><tt>.pos</tt>: <a href="#Positions">Positions</a></li>
 *   <li><tt>.pay</tt>: <a href="#Payloads">Payloads and Offsets</a></li>
 * </ul>
 * <p>
 * <br/>
 * <b> term Dictionary (term的词典？词的词典？)</b>
 * <p> .tim文件包含每个域中的所有词的列表，　以及每一个词的统计信息(比如文档频率). 以及指向与频率，位置，有效负载和跳表的数据在.doc,.pos,.
 * pay等文件。格式的更加详细的信息在｀BlockTreeTermsWriter`</p>
 * <p> 提示：term词典可以有多种实现，倒排表的读写者实际上负责去编码解码`PostingHeader`和`TermMetaData`部分。</p>
 * <ul>
 *   <li>PositionsHeader --> 文件头，打包块的大小</li>
 *   <li>TermMetaDara --> (DocFPDelta|SingletonDocID), PosFPDelta?, PosVIntBlockFPDelta?, PayFPDelta?, SkipFPDelta?, 一堆偏移量之类的东西，看不懂</li>
 *   <li> Header --> 索引文件头</li>
 *   <li> PackedBlockSize, SingleDocId --> 变长int</li>
 *   <li> DocFPDelta, PosFPDelta, PayFPDelta, PosVintBlockFpDelta, SkipFPDelta --> 变长的long</li>
 *   <li> Footer: 编码器的脚部【</li>
 * </ul>
 * <b> 提示:</b>
 * <ul>
 *   <li><b>Header:</b> 存储了倒排表的版本信息之类的</li>
 *   <li><b>PackedBlockSize</b> 打包块的固定大小. 在打包块中,bit宽度由最大的整数决定.
 *   较小的块大小, 导致整数之间宽度差异较小, 因此索引较小. 较大的块大小, 可以有更加高效的批量io,因此速度更好.</li>
 *   <li> <b>DocFPDelta</b> 确定了这个词在.doc文件中的词频的位置.
 *   特别是，这一项的数据与前一项的数据之间的文件偏移量之差（对于块中的第一项为零），在磁盘上按顺序存储为与前一项值的差。</li>
 *   <li><b> PosFPDelta</b> 确定了这个词在.pos文件中的词位置的位置.
 *   PayFPDelta则确定了这个词的词负载及词偏移在.pay文件中的位置.
 *   就像DocFPDelta一样,他是两个文件位置之间的差值.(或者对于会略payloads和offset的域,这个值会被忽略掉).
 *   </li>
 *   <li><b>POsVIntBlockFPDelta</b>
 *   <P>定义了pos文件中, 这个词在最后一个打包块中的最后一个词位置.
 *    PayVIntBlockFPDelta or OffsetVIntBlockFPDelta 是一样的含义.
 *    者经常用于表名需要从.pos文件加载基础负载和偏移量,而不是.pay文件.
 *    每次加载一个位置的新块时, 倒排表读取器会使用这个值检查当前块是打包编码还是变长整数编码.
 *    如果是打包编码, 基础负载和偏移量将从.pay文件加载,否则从.pos文件加载.
 *    这个值将被忽略, 如果位置的总数少于或者等于打包块的大小.
 *    </P></li>
 *    <li><b>SkipFPDelta</b>< 确定了这个词的跳跃数据在.doc文件中的. 特殊的是, 他是词频数据的长度.
 *    `SkipDelta`只有在词频数据不比最小跳跃数据小的时候存储.(在当前类中,这个数据是128)/li>
 *    <li><SingletonDocID: 是当一个词只在一个doc中出现的时候的一个优化, 在这种情况下,
 *    就别打文件指针了, 直接把这个id写成一个变长的int块就好了.</li>
 * </ul>
 * <b> term index</b>
 * .tip文件包含了term Dictionary的索引，因此它可以被随机读取。更多细节看`BlockTreeTermsWriter`.
 *
 * <b>词频和跳跃数据</b>
 * .doc文件包含了每一个词包含的文档列表，　还有这个词在该文档中的频率(可以有选项进行忽略掉词频). 该文件还存储了跳跃数据，跳向每一个打包块或者变长int块的开始。
 * 当然前提是，文档列表长度大于打包块的大小.
 *
 * <ul>
 *   <li><b>docFile</b> Header, &lt;TermFreq, SkipData&gt; ^ termCount, Footer</li>
 *   <li>header: 索引文件头</li>
 *   <li>TermFreqs: packedBlock * blockCount, VintBlock</li>
 *   <li> PackedBlock: PackedDocDeltaBlock, PackedFreqBlock</li>
 *   <li> VintBlock: DocDelta,Freq. DocFreqSize*FreqNum</li>
 *   <li> SkipData: skipLevelLength:</li>
 * </ul>
 * 这堆东西不赘述了，因为没啥意思，　他写了每个名词包含的一堆名词，我知道有这些东西，看上面原来的注释就好了，每一个具体的含义，还是要自己去发现，及下面的注释。
 * <br/>
 * <B>notes:</B>
 * <ul>
 *   <li>
 *     <p> PackedDocDeltaBlock 理论上分成两步生成: </p>
 *     <p> 1. 计算每一个docId和前一个的差值，搞一个差值的列表。对于第一个文档，使用绝对值</p>
 *     <p> 2. 对于从第一个到PackedDocBlockNum * PackedBlockSizeth的那些差值列表，分别编码为打包块。</p>
 *     如果词频没有被忽略，那么也是这样子编码的。
 *   </li>
 *   <li>
 *     <P> 变长int块存储了剩下的差值列表，使用DOcDelta, 和Freq:</P>
 *     <p> DocDelta: 如果频率已经被索引,就是打开了对应的选项，　这个代表了全部的文档编号和频率，　特殊的是，　DocDelta/2　是这个文档和前一个的编号之间的差值。或者如果这个文档是TermFreqs的第一个文档，那么这个值为0</p>
 *     <p> 如果DocDelta是奇数，频率是１，　如果DocDelta是偶数，频率是另外读取的一个变长int. 如果频率被忽略了，那么DocDelta存储了间隙值（两个文档编号的差值），没有频率信息。</p>
 *     <p> 举个例子：TermFreqs ，如果一个词命中了文档７，　在文档７中是３次。如果频率是要索引的 那么他对应的TermFreqs是变长int的序列: 15,8,3. 如果频率被忽略，　那么将是变长int的序列: 7,4。</p>
 *   </li>
 *   <li> PackedDocBlockNum 是打包块的总数，　对于当前词的文档id或者词频. 特殊的是，　PackedDocBlockNum = floor(DocFreq/PackedBlockSize)</li>
 *   <li> TrimmedDocFreq = DocFreq % PackedBlockSize == 0 ? DocFreq-1:DocFreq. 我们用这个，因此跳跃条目的定义与基本接口有点不同。
 *    花里胡哨的写了一堆跳跃表级别的算法，　我也看不懂，　是具体的跳跃表实现，到时候再看把。要吐了</li>
 *    <li>
 *      <p> SkipDatum 是跳跃entry的元数据，对于第一个block, 他被省略了。</p>
 *    </li>
 *    <li>
 *      <b>DocSKip: 记录了倒排表中，每个，第PackedBlockSize个文档的编号（每个打包块中的最后一个文档编号). 在硬盘上存储时，　他被存储为序列中每一个值与前一个值的增量。</b>
 *    </li>
 *    <li> DocFpSkip: 记录了每一个块的文件偏移位置。当然不是每一个，　是第n*PackedBlockSize+1个。　文件偏移位置关联到当前词的TermFreq。　在磁盘上，他也是存储为序列中前一个的增量。</li>
 *    <li> 如果位置和基础负责也被编码，跳跃将首先跳跃到关联的block。然后获取关联的block的偏移位置，　PosFPSkip 和 PayFPSKip 记录了关联块的地址，在.pos和.pay文件中的位置</li>
 *    <p> 当PosBlockOffset表示应该获取关连快中的那个值。和ＤｏｃＦＰＳＫｉｐ一样，文件偏移量关联到当前词的TermFreqs.也被存储为增量</p>
 *    <li> PayByteUpto 表示当前基础负载的起始位置，　他相当于当前块中的有效负载长度的总和，知道POsBlockOffset.</li>
 *
 * </ul>
 *
 * <b>Positions</b>
 * .pos文件包含每个词在文档中出现的位置的列表，有时还存储部分有效负载用来加快速度。
 * <br/>
 * 列出来的每一个里面包含了啥，　不看了，看代码去，记得这里有就好了。
 * <b>Notes:</b>
 * <ul>
 *   <li>
 *     TermPositions 是按照词排序的(词是隐含的，关联到TermDictionary)，每个词的位置列表是递增的，　并且按照文档编号排序
 *   </li>
 *   <li>
 *     PackedPosBlockNum 是当前词的位置，有效负载，偏移量的打包快的数量。　`PackedPosBlockNum=footer(totalTermFreq/PackedBlockSize).
 *   </li>
 *   <li>
 *     PosVintCount 是编码为变长int块的位位置数量。`PosVintCount=totalTermFreq - PackedPosBlockNum * PackedBlockSize)
 *   </li>
 *   <li>
 *     PackedPosDeltaBlock的生成过程与“频率和跳过数据”一章中的PackedDocDeltaBlock相同。
 *   </li>
 *   <li>
 *     PositionDelta 是：　如何有效负载被关闭了，那么就是文档中出现位置与上一个位置的差值，　如果启用了有效负载，那么`PositionDelta/2是当前位置与上一个位置的差.
 *     如果启用了有效负载且PositionDelta为奇数，则存储PayLoadLength.来只是当前词位置的有效负载长度.
 *   </li>
 *   <li>
 *     举个例子：如果一个词，在当前文档中是第四个词，在随后的文档中是地五个，第九个词，那么将存储一下变长int序列：454。(禁用有效负载的情况下)
 *   </li>
 *   <li>
 *     PayLloadData是与当前词的位置关联的元数据，　如果当前位置的payloadLength要存储，则表明了这个有效负载的长度.　如果没有存储，则表明这个有效负载的长度与前一个的一样。
 *   </li>
 *   <li>
 *     OffsetDelta/2 是该位置与上一个位置的差，　如果OffsetDelta为计数，则长度与上一次出现的值不同，并且紧随其后的是OffsetLength.
 *     偏移数据只有当IndexOptions是全部的时候存储
 *   </li>
 * </ul>
 *
 * <b>有效负载和偏移</b>
 * <p> .pay　文件将存储有效负责和偏移量，关联到词在文档中的位置。　一些payloads和offset将被存储在.pos文件中，为了提升性能</p>
 * 又是一堆文件的具体格式，不细看了，等看到代码的时候回来一一对应。
 * <b> Notes:</b>
 * <ul>
 *   <li> TermPayloads/TermOffsets　的排序和TermPositions一样，　注意部分的有效负载和偏移量存储在pos文件中.</li>
 *   <li> PackedPayLengthBlock和PackedOffsetLengthBlock的生成过程与“频率和跳过数据”一章中的PackedFreqBlock相同。而PackedStartDeltaBlock遵循与PackedDocDeltaBlock相同的过程。</li>
 *   <li> PackedPayBlockNum is always equal to PackedPosBlockNum, for the same term. It is also synonym for PackedOffsetBlockNum. </li>
 *   <li>SumPayLength是在一个块内写入的有效负载的总长度，应为一个打包块中PayLengths的总和。</li>
 * <li> PackedPayLengthBlock中的PayLength是与当前位置关联的每个有效负载的长度。 </li>
 * </ul>
 *
 * <b> 这个类的注释兼简直离谱，我他妈就没见过这么多的注释，写作文呢搁这。</b>
 * <b> 大概讲了倒排表中都有哪些数据，存储在哪些文件，对应的文件格式都有哪些数据，并简单介绍了下对应的名词。同时，还给了一些注意事项。</b>
 */
public final class Lucene84PostingsFormat extends PostingsFormat {

  /**
   * Filename extension for document number, frequencies, and skip data.
   * See chapter: <a href="#Frequencies">Frequencies and Skip Data</a>
   */
  public static final String DOC_EXTENSION = "doc";

  /**
   * Filename extension for positions.
   * See chapter: <a href="#Positions">Positions</a>
   */
  public static final String POS_EXTENSION = "pos";

  /**
   * Filename extension for payloads and offsets.
   * See chapter: <a href="#Payloads">Payloads and Offsets</a>
   */
  public static final String PAY_EXTENSION = "pay";

  /**
   * Size of blocks.
   * block内部的int数量. 这个值很重要，取膜啊什么的
   */
  public static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

  /**
   * Expert: The maximum number of skip levels. Smaller values result in
   * slightly smaller indexes, but slower skipping in big posting lists.
   *
   * 跳跃表的最大级别。　越小的值，建的索引越小，但是在大点的倒排表里会更慢一些。
   */
  static final int MAX_SKIP_LEVELS = 10;

  // 倒排表相关的４种文件的编码名字
  final static String TERMS_CODEC = "Lucene84PostingsWriterTerms";
  final static String DOC_CODEC = "Lucene84PostingsWriterDoc";
  final static String POS_CODEC = "Lucene84PostingsWriterPos";
  final static String PAY_CODEC = "Lucene84PostingsWriterPay";

  // Increment version to change it
  // 一个版本号咯
  final static int VERSION_START = 0;
  // Better compression of the terms dictionary in case most terms have a docFreq of 1
  // 当前用的版本号
  final static int VERSION_COMPRESSED_TERMS_DICT_IDS = 1;
  final static int VERSION_CURRENT = VERSION_COMPRESSED_TERMS_DICT_IDS;

  // 最小和最大的blockSize??
  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /**
   * Creates {@code Lucene84PostingsFormat} with default
   * settings.
   */
  public Lucene84PostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /**
   * Creates {@code Lucene84PostingsFormat} with custom
   * values for {@code minBlockSize} and {@code
   * maxBlockSize} passed to block terms dictionary.
   *
   * @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState, PostingsWriterBase, int, int)
   */
  public Lucene84PostingsFormat(int minTermBlockSize, int maxTermBlockSize) {
    super("Lucene84");
    BlockTreeTermsWriter.validateSettings(minTermBlockSize, maxTermBlockSize);
    this.minTermBlockSize = minTermBlockSize;
    this.maxTermBlockSize = maxTermBlockSize;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new Lucene84PostingsWriter(state);
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state,
          postingsWriter,
          minTermBlockSize,
          maxTermBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsWriter);
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new Lucene84PostingsReader(state);
    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(postingsReader, state);
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(postingsReader);
      }
    }
  }

  /**
   * Holds all state required for {@link Lucene84PostingsReader} to produce a
   * {@link org.apache.lucene.index.PostingsEnum} without re-seeking the terms dict.
   *
   * @lucene.internal
   */
  public static final class IntBlockTermState extends BlockTermState {
    /**
     * file pointer to the start of the doc ids enumeration, in {@link #DOC_EXTENSION} file
     */
    public long docStartFP;
    /**
     * file pointer to the start of the positions enumeration, in {@link #POS_EXTENSION} file
     */
    public long posStartFP;
    /**
     * file pointer to the start of the payloads enumeration, in {@link #PAY_EXTENSION} file
     */
    public long payStartFP;
    /**
     * file offset for the start of the skip list, relative to docStartFP, if there are more
     * than {@link ForUtil#BLOCK_SIZE} docs; otherwise -1
     */
    public long skipOffset;
    /**
     * file offset for the last position in the last block, if there are more than
     * {@link ForUtil#BLOCK_SIZE} positions; otherwise -1
     */
    public long lastPosBlockOffset;
    /**
     * docid when there is a single pulsed posting, otherwise -1.
     * freq is always implicitly totalTermFreq in this case.
     */
    public int singletonDocID;

    /**
     * Sole constructor.
     */
    public IntBlockTermState() {
      skipOffset = -1;
      lastPosBlockOffset = -1;
      singletonDocID = -1;
    }

    @Override
    public IntBlockTermState clone() {
      IntBlockTermState other = new IntBlockTermState();
      other.copyFrom(this);
      return other;
    }

    @Override
    public void copyFrom(TermState _other) {
      super.copyFrom(_other);
      IntBlockTermState other = (IntBlockTermState) _other;
      docStartFP = other.docStartFP;
      posStartFP = other.posStartFP;
      payStartFP = other.payStartFP;
      lastPosBlockOffset = other.lastPosBlockOffset;
      skipOffset = other.skipOffset;
      singletonDocID = other.singletonDocID;
    }

    @Override
    public String toString() {
      return super.toString() + " docStartFP=" + docStartFP + " posStartFP=" + posStartFP + " payStartFP=" + payStartFP + " lastPosBlockOffset=" + lastPosBlockOffset + " singletonDocID=" + singletonDocID;
    }
  }
}
