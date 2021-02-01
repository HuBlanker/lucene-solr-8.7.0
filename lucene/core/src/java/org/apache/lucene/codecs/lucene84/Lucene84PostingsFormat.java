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
 *
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
 *
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
 *
 *
 * @lucene.experimental
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

  /** Size of blocks. */
  public static final int BLOCK_SIZE = ForUtil.BLOCK_SIZE;

  /**
   * Expert: The maximum number of skip levels. Smaller values result in 
   * slightly smaller indexes, but slower skipping in big posting lists.
   */
  static final int MAX_SKIP_LEVELS = 10;

  final static String TERMS_CODEC = "Lucene84PostingsWriterTerms";
  final static String DOC_CODEC = "Lucene84PostingsWriterDoc";
  final static String POS_CODEC = "Lucene84PostingsWriterPos";
  final static String PAY_CODEC = "Lucene84PostingsWriterPay";

  // Increment version to change it
  final static int VERSION_START = 0;
  // Better compression of the terms dictionary in case most terms have a docFreq of 1
  final static int VERSION_COMPRESSED_TERMS_DICT_IDS = 1;
  final static int VERSION_CURRENT = VERSION_COMPRESSED_TERMS_DICT_IDS;

  private final int minTermBlockSize;
  private final int maxTermBlockSize;

  /** Creates {@code Lucene84PostingsFormat} with default
   *  settings. */
  public Lucene84PostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE);
  }

  /** Creates {@code Lucene84PostingsFormat} with custom
   *  values for {@code minBlockSize} and {@code
   *  maxBlockSize} passed to block terms dictionary.
   *  @see BlockTreeTermsWriter#BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int) */
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
    /** file pointer to the start of the doc ids enumeration, in {@link #DOC_EXTENSION} file */
    public long docStartFP;
    /** file pointer to the start of the positions enumeration, in {@link #POS_EXTENSION} file */
    public long posStartFP;
    /** file pointer to the start of the payloads enumeration, in {@link #PAY_EXTENSION} file */
    public long payStartFP;
    /** file offset for the start of the skip list, relative to docStartFP, if there are more
     * than {@link ForUtil#BLOCK_SIZE} docs; otherwise -1 */
    public long skipOffset;
    /** file offset for the last position in the last block, if there are more than
     * {@link ForUtil#BLOCK_SIZE} positions; otherwise -1 */
    public long lastPosBlockOffset;
    /** docid when there is a single pulsed posting, otherwise -1.
     * freq is always implicitly totalTermFreq in this case. */
    public int singletonDocID;

    /** Sole constructor. */
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
