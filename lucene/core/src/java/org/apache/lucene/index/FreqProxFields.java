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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Implements limited (iterators only, no stats) {@link Fields} interface over the in-RAM buffered
 * fields/terms/postings, to flush postings through the PostingsFormat.
 */
class FreqProxFields extends Fields {
  final Map<String, FreqProxTermsWriterPerField> fields = new LinkedHashMap<>();

  public FreqProxFields(List<FreqProxTermsWriterPerField> fieldList) {
    // NOTE: fields are already sorted by field name
    for (FreqProxTermsWriterPerField field : fieldList) {
      fields.put(field.getFieldName(), field);
    }
  }

  @Override
  public Iterator<String> iterator() {
    return fields.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    FreqProxTermsWriterPerField perField = fields.get(field);
    return perField == null ? null : new FreqProxTerms(perField);
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  private static class FreqProxTerms extends Terms {
    // 包含了我们在内存中倒排的所有信息
    final FreqProxTermsWriterPerField terms;

    public FreqProxTerms(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
    }

    @Override
    public TermsEnum iterator() {
      FreqProxTermsEnum termsEnum = new FreqProxTermsEnum(terms);

      //重置游标
      termsEnum.reset();

      return termsEnum;
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFreqs() {
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      // NOTE: the in-memory buffer may have indexed offsets
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
          >= 0;
    }

    @Override
    public boolean hasPositions() {
      // NOTE: the in-memory buffer may have indexed positions
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return terms.sawPayloads;
    }
  }

  private static class FreqProxTermsEnum extends BaseTermsEnum {
    final FreqProxTermsWriterPerField terms;

    // 按term大小排序的termID
    final int[] sortedTermIDs;

    // 上一篇文章中遗留的问题： 最后处理的term的文档的id和频率没有写入bytePool，它存储在postingsArray中
    final FreqProxPostingsArray postingsArray;

    // 当前处理的term
    final BytesRef scratch = new BytesRef();

    final int numTerms;

    // term从小到大的排序序号，也就是field中的第几个term
    int ord;

    FreqProxTermsEnum(FreqProxTermsWriterPerField terms) {
      this.terms = terms;

      this.numTerms = terms.getNumTerms();

      sortedTermIDs = terms.getSortedTermIDs();

      assert sortedTermIDs != null;

      postingsArray = (FreqProxPostingsArray) terms.postingsArray;
    }

    public void reset() {
      ord = -1;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) {
      // TODO: we could instead keep the BytesRefHash
      // intact so this is a hash lookup

      // sortedTermIDs已经是有序的了，所以可以通过二分查找指定的term
      // binary search:
      int lo = 0;
      int hi = numTerms - 1;
      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;
        int textStart = postingsArray.textStarts[sortedTermIDs[mid]];
        terms.bytePool.setBytesRef(scratch, textStart);
        int cmp = scratch.compareTo(text);
        if (cmp < 0) {
          lo = mid + 1;
        } else if (cmp > 0) {
          hi = mid - 1;
        } else {
          // found:
          ord = mid;
          assert term().compareTo(text) == 0;
          return SeekStatus.FOUND;
        }
      }

      // not found:
      ord = lo;
      if (ord >= numTerms) {
        return SeekStatus.END;
      } else {
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        assert term().compareTo(text) > 0;
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public void seekExact(long ord) {
      this.ord = (int) ord;
      int textStart = postingsArray.textStarts[sortedTermIDs[this.ord]];
      terms.bytePool.setBytesRef(scratch, textStart);
    }

    @Override
    public BytesRef next() {
      ord++;

      if (ord >= numTerms) {
        return null;
      } else {
        //获取term对应在bytePool的起始位置
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];

        //复制文本
        terms.bytePool.setBytesRef(scratch, textStart);

        return scratch;
      }
    }

    @Override
    public BytesRef term() {
      return scratch;
    }

    @Override
    public long ord() {
      return ord;
    }

    @Override
    public int docFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    // 获取倒排信息的迭代器。
    // 如果需要需要stream0中的倒排信息则使用FreqProxDocsEnum，否则使用FreqProxPostingsEnum。
    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        FreqProxPostingsEnum posEnum;

        if (!terms.hasProx) {
          // 没有构建位置信息抛出异常
          // Caller wants positions but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index positions");
        }

        if (!terms.hasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
          // Caller wants offsets but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index offsets");
        }

        if (reuse instanceof FreqProxPostingsEnum) {
          posEnum = (FreqProxPostingsEnum) reuse;

          if (posEnum.postingsArray != postingsArray) {
            posEnum = new FreqProxPostingsEnum(terms, postingsArray);
          }
        } else {
          posEnum = new FreqProxPostingsEnum(terms, postingsArray);
        }

        posEnum.reset(sortedTermIDs[ord]);

        return posEnum;
      }

      FreqProxDocsEnum docsEnum;

      if (!terms.hasFreq && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        // Caller wants freqs but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index freq");
      }

      if (reuse instanceof FreqProxDocsEnum) {
        docsEnum = (FreqProxDocsEnum) reuse;

        if (docsEnum.postingsArray != postingsArray) {
          docsEnum = new FreqProxDocsEnum(terms, postingsArray);
        }
      } else {
        docsEnum = new FreqProxDocsEnum(terms, postingsArray);
      }

      docsEnum.reset(sortedTermIDs[ord]);

      return docsEnum;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Expert: Returns the TermsEnums internal state to position the TermsEnum without re-seeking
     * the term dictionary.
     *
     * <p>NOTE: A seek by {@link TermState} might not capture the {@link AttributeSource}'s state.
     * Callers must maintain the {@link AttributeSource} states separately
     *
     * @see TermState
     * @see #seekExact(BytesRef, TermState)
     */
    @Override
    public TermState termState() throws IOException {
      return new TermState() {
        @Override
        public void copyFrom(TermState other) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private static class FreqProxDocsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;

    final FreqProxPostingsArray postingsArray;

    final ByteSliceReader reader = new ByteSliceReader();

    final boolean readTermFreq;

    int docID = -1;

    int freq;

    // 用来标记是否处理结束
    boolean ended;

    int termID;

    public FreqProxDocsEnum(
        FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readTermFreq = terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;

      //初始化reader
      terms.initReader(reader, termID, 0);

      ended = false;

      docID = -1;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      // Don't lie here ... don't want codecs writings lots
      // of wasted 1s into the index:
      if (!readTermFreq) {
        throw new IllegalStateException("freq was not indexed");
      } else {
        return freq;
      }
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int nextDoc() throws IOException {
      // 因为docID是差值存储，并且第一个docID是和0做差值，所以docID初始化为0
      if (docID == -1) {
        docID = 0;
      }

      // stream 0是否结束了
      if (reader.eof()) {
        // 如果已经全部处理结束，则返回  NO_MORE_DOCS
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          // 标记全部处理结束
          ended = true;

          // 我们在构建中的遗留问题答案：
          // term出现的最后一个docID和freq没有写入bytePool的stream0中，
          // 所以在bytePool的stream0读取结束之后，
          // 我们需要从postingsArray.lastDocIDs和postingsArray.termFreqs获取term出现的最后一个docID和对应的freq
          docID = postingsArray.lastDocIDs[termID];
          if (readTermFreq) {
            freq = postingsArray.termFreqs[termID];
          }
        }
      } else {
        int code = reader.readVInt();

        // 如果没有存频率，则读取的就是docID的差值
        if (!readTermFreq) {
          docID += code;
        } else { // 如果存在频率，则读取的是docID差值的左移一位，因为还原需要右移一位
          docID += code >>> 1;
          if ((code & 1) != 0) { // code的最后一位是1，表示频率就是1
            freq = 1;
          } else {// code的最后一位是0，表示频率需要读取下一个数据
            freq = reader.readVInt();
          }
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FreqProxPostingsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;

    final FreqProxPostingsArray postingsArray;

    final ByteSliceReader reader = new ByteSliceReader();

    final ByteSliceReader posReader = new ByteSliceReader();

    final boolean readOffsets;
    int docID = -1;
    int freq;
    int pos;
    int startOffset;
    int endOffset;
    int posLeft;
    int termID;
    boolean ended;
    boolean hasPayload;
    BytesRefBuilder payload = new BytesRefBuilder();

    public FreqProxPostingsEnum(
        FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readOffsets = terms.hasOffsets;
      assert terms.hasProx;
      assert terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.initReader(reader, termID, 0);
      terms.initReader(posReader, termID, 1);
      ended = false;
      docID = -1;
      posLeft = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    // 同FreqProxDocsEnum#nextDoc类似
    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;
      }

      // 如果当前docID的stream1的信息还没处理完，则通过  nextPosition 都忽略掉
      while (posLeft != 0) {
        nextPosition();
      }

      //读取到结束，没有可读字符
      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true;
          //最后一个
          docID = postingsArray.lastDocIDs[termID];
          freq = postingsArray.termFreqs[termID];
        }
      } else {
        int code = reader.readVInt();
        docID += code >>> 1;
        if ((code & 1) != 0) {
          freq = 1;
        } else {
          freq = reader.readVInt();
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      posLeft = freq;
      pos = 0;
      startOffset = 0;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      assert posLeft > 0;
      // 剩余的位置个数减一
      posLeft--;
      int code = posReader.readVInt();
      pos += code >>> 1;
      if ((code & 1) != 0) {// 如果存在payload数据则解析payload数据
        hasPayload = true;
        // has a payload
        payload.setLength(posReader.readVInt());
        payload.grow(payload.length());
        posReader.readBytes(payload.bytes(), 0, payload.length());
      } else {
        hasPayload = false;
      }

      // 获取startOffset和endOffset
      if (readOffsets) {
        startOffset += posReader.readVInt();
        endOffset = startOffset + posReader.readVInt();
      }

      return pos;
    }

    @Override
    public int startOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return startOffset;
    }

    @Override
    public int endOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (hasPayload) {
        return payload.get();
      } else {
        return null;
      }
    }
  }
}
