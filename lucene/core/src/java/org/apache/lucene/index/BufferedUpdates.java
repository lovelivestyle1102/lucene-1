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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates, by docID, term or query for a single segment. This is used to
 * hold buffered pending deletes and updates against the to-be-flushed segment. Once the deletes and
 * updates are pushed (on flush in DocumentsWriter), they are converted to a {@link
 * FrozenBufferedUpdates} instance and pushed to the {@link BufferedUpdatesStream}.
 */

// NOTE: instances of this class are accessed either via a private
// instance on DocumentWriterPerThread, or via sync'd code by
// DocumentsWriterDeleteQueue

class BufferedUpdates implements Accountable {

  /* Rough logic: HashMap has an array[Entry] w/ varying
  load factor (say 2 * POINTER).  Entry is object w/ Term
  key, Integer val, int hash, Entry next
  (OBJ_HEADER + 3*POINTER + INT).  Term is object w/
  String field and String text (OBJ_HEADER + 2*POINTER).
  Term's field is String (OBJ_HEADER + 4*INT + POINTER +
  OBJ_HEADER + string.length*CHAR).
  Term's text is String (OBJ_HEADER + 4*INT + POINTER +
  OBJ_HEADER + string.length*CHAR).  Integer is
  OBJ_HEADER + INT. */
  static final int BYTES_PER_DEL_TERM =
      9 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 7 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 10 * Integer.BYTES;

  /* Rough logic: HashMap has an array[Entry] w/ varying
  load factor (say 2 * POINTER).  Entry is object w/
  Query key, Integer val, int hash, Entry next
  (OBJ_HEADER + 3*POINTER + INT).  Query we often
  undercount (say 24 bytes).  Integer is OBJ_HEADER + INT. */
  static final int BYTES_PER_DEL_QUERY =
      5 * RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
          + 2 * Integer.BYTES
          + 24;

  // 根据term匹配条件删除的个数
  final AtomicInteger numTermDeletes = new AtomicInteger();

  // docValue更新的个数
  final AtomicInteger numFieldUpdates = new AtomicInteger();

  //该Map的key为Term，表示包含该Term的文档都会被删除，value为一个哨兵值，描述了该删除操作的作用范围，即只能作用于文档号小于哨兵值的文档
  final Map<Term, Integer> deleteTerms =
      new HashMap<>(); // TODO cut this over to FieldUpdatesBuffer

  //该Map的key为Query，表示满足该查询要求的文档都会被删除，value的概念同deleteTerms
  final Map<Query, Integer> deleteQueries = new HashMap<>();

  // key是字段名称，value是对应字段的docValue的更新信息
  final Map<String, FieldUpdatesBuffer> fieldUpdates = new HashMap<>();

  public static final Integer MAX_INT = Integer.valueOf(Integer.MAX_VALUE);

  private final Counter bytesUsed = Counter.newCounter(true);

  final Counter fieldUpdatesBytesUsed = Counter.newCounter(true);

  private final Counter termsBytesUsed = Counter.newCounter(true);

  private static final boolean VERBOSE_DELETES = false;

  long gen;

  final String segmentName;

  public BufferedUpdates(String segmentName) {
    this.segmentName = segmentName;
  }

  @Override
  public String toString() {
    if (VERBOSE_DELETES) {
      return ("gen=" + gen)
          + (" numTerms=" + numTermDeletes)
          + (", deleteTerms=" + deleteTerms)
          + (", deleteQueries=" + deleteQueries)
          + (", fieldUpdates=" + fieldUpdates)
          + (", bytesUsed=" + bytesUsed);
    } else {
      String s = "gen=" + gen;
      if (numTermDeletes.get() != 0) {
        s +=
            " " + numTermDeletes.get() + " deleted terms (unique count=" + deleteTerms.size() + ")";
      }
      if (deleteQueries.size() != 0) {
        s += " " + deleteQueries.size() + " deleted queries";
      }
      if (numFieldUpdates.get() != 0) {
        s += " " + numFieldUpdates.get() + " field updates";
      }
      if (bytesUsed.get() != 0) {
        s += " bytesUsed=" + bytesUsed.get();
      }

      return s;
    }
  }

  // 根据query删除
  public void addQuery(Query query, int docIDUpto) {
    Integer current = deleteQueries.put(query, docIDUpto);
    // increment bytes used only if the query wasn't added so far.
    // 如果是新增的，则需要更新占用的内存大小
    if (current == null) {
      bytesUsed.addAndGet(BYTES_PER_DEL_QUERY);
    }
  }

  // 根据term删除
  public void addTerm(Term term, int docIDUpto) {
    Integer current = deleteTerms.get(term);
    // 如果已经有了按term删除的，并且有效的docID上限更大，则忽略当前的删除动作
    if (current != null && docIDUpto < current) {
      // Only record the new number if it's greater than the
      // current one.  This is important because if multiple
      // threads are replacing the same doc at nearly the
      // same time, it's possible that one thread that got a
      // higher docID is scheduled before the other
      // threads.  If we blindly replace than we can
      // incorrectly get both docs indexed.
      return;
    }

    deleteTerms.put(term, Integer.valueOf(docIDUpto));

    // note that if current != null then it means there's already a buffered
    // delete on that term, therefore we seem to over-count. this over-counting
    // is done to respect IndexWriterConfig.setMaxBufferedDeleteTerms.
    numTermDeletes.incrementAndGet();

    // 新增的需要更新内存占用信息
    if (current == null) {
      termsBytesUsed.addAndGet(
          BYTES_PER_DEL_TERM + term.bytes.length + (Character.BYTES * term.field().length()));
    }
  }

  // NumericDocValues的更新
  void addNumericUpdate(NumericDocValuesUpdate update, int docIDUpto) {
    // 获取字段的  FieldUpdatesBuffer ，如果是这个字段第一次更新docValues，则创建一个
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void addBinaryUpdate(BinaryDocValuesUpdate update, int docIDUpto) {
    FieldUpdatesBuffer buffer =
        fieldUpdates.computeIfAbsent(
            update.field, k -> new FieldUpdatesBuffer(fieldUpdatesBytesUsed, update, docIDUpto));
    if (update.hasValue) {
      buffer.addUpdate(update.term, update.getValue(), docIDUpto);
    } else {
      buffer.addNoValue(update.term, docIDUpto);
    }
    numFieldUpdates.incrementAndGet();
  }

  void clearDeleteTerms() {
    numTermDeletes.set(0);
    termsBytesUsed.addAndGet(-termsBytesUsed.get());
    deleteTerms.clear();
  }

  void clear() {
    deleteTerms.clear();
    deleteQueries.clear();
    numTermDeletes.set(0);
    numFieldUpdates.set(0);
    fieldUpdates.clear();
    bytesUsed.addAndGet(-bytesUsed.get());
    fieldUpdatesBytesUsed.addAndGet(-fieldUpdatesBytesUsed.get());
    termsBytesUsed.addAndGet(-termsBytesUsed.get());
  }

  boolean any() {
    return deleteTerms.size() > 0 || deleteQueries.size() > 0 || numFieldUpdates.get() > 0;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get() + fieldUpdatesBytesUsed.get() + termsBytesUsed.get();
  }
}
