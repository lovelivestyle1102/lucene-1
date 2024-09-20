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

/**
 * Controls how much information is stored in the postings lists.
 *
 * @lucene.experimental
 */
public enum IndexOptions {
  // NOTE: order is important here; FieldInfo uses this
  // order to merge two conflicting IndexOptions (always
  // "downgrades" by picking the lowest).
  /** Not indexed */
  NONE,

  /**
   *
   * 文档写入倒排表，但由于不记录词频信息，因此出现多次也仅当一次处理
   *
   * Only documents are indexed: term frequencies and positions are omitted. Phrase and other
   * positional queries on the field will throw an exception, and scoring will behave as if any term
   * in the document appears only once.
   */
  DOCS,

  /**
   * 文档和词频写入倒排表
   *
   * Only documents and term frequencies are indexed: positions are omitted. This enables normal
   * scoring, except Phrase and other positional queries will throw an exception.
   */
  DOCS_AND_FREQS,

  /**
   * 文档、词频及位置写入倒排表
   *
   * Indexes documents, frequencies and positions. This is a typical default for full-text search:
   * full scoring is enabled and positional queries are supported.
   */
  DOCS_AND_FREQS_AND_POSITIONS,

  /**
   * 文档、词频、位置及偏移写入倒排表
   *
   * Indexes documents, frequencies, positions and offsets. Character offsets are encoded alongside
   * the positions.
   */
  DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
}
