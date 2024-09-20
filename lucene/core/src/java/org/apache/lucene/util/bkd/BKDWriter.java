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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.bkd.BKDUtil.ByteArrayPredicate;

// TODO
//   - allow variable length byte[] (across docs and dims), but this is quite a bit more hairy
//   - we could also index "auto-prefix terms" here, and use better compression, and maybe only use
// for the "fully contained" case so we'd
//     only index docIDs
//   - the index could be efficiently encoded as an FST, so we don't have wasteful
//     (monotonic) long[] leafBlockFPs; or we could use MonotonicLongValues ... but then
//     the index is already plenty small: 60M OSM points --> 1.1 MB with 128 points
//     per leaf, and you can reduce that by putting more points per leaf
//   - we could use threads while building; the higher nodes are very parallelizable

/**
 * Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller and
 * smaller N-dim rectangles (cells) until the number of points in a given rectangle is &lt;= <code>
 * config.maxPointsInLeafNode</code>. The tree is partially balanced, which means the leaf nodes
 * will have the requested <code>config.maxPointsInLeafNode</code> except one that might have less.
 * Leaf nodes may straddle the two bottom levels of the binary tree. Values that fall exactly on a
 * cell boundary may be in either cell.
 *
 * <p>The number of dimensions can be 1 to 8, but every byte[] value is fixed length.
 *
 * <p>This consumes heap during writing: it allocates a <code>Long[numLeaves]</code>, a <code>
 * byte[numLeaves*(1+config.bytesPerDim)]</code> and then uses up to the specified {@code
 * maxMBSortInHeap} heap space for writing.
 *
 * <p><b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>config.maxPointsInLeafNode
 * </code> / config.bytesPerDim total points.
 *
 * @lucene.experimental
 */
public class BKDWriter implements Closeable {

  public static final String CODEC_NAME = "BKD";

  public static final int VERSION_START = 4; // version used by Lucene 7.0

  // public static final int VERSION_CURRENT = VERSION_START;
  public static final int VERSION_LEAF_STORES_BOUNDS = 5;
  public static final int VERSION_SELECTIVE_INDEXING = 6;
  public static final int VERSION_LOW_CARDINALITY_LEAVES = 7;
  public static final int VERSION_META_FILE = 9;
  public static final int VERSION_CURRENT = VERSION_META_FILE;

  /** Number of splits before we compute the exact bounding box of an inner node. */
  private static final int SPLITS_BEFORE_EXACT_BOUNDS = 4;

  /** Default maximum heap to use, before spilling to (slower) disk */
  public static final float DEFAULT_MAX_MB_SORT_IN_HEAP = 16.0f;

  /** BKD tree configuration */
  // 关于BKD树构建过程的一些配置信息
  protected final BKDConfig config;

  private final ByteArrayComparator comparator;
  private final ByteArrayPredicate equalsPredicate;
  private final ByteArrayComparator commonPrefixComparator;

  final TrackingDirectoryWrapper tempDir;
  final String tempFileNamePrefix;
  final double maxMBSortInHeap;

  // 以下这些都是临时变量
  final byte[] scratchDiff;
  final byte[] scratch1;
  final byte[] scratch2;
  final BytesRef scratchBytesRef1 = new BytesRef();
  final BytesRef scratchBytesRef2 = new BytesRef();

  // 下标是维度，值是对应维度的所有值的最长公共前缀
  final int[] commonPrefixLengths;

  // 存储docID
  protected final FixedBitSet docsSeen;

  // 存储所有的数据
  private PointWriter pointWriter;

  // 是否结束数据添加，开始构建
  private boolean finished;

  private IndexOutput tempInput;
  private final int maxPointsSortInHeap;

  /** Minimum per-dim values, packed */
  // 下标是维度，值是这一维中的最小值
  protected final byte[] minPackedValue;

  /** Maximum per-dim values, packed */
  // 下标是维度，值是这一维中的最大值
  protected final byte[] maxPackedValue;

  protected long pointCount;

  /** An upper bound on how many points the caller will add (includes deletions) */
  // 总的point数
  private final long totalPointCount;

  private final int maxDoc;

  public BKDWriter(
      int maxDoc,
      Directory tempDir,
      String tempFileNamePrefix,
      BKDConfig config,
      double maxMBSortInHeap,
      long totalPointCount) {

    verifyParams(maxMBSortInHeap, totalPointCount);

    // We use tracking dir to deal with removing files on exception, so each place that
    // creates temp files doesn't need crazy try/finally/sucess logic:
    this.tempDir = new TrackingDirectoryWrapper(tempDir);

    this.tempFileNamePrefix = tempFileNamePrefix;

    this.maxMBSortInHeap = maxMBSortInHeap;

    this.totalPointCount = totalPointCount;

    this.maxDoc = maxDoc;

    this.config = config;

    this.comparator = ArrayUtil.getUnsignedComparator(config.bytesPerDim);

    this.equalsPredicate = BKDUtil.getEqualsPredicate(config.bytesPerDim);

    this.commonPrefixComparator = BKDUtil.getPrefixLengthComparator(config.bytesPerDim);

    docsSeen = new FixedBitSet(maxDoc);

    scratchDiff = new byte[config.bytesPerDim];

    scratch1 = new byte[config.packedBytesLength];

    scratch2 = new byte[config.packedBytesLength];

    commonPrefixLengths = new int[config.numDims];

    minPackedValue = new byte[config.packedIndexBytesLength];

    maxPackedValue = new byte[config.packedIndexBytesLength];

    // Maximum number of points we hold in memory at any time
    maxPointsSortInHeap = (int) ((maxMBSortInHeap * 1024 * 1024) / (config.bytesPerDoc));

    // Finally, we must be able to hold at least the leaf node in heap during build:
    if (maxPointsSortInHeap < config.maxPointsInLeafNode) {
      throw new IllegalArgumentException(
          "maxMBSortInHeap="
              + maxMBSortInHeap
              + " only allows for maxPointsSortInHeap="
              + maxPointsSortInHeap
              + ", but this is less than maxPointsInLeafNode="
              + config.maxPointsInLeafNode
              + "; "
              + "either increase maxMBSortInHeap or decrease maxPointsInLeafNode");
    }
  }

  private static void verifyParams(double maxMBSortInHeap, long totalPointCount) {
    if (maxMBSortInHeap < 0.0) {
      throw new IllegalArgumentException(
          "maxMBSortInHeap must be >= 0.0 (got: " + maxMBSortInHeap + ")");
    }
    if (totalPointCount < 0) {
      throw new IllegalArgumentException(
          "totalPointCount must be >=0 (got: " + totalPointCount + ")");
    }
  }

  private void initPointWriter() throws IOException {
    assert pointWriter == null : "Point writer is already initialized";
    // Total point count is an estimation but the final point count must be equal or lower to that
    // number.
    if (totalPointCount > maxPointsSortInHeap) {
      pointWriter = new OfflinePointWriter(config, tempDir, tempFileNamePrefix, "spill", 0);

      tempInput = ((OfflinePointWriter) pointWriter).out;
    } else {
      pointWriter = new HeapPointWriter(config, Math.toIntExact(totalPointCount));
    }
  }

  public void add(byte[] packedValue, int docID) throws IOException {
    if (packedValue.length != config.packedBytesLength) {
      throw new IllegalArgumentException(
          "packedValue should be length="
              + config.packedBytesLength
              + " (got: "
              + packedValue.length
              + ")");
    }

    if (pointCount >= totalPointCount) {
      throw new IllegalStateException(
          "totalPointCount="
              + totalPointCount
              + " was passed when we were created, but we just hit "
              + (pointCount + 1)
              + " values");
    }

    if (pointCount == 0) {
      initPointWriter();
      System.arraycopy(packedValue, 0, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(packedValue, 0, maxPackedValue, 0, config.packedIndexBytesLength);
    } else {
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        int offset = dim * config.bytesPerDim;
        if (comparator.compare(packedValue, offset, minPackedValue, offset) < 0) {
          System.arraycopy(packedValue, offset, minPackedValue, offset, config.bytesPerDim);
        } else if (comparator.compare(packedValue, offset, maxPackedValue, offset) > 0) {
          System.arraycopy(packedValue, offset, maxPackedValue, offset, config.bytesPerDim);
        }
      }
    }

    pointWriter.append(packedValue, docID);

    pointCount++;

    docsSeen.set(docID);
  }

  private static class MergeReader {
    final BKDReader bkd;

    final BKDReader.IntersectState state;

    final MergeState.DocMap docMap;

    /** Current doc ID */
    public int docID;

    /** Which doc in this block we are up to */
    private int docBlockUpto;

    /** How many docs in the current block */
    private int docsInBlock;

    /** Which leaf block we are up to */
    private int blockID;

    private final byte[] packedValues;

    public MergeReader(BKDReader bkd, MergeState.DocMap docMap) throws IOException {
      this.bkd = bkd;
      state = new BKDReader.IntersectState(bkd.in.clone(), bkd.config, null, null);
      this.docMap = docMap;
      state.in.seek(bkd.getMinLeafBlockFP());
      this.packedValues = new byte[bkd.config.maxPointsInLeafNode * bkd.config.packedBytesLength];
    }

    public boolean next() throws IOException {
      // System.out.println("MR.next this=" + this);
      while (true) {
        if (docBlockUpto == docsInBlock) {
          if (blockID == bkd.leafNodeOffset) {
            // System.out.println("  done!");
            return false;
          }
          // System.out.println("  new block @ fp=" + state.in.getFilePointer());
          docsInBlock = bkd.readDocIDs(state.in, state.in.getFilePointer(), state.scratchIterator);
          assert docsInBlock > 0;
          docBlockUpto = 0;
          bkd.visitDocValues(
              state.commonPrefixLengths,
              state.scratchDataPackedValue,
              state.scratchMinIndexPackedValue,
              state.scratchMaxIndexPackedValue,
              state.in,
              state.scratchIterator,
              docsInBlock,
              new IntersectVisitor() {
                int i = 0;

                @Override
                public void visit(int docID) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                  assert docID == state.scratchIterator.docIDs[i];
                  System.arraycopy(
                      packedValue,
                      0,
                      packedValues,
                      i * bkd.config.packedBytesLength,
                      bkd.config.packedBytesLength);
                  i++;
                }

                @Override
                public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                  return Relation.CELL_CROSSES_QUERY;
                }
              });

          blockID++;
        }

        final int index = docBlockUpto++;
        int oldDocID = state.scratchIterator.docIDs[index];

        int mappedDocID;
        if (docMap == null) {
          mappedDocID = oldDocID;
        } else {
          mappedDocID = docMap.get(oldDocID);
        }

        if (mappedDocID != -1) {
          // Not deleted!
          docID = mappedDocID;
          System.arraycopy(
              packedValues,
              index * bkd.config.packedBytesLength,
              state.scratchDataPackedValue,
              0,
              bkd.config.packedBytesLength);
          return true;
        }
      }
    }
  }

  private static class BKDMergeQueue extends PriorityQueue<MergeReader> {
    private final ByteArrayComparator comparator;

    public BKDMergeQueue(int bytesPerDim, int maxSize) {
      super(maxSize);
      this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
    }

    @Override
    public boolean lessThan(MergeReader a, MergeReader b) {
      assert a != b;

      int cmp =
          comparator.compare(a.state.scratchDataPackedValue, 0, b.state.scratchDataPackedValue, 0);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      }

      // Tie break by sorting smaller docIDs earlier:
      return a.docID < b.docID;
    }
  }

  /** flat representation of a kd-tree */
  private interface BKDTreeLeafNodes {
    /** number of leaf nodes */
    int numLeaves();
    /**
     * pointer to the leaf node previously written. Leaves are order from left to right, so leaf at
     * {@code index} 0 is the leftmost leaf and the the leaf at {@code numleaves()} -1 is the
     * rightmost leaf
     */
    long getLeafLP(int index);
    /**
     * split value between two leaves. The split value at position n corresponds to the leaves at (n
     * -1) and n.
     */
    BytesRef getSplitValue(int index);
    /**
     * split dimension between two leaves. The split dimension at position n corresponds to the
     * leaves at (n -1) and n.
     */
    int getSplitDimension(int index);
  }

  /**
   * Write a field from a {@link MutablePointValues}. This way of writing points is faster than
   * regular writes with {@link BKDWriter#add} since there is opportunity for reordering points
   * before writing them to disk. This method does not use transient disk in order to reorder
   * points.
   */
  public Runnable writeField(
      IndexOutput metaOut,
      IndexOutput indexOut,
      IndexOutput dataOut,
      String fieldName,
      MutablePointValues reader)
      throws IOException {
    if (config.numDims == 1) {
      return writeField1Dim(metaOut, indexOut, dataOut, fieldName, reader);
    } else {
      return writeFieldNDims(metaOut, indexOut, dataOut, fieldName, reader);
    }
  }

  private void computePackedValueBounds(
      MutablePointValues values,
      int from,
      int to,
      byte[] minPackedValue,
      byte[] maxPackedValue,
      BytesRef scratch) {
    if (from == to) {
      return;
    }
    values.getValue(from, scratch);
    System.arraycopy(
        scratch.bytes, scratch.offset, minPackedValue, 0, config.packedIndexBytesLength);
    System.arraycopy(
        scratch.bytes, scratch.offset, maxPackedValue, 0, config.packedIndexBytesLength);
    for (int i = from + 1; i < to; ++i) {
      values.getValue(i, scratch);
      for (int dim = 0; dim < config.numIndexDims; dim++) {
        final int startOffset = dim * config.bytesPerDim;
        if (comparator.compare(
                scratch.bytes, scratch.offset + startOffset, minPackedValue, startOffset)
            < 0) {
          System.arraycopy(
              scratch.bytes,
              scratch.offset + startOffset,
              minPackedValue,
              startOffset,
              config.bytesPerDim);
        } else if (comparator.compare(
                scratch.bytes, scratch.offset + startOffset, maxPackedValue, startOffset)
            > 0) {
          System.arraycopy(
              scratch.bytes,
              scratch.offset + startOffset,
              maxPackedValue,
              startOffset,
              config.bytesPerDim);
        }
      }
    }
  }

  /* In the 2+D case, we recursively pick the split dimension, compute the
   * median value and partition other values around it. */
  private Runnable writeFieldNDims(
      IndexOutput metaOut,
      IndexOutput indexOut,
      IndexOutput dataOut,
      String fieldName,
      MutablePointValues values)
      throws IOException {
    if (pointCount != 0) {
      throw new IllegalStateException("cannot mix add and writeField");
    }

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    // Mark that we already finished:
    finished = true;

    pointCount = values.size();

    // 通过point总数和每个叶子节点最多有多少个point计算有多少个叶子节点
    final int numLeaves =
        Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);

    // 需要多少个分裂点
    final int numSplits = numLeaves - 1;

    checkMaxLeafNodeCount(numLeaves);

    // 下标是第几次分裂，值是split的value
    final byte[] splitPackedValues = new byte[numSplits * config.bytesPerDim];

    // 下标是第几次分裂，值是split用的维度
    final byte[] splitDimensionValues = new byte[numSplits];

    // 每个叶子节点在kdd文件中的起始位置
    final long[] leafBlockFPs = new long[numLeaves];

    // compute the min/max for this slice
    // 所有point的所有维度的最大值和最小值分别存储在maxPackedValue和minPackedValue中
    computePackedValueBounds(
        values, 0, Math.toIntExact(pointCount), minPackedValue, maxPackedValue, scratchBytesRef1);

    // 记录所有的docID
    for (int i = 0; i < Math.toIntExact(pointCount); ++i) {
      docsSeen.set(values.getDocID(i));
    }

    final long dataStartFP = dataOut.getFilePointer();

    // 记录的是每一维已经用作split的次数
    final int[] parentSplits = new int[config.numIndexDims];

    // 递归获取每个叶子节点并进行存储在kdd中
    build(
        0,
        numLeaves,
        values,
        0,
        Math.toIntExact(pointCount),
        dataOut,
        minPackedValue.clone(),
        maxPackedValue.clone(),
        parentSplits,
        splitPackedValues,
        splitDimensionValues,
        leafBlockFPs,
        new int[config.maxPointsInLeafNode]);
    assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

    scratchBytesRef1.length = config.bytesPerDim;
    scratchBytesRef1.bytes = splitPackedValues;

    BKDTreeLeafNodes leafNodes =
        new BKDTreeLeafNodes() {

          // 获取第index个叶子节点在kdd中的起始位置
          @Override
          public long getLeafLP(int index) {
            return leafBlockFPs[index];
          }

          // 获取第index个叶子节点的分裂值
          @Override
          public BytesRef getSplitValue(int index) {
            scratchBytesRef1.offset = index * config.bytesPerDim;
            return scratchBytesRef1;
          }

          // 获取第index个叶子节点的分裂维度
          @Override
          public int getSplitDimension(int index) {
            return splitDimensionValues[index] & 0xff;
          }

          @Override
          public int numLeaves() {
            return leafBlockFPs.length;
          }
        };

    return () -> {
      try {
        writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /* In the 1D case, we can simply sort points in ascending order and use the
   * same writing logic as we use at merge time. */
  private Runnable writeField1Dim(
      IndexOutput metaOut,
      IndexOutput indexOut,
      IndexOutput dataOut,
      String fieldName,
      MutablePointValues reader)
      throws IOException {
    MutablePointsReaderUtils.sort(config, maxDoc, reader, 0, Math.toIntExact(reader.size()));

    final OneDimensionBKDWriter oneDimWriter =
        new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    reader.intersect(
        new IntersectVisitor() {

          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {
            // 入口在这个方法，把所有的数据都添加到oneDimWriter
            oneDimWriter.add(packedValue, docID);
          }

          @Override
          public void visit(int docID) {
            throw new IllegalStateException();
          }

          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_CROSSES_QUERY;
          }
        });

    // 返回一个构建kdi的Runnable方法
    return oneDimWriter.finish();
  }

  /**
   * More efficient bulk-add for incoming {@link BKDReader}s. This does a merge sort of the already
   * sorted values and currently only works when numDims==1. This returns -1 if all documents
   * containing dimensional values were deleted.
   */
  public Runnable merge(
      IndexOutput metaOut,
      IndexOutput indexOut,
      IndexOutput dataOut,
      List<MergeState.DocMap> docMaps,
      List<BKDReader> readers)
      throws IOException {
    assert docMaps == null || readers.size() == docMaps.size();

    BKDMergeQueue queue = new BKDMergeQueue(config.bytesPerDim, readers.size());

    for (int i = 0; i < readers.size(); i++) {
      BKDReader bkd = readers.get(i);
      MergeState.DocMap docMap;
      if (docMaps == null) {
        docMap = null;
      } else {
        docMap = docMaps.get(i);
      }
      MergeReader reader = new MergeReader(bkd, docMap);
      if (reader.next()) {
        queue.add(reader);
      }
    }

    OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(metaOut, indexOut, dataOut);

    while (queue.size() != 0) {
      MergeReader reader = queue.top();
      // System.out.println("iter reader=" + reader);

      oneDimWriter.add(reader.state.scratchDataPackedValue, reader.docID);

      if (reader.next()) {
        queue.updateTop();
      } else {
        // This segment was exhausted
        queue.pop();
      }
    }

    return oneDimWriter.finish();
  }

  private class OneDimensionBKDWriter {

    final IndexOutput metaOut, indexOut, dataOut;
    final long dataStartFP;
    final List<Long> leafBlockFPs = new ArrayList<>();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();
    final byte[] leafValues = new byte[config.maxPointsInLeafNode * config.packedBytesLength];
    final int[] leafDocs = new int[config.maxPointsInLeafNode];
    private long valueCount;
    private int leafCount;
    private int leafCardinality;

    OneDimensionBKDWriter(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut) {
      if (config.numIndexDims != 1) {
        throw new UnsupportedOperationException(
            "config.numIndexDims must be 1 but got " + config.numIndexDims);
      }
      if (pointCount != 0) {
        throw new IllegalStateException("cannot mix add and merge");
      }

      // Catch user silliness:
      if (finished == true) {
        throw new IllegalStateException("already finished");
      }

      // Mark that we already finished:
      finished = true;

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      this.dataOut = dataOut;
      this.dataStartFP = dataOut.getFilePointer();

      lastPackedValue = new byte[config.packedBytesLength];
    }

    // for asserts
    final byte[] lastPackedValue;
    private int lastDocID;

    void add(byte[] packedValue, int docID) throws IOException {
      assert valueInOrder(
          config, valueCount + leafCount, 0, lastPackedValue, packedValue, 0, docID, lastDocID);

      if (leafCount == 0
          || equalsPredicate.test(leafValues, (leafCount - 1) * config.bytesPerDim, packedValue, 0)
              == false) {// 统计有多少个不一样的值，只要跟前一个值相比就行，因为是排好序的
        leafCardinality++;
      }

      // 当前正在处理的point拷贝到leafValues
      System.arraycopy(
          packedValue,
          0,
          leafValues,
          leafCount * config.packedBytesLength,
          config.packedBytesLength);

      // 记录docID
      leafDocs[leafCount] = docID;
      docsSeen.set(docID);

      // 当前叶子节点的point个数
      leafCount++;

      if (valueCount + leafCount > totalPointCount) {
        throw new IllegalStateException(
            "totalPointCount="
                + totalPointCount
                + " was passed when we were created, but we just hit "
                + (valueCount + leafCount)
                + " values");
      }

      if (leafCount == config.maxPointsInLeafNode) { // 一个叶子节点满了
        // We write a block once we hit exactly the max count ... this is different from
        // when we write N > 1 dimensional points where we write between max/2 and max per leaf
        // block
        // 持久化叶子节点的数据
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
    }

    public Runnable finish() throws IOException {
      if (leafCount > 0) {
        writeLeafBlock(leafCardinality);
        leafCardinality = 0;
        leafCount = 0;
      }

      if (valueCount == 0) {
        return null;
      }

      pointCount = valueCount;

      scratchBytesRef1.length = config.bytesPerDim;
      scratchBytesRef1.offset = 0;
      assert leafBlockStartValues.size() + 1 == leafBlockFPs.size();
      BKDTreeLeafNodes leafNodes =
          new BKDTreeLeafNodes() {
            @Override
            public long getLeafLP(int index) {
              return leafBlockFPs.get(index);
            }

            @Override
            public BytesRef getSplitValue(int index) {
              scratchBytesRef1.bytes = leafBlockStartValues.get(index);
              return scratchBytesRef1;
            }

            @Override
            public int getSplitDimension(int index) {
              return 0;
            }

            @Override
            public int numLeaves() {
              return leafBlockFPs.size();
            }
          };
      return () -> {
        try {
          writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };
    }

    private void writeLeafBlock(int leafCardinality) throws IOException {
      assert leafCount != 0;
      if (valueCount == 0) {// minPackedValue就是第一个叶子节点的第一个point
        System.arraycopy(leafValues, 0, minPackedValue, 0, config.packedIndexBytesLength);
      }

      // 当前叶子节点的最后一个point就是maxPackedValue
      System.arraycopy(
          leafValues,
          (leafCount - 1) * config.packedBytesLength,
          maxPackedValue,
          0,
          config.packedIndexBytesLength);

      // 更新当前已经处理的point个数
      valueCount += leafCount;

      // 每个叶子节点的第一个point当做是叶子节点之间的分割点
      if (leafBlockFPs.size() > 0) {
        // Save the first (minimum) value in each leaf block except the first, to build the split
        // value index in the end:
        leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength));
      }

      // 当前叶子节点在kdd文件中的起始位置
      leafBlockFPs.add(dataOut.getFilePointer());
      checkMaxLeafNodeCount(leafBlockFPs.size());

      // Find per-dim common prefix:
      // 获取所有值的公共前缀
      commonPrefixLengths[0] =
          commonPrefixComparator.compare(
              leafValues, 0, leafValues, (leafCount - 1) * config.packedBytesLength);

      // 记录当前叶子节点中point对应的docID集合
      writeLeafBlockDocs(dataOut, leafDocs, 0, leafCount);

      // 记录公共前缀
      writeCommonPrefixes(dataOut, commonPrefixLengths, leafValues);

      scratchBytesRef1.length = config.packedBytesLength;
      scratchBytesRef1.bytes = leafValues;

      final IntFunction<BytesRef> packedValues =
          new IntFunction<BytesRef>() {
            @Override
            public BytesRef apply(int i) {
              scratchBytesRef1.offset = config.packedBytesLength * i;
              return scratchBytesRef1;
            }
          };
      assert valuesInOrderAndBounds(
          config,
          leafCount,
          0,
          ArrayUtil.copyOfSubArray(leafValues, 0, config.packedBytesLength),
          ArrayUtil.copyOfSubArray(
              leafValues,
              (leafCount - 1) * config.packedBytesLength,
              leafCount * config.packedBytesLength),
          packedValues,
          leafDocs,
          0);

      // 持久化所有的point数据，有两种方式，后面详细介绍
      writeLeafBlockPackedValues(
          dataOut, commonPrefixLengths, leafCount, 0, packedValues, leafCardinality);
    }
  }

  private int getNumLeftLeafNodes(int numLeaves) {
    assert numLeaves > 1 : "getNumLeftLeaveNodes() called with " + numLeaves;
    // return the level that can be filled with this number of leaves
    int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
    // how many leaf nodes are in the full level
    int leavesFullLevel = 1 << lastFullLevel;
    // half of the leaf nodes from the full level goes to the left
    int numLeftLeafNodes = leavesFullLevel / 2;
    // leaf nodes that do not fit in the full level
    int unbalancedLeafNodes = numLeaves - leavesFullLevel;
    // distribute unbalanced leaf nodes
    numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
    // we should always place unbalanced leaf nodes on the left
    assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes
        && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
    return numLeftLeafNodes;
  }

  // TODO: if we fixed each partition step to just record the file offset at the "split point", we
  // could probably handle variable length
  // encoding and not have our own ByteSequencesReader/Writer

  // useful for debugging:
  /*
  private void printPathSlice(String desc, PathSlice slice, int dim) throws IOException {
    System.out.println("    " + desc + " dim=" + dim + " count=" + slice.count + ":");
    try(PointReader r = slice.writer.getReader(slice.start, slice.count)) {
      int count = 0;
      while (r.next()) {
        byte[] v = r.packedValue();
        System.out.println("      " + count + ": " + new BytesRef(v, dim*config.bytesPerDim, config.bytesPerDim));
        count++;
        if (count == slice.count) {
          break;
        }
      }
    }
  }
  */

  private void checkMaxLeafNodeCount(int numLeaves) {
    if (config.bytesPerDim * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalStateException(
          "too many nodes; increase config.maxPointsInLeafNode (currently "
              + config.maxPointsInLeafNode
              + ") and reindex");
    }
  }

  /**
   * Writes the BKD tree to the provided {@link IndexOutput}s and returns a {@link Runnable} that
   * writes the index of the tree if at least one point has been added, or {@code null} otherwise.
   */
  public Runnable finish(IndexOutput metaOut, IndexOutput indexOut, IndexOutput dataOut)
      throws IOException {
    // System.out.println("\nBKDTreeWriter.finish pointCount=" + pointCount + " out=" + out + "
    // heapWriter=" + heapPointWriter);

    // TODO: specialize the 1D case?  it's much faster at indexing time (no partitioning on
    // recurse...)

    // Catch user silliness:
    if (finished == true) {
      throw new IllegalStateException("already finished");
    }

    if (pointCount == 0) {
      return null;
    }

    // mark as finished
    finished = true;

    pointWriter.close();
    BKDRadixSelector.PathSlice points = new BKDRadixSelector.PathSlice(pointWriter, 0, pointCount);
    // clean up pointers
    tempInput = null;
    pointWriter = null;

    final int numLeaves =
        Math.toIntExact((pointCount + config.maxPointsInLeafNode - 1) / config.maxPointsInLeafNode);
    final int numSplits = numLeaves - 1;

    checkMaxLeafNodeCount(numLeaves);

    // NOTE: we could save the 1+ here, to use a bit less heap at search time, but then we'd need a
    // somewhat costly check at each
    // step of the recursion to recompute the split dim:

    // Indexed by nodeID, but first (root) nodeID is 1.  We do 1+ because the lead byte at each
    // recursion says which dim we split on.
    byte[] splitPackedValues = new byte[Math.toIntExact(numSplits * config.bytesPerDim)];
    byte[] splitDimensionValues = new byte[numSplits];

    // +1 because leaf count is power of 2 (e.g. 8), and innerNodeCount is power of 2 minus 1 (e.g.
    // 7)
    long[] leafBlockFPs = new long[numLeaves];

    // Make sure the math above "worked":
    assert pointCount / numLeaves <= config.maxPointsInLeafNode
        : "pointCount="
            + pointCount
            + " numLeaves="
            + numLeaves
            + " config.maxPointsInLeafNode="
            + config.maxPointsInLeafNode;

    // We re-use the selector so we do not need to create an object every time.
    BKDRadixSelector radixSelector =
        new BKDRadixSelector(config, maxPointsSortInHeap, tempDir, tempFileNamePrefix);

    final long dataStartFP = dataOut.getFilePointer();
    boolean success = false;
    try {

      final int[] parentSplits = new int[config.numIndexDims];
      build(
          0,
          numLeaves,
          points,
          dataOut,
          radixSelector,
          minPackedValue.clone(),
          maxPackedValue.clone(),
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          new int[config.maxPointsInLeafNode]);
      assert Arrays.equals(parentSplits, new int[config.numIndexDims]);

      // If no exception, we should have cleaned everything up:
      assert tempDir.getCreatedFiles().isEmpty();
      // long t2 = System.nanoTime();
      // System.out.println("write time: " + ((t2-t1)/1000000.0) + " msec");

      success = true;
    } finally {
      if (success == false) {
        IOUtils.deleteFilesIgnoringExceptions(tempDir, tempDir.getCreatedFiles());
      }
    }

    scratchBytesRef1.bytes = splitPackedValues;
    scratchBytesRef1.length = config.bytesPerDim;
    BKDTreeLeafNodes leafNodes =
        new BKDTreeLeafNodes() {
          @Override
          public long getLeafLP(int index) {
            return leafBlockFPs[index];
          }

          @Override
          public BytesRef getSplitValue(int index) {
            scratchBytesRef1.offset = index * config.bytesPerDim;
            return scratchBytesRef1;
          }

          @Override
          public int getSplitDimension(int index) {
            return splitDimensionValues[index] & 0xff;
          }

          @Override
          public int numLeaves() {
            return leafBlockFPs.length;
          }
        };

    return () -> {
      // Write index:
      try {
        writeIndex(metaOut, indexOut, config.maxPointsInLeafNode, leafNodes, dataStartFP);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  /**
   * Packs the two arrays, representing a semi-balanced binary tree, into a compact byte[]
   * structure.
   */
  private byte[] packIndex(BKDTreeLeafNodes leafNodes) throws IOException {
    /** Reused while packing the index */
    // 临时存储
    ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();

    // This is the "file" we append the byte[] to:
    // 递归过程中，构建好的索引树片段存储在blocks中
    List<byte[]> blocks = new ArrayList<>();
    byte[] lastSplitValues = new byte[config.bytesPerDim * config.numIndexDims];
    // System.out.println("\npack index");

    // 递归构建索引树
    int totalSize =
        recursePackIndex(
            writeBuffer,
            leafNodes,
            0l,
            blocks,
            lastSplitValues,
            new boolean[config.numIndexDims],
            false,
            0,
            leafNodes.numLeaves());

    // Compact the byte[] blocks into single byte index:
    // 完整的索引树存储到index
    byte[] index = new byte[totalSize];
    int upto = 0;
    for (byte[] block : blocks) {
      System.arraycopy(block, 0, index, upto, block.length);
      upto += block.length;
    }
    assert upto == totalSize;

    return index;
  }

  /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
  private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) {
    byte[] block = writeBuffer.toArrayCopy();
    blocks.add(block);
    writeBuffer.reset();
    return block.length;
  }

  /**
   * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the
   * split byte[] on each inner node
   */
  private int recursePackIndex(
      ByteBuffersDataOutput writeBuffer,// 用来临时存储数据，满足一个block的时候会加入到block中
      BKDTreeLeafNodes leafNodes,// 获取叶子节点的相关数据
      long minBlockFP,// 当前处理叶子节点范围中最左边叶子节点在kdd中的位置
      List<byte[]> blocks,
      byte[] lastSplitValues,// 每一维的上一次分割点的值
      boolean[] negativeDeltas,
      boolean isLeft, // 是否是左子树
      int leavesOffset,// 从第几个叶子节点开始
      int numLeaves)// 要处理多少个叶子节点
      throws IOException {
    if (numLeaves == 1) {// 叶子节点
      if (isLeft) {
        assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
        return 0;
      } else {
        long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0
            : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
        return appendBlock(writeBuffer, blocks);
      }
    } else {
      long leftBlockFP;
      if (isLeft) {
        // The left tree's left most leaf block FP is always the minimal FP:
        assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
        leftBlockFP = minBlockFP;
      } else {
        leftBlockFP = leafNodes.getLeafLP(leavesOffset);
        long delta = leftBlockFP - minBlockFP;
        assert leafNodes.numLeaves() == numLeaves || delta > 0
            : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
        writeBuffer.writeVLong(delta);
      }

      // 左子树的叶子节点个数
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);

      // 右子树的叶子节点起始编号
      final int rightOffset = leavesOffset + numLeftLeafNodes;

      // 分割点的编号
      final int splitOffset = rightOffset - 1;

      // 用来分割的值的维度
      int splitDim = leafNodes.getSplitDimension(splitOffset);

      // 分割点的值，注意值是从分割维度开始的
      BytesRef splitValue = leafNodes.getSplitValue(splitOffset);
      int address = splitValue.offset;

      // System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + "
      // splitValue=" + new BytesRef(splitPackedValues, address, config.bytesPerDim));

      // find common prefix with last split value in this dim:
      // 和前一个分割值相比，前缀长度
      int prefix =
          commonPrefixComparator.compare(
              splitValue.bytes, address, lastSplitValues, splitDim * config.bytesPerDim);

      // System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims="
      // + numDims + " config.bytesPerDim=" + config.bytesPerDim + " prefix=" + prefix);

      // 第一个不相等的值的差值
      int firstDiffByteDelta;
      if (prefix < config.bytesPerDim) {
        // System.out.println("  delta byte cur=" +
        // Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" +
        // Integer.toHexString(lastSplitValues[splitDim * config.bytesPerDim + prefix]&0xFF) + "
        // negated?=" + negativeDeltas[splitDim]);
        firstDiffByteDelta =
            (splitValue.bytes[address + prefix] & 0xFF)
                - (lastSplitValues[splitDim * config.bytesPerDim + prefix] & 0xFF);
        if (negativeDeltas[splitDim]) {// 左子树和split value的差值是负的，需要转化下符号
          firstDiffByteDelta = -firstDiffByteDelta;
        }
        // System.out.println("  delta=" + firstDiffByteDelta);
        assert firstDiffByteDelta > 0;
      } else {
        firstDiffByteDelta = 0;
      }

      // pack the prefix, splitDim and delta first diff byte into a single vInt:
      // 把  firstDiffByteDelta，prefix，splitDim编码成code
      int code =
          (firstDiffByteDelta * (1 + config.bytesPerDim) + prefix) * config.numIndexDims + splitDim;

      // System.out.println("  code=" + code);
      // System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address,
      // config.bytesPerDim));

      writeBuffer.writeVInt(code);

      // write the split value, prefix coded vs. our parent's split value:
      // 存储splitvalue的后缀
      int suffix = config.bytesPerDim - prefix;
      byte[] savSplitValue = new byte[suffix];
      if (suffix > 1) {
        writeBuffer.writeBytes(splitValue.bytes, address + prefix + 1, suffix - 1);
      }

      byte[] cmp = lastSplitValues.clone();

      // 更新lastSplitValues
      System.arraycopy(
          lastSplitValues, splitDim * config.bytesPerDim + prefix, savSplitValue, 0, suffix);

      // copy our split value into lastSplitValues for our children to prefix-code against
      System.arraycopy(
          splitValue.bytes,
          address + prefix,
          lastSplitValues,
          splitDim * config.bytesPerDim + prefix,
          suffix);

      int numBytes = appendBlock(writeBuffer, blocks);

      // placeholder for left-tree numBytes; we need this so that at search time if we only need to
      // recurse into the right sub-tree we can
      // quickly seek to its starting point
      // 当前位置空出来，留着存储左子树的总大小，这样就能快速定位到兄弟节点
      int idxSav = blocks.size();
      blocks.add(null);

      boolean savNegativeDelta = negativeDeltas[splitDim];
      negativeDeltas[splitDim] = true;

      // 递归构建左子树的索引树
      int leftNumBytes =
          recursePackIndex(
              writeBuffer,
              leafNodes,
              leftBlockFP,
              blocks,
              lastSplitValues,
              negativeDeltas,
              true,
              leavesOffset,
              numLeftLeafNodes);

      if (numLeftLeafNodes != 1) {
        writeBuffer.writeVInt(leftNumBytes);
      } else {
        assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
      }

      byte[] bytes2 = writeBuffer.toArrayCopy();
      writeBuffer.reset();
      // replace our placeholder:
      blocks.set(idxSav, bytes2);

      negativeDeltas[splitDim] = false;

      // 递归构建右子树的索引树
      int rightNumBytes =
          recursePackIndex(
              writeBuffer,
              leafNodes,
              leftBlockFP,
              blocks,
              lastSplitValues,
              negativeDeltas,
              false,
              rightOffset,
              numLeaves - numLeftLeafNodes);

      negativeDeltas[splitDim] = savNegativeDelta;

      // restore lastSplitValues to what caller originally passed us:
      System.arraycopy(
          savSplitValue, 0, lastSplitValues, splitDim * config.bytesPerDim + prefix, suffix);

      assert Arrays.equals(lastSplitValues, cmp);

      return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
    }
  }

  private void writeIndex(
      IndexOutput metaOut,
      IndexOutput indexOut,
      int countPerLeaf,
      BKDTreeLeafNodes leafNodes,
      long dataStartFP)
      throws IOException {
    byte[] packedIndex = packIndex(leafNodes);
    writeIndex(metaOut, indexOut, countPerLeaf, leafNodes.numLeaves(), packedIndex, dataStartFP);
  }

  private void writeIndex(
      IndexOutput metaOut,
      IndexOutput indexOut,
      int countPerLeaf,
      int numLeaves,
      byte[] packedIndex,
      long dataStartFP)
      throws IOException {
    CodecUtil.writeHeader(metaOut, CODEC_NAME, VERSION_CURRENT);
    metaOut.writeVInt(config.numDims);
    metaOut.writeVInt(config.numIndexDims);
    metaOut.writeVInt(countPerLeaf);
    metaOut.writeVInt(config.bytesPerDim);

    assert numLeaves > 0;
    metaOut.writeVInt(numLeaves);
    metaOut.writeBytes(minPackedValue, 0, config.packedIndexBytesLength);
    metaOut.writeBytes(maxPackedValue, 0, config.packedIndexBytesLength);

    metaOut.writeVLong(pointCount);
    metaOut.writeVInt(docsSeen.cardinality());
    metaOut.writeVInt(packedIndex.length);
    metaOut.writeLong(dataStartFP);
    // If metaOut and indexOut are the same file, we account for the fact that
    // writing a long makes the index start 8 bytes later.
    metaOut.writeLong(indexOut.getFilePointer() + (metaOut == indexOut ? Long.BYTES : 0));

    indexOut.writeBytes(packedIndex, 0, packedIndex.length);
  }

  private void writeLeafBlockDocs(DataOutput out, int[] docIDs, int start, int count)
      throws IOException {
    assert count > 0 : "config.maxPointsInLeafNode=" + config.maxPointsInLeafNode;
    out.writeVInt(count);
    DocIdsWriter.writeDocIds(docIDs, start, count, out);
  }

  private void writeLeafBlockPackedValues(
      DataOutput out,
      int[] commonPrefixLengths,
      int count,
      int sortedDim,
      IntFunction<BytesRef> packedValues,
      int leafCardinality)
      throws IOException {
    int prefixLenSum = Arrays.stream(commonPrefixLengths).sum();
    if (prefixLenSum == config.packedBytesLength) {
      // all values in this block are equal
      // 所有的point数据都相等，直接写个-1做标记
      out.writeByte((byte) -1);
    } else {
      assert commonPrefixLengths[sortedDim] < config.bytesPerDim;
      // estimate if storing the values with cardinality is cheaper than storing all values.

      // 每个point是从compressedByteOffset开始记录的
      int compressedByteOffset = sortedDim * config.bytesPerDim + commonPrefixLengths[sortedDim];
      int highCardinalityCost;
      int lowCardinalityCost;
      if (count == leafCardinality) {
        // all values in this block are different
        // 所有的值都不相等，直接用highCardinality方式进行存储，所以随便设个highCardinalityCost < lowCardinalityCost就行
        highCardinalityCost = 0;
        lowCardinalityCost = 1;
      } else {
        // compute cost of runLen compression
        int numRunLens = 0;
        for (int i = 0; i < count; ) {
          // do run-length compression on the byte at compressedByteOffset
          // runLen是从第i个point开始，suffix的第一个字节相同的point有多少个
          int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
          assert runLen <= 0xff;
          numRunLens++;
          i += runLen;
        }
        // Add cost of runLen compression
        // numRunLens就是下面例子图中，可以按suffix的第一个字节相同的分为多少组
        // 存储suffix占用的空间是：count * (config.packedBytesLength - prefixLenSum - 1)
        // 存储每一组个数需要： numRunLens个字节
        // 存储每一组中suffix的第一个字节需要： numRunLens个字节
        highCardinalityCost =
            count * (config.packedBytesLength - prefixLenSum - 1) + 2 * numRunLens;
        // +1 is the byte needed for storing the cardinality

        // leafCardinality就是我们下面例子图中，可以按值相同分为多少组
        // 每一组占用的空间是：suffix+count
        // suffix占用的字节数：config.packedBytesLength - prefixLenSum
        // count占用的字节数：1
        lowCardinalityCost = leafCardinality * (config.packedBytesLength - prefixLenSum + 1);
      }
      if (lowCardinalityCost <= highCardinalityCost) {
        out.writeByte((byte) -2);
        writeLowCardinalityLeafBlockPackedValues(out, commonPrefixLengths, count, packedValues);
      } else {
        out.writeByte((byte) sortedDim);
        writeHighCardinalityLeafBlockPackedValues(
            out, commonPrefixLengths, count, sortedDim, packedValues, compressedByteOffset);
      }
    }
  }

  private void writeLowCardinalityLeafBlockPackedValues(
      DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues)
      throws IOException {
    if (config.numIndexDims != 1) {// 记录每一维的最小值和最大值
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    }

    // 获取叶子节点中的第一个point
    BytesRef value = packedValues.apply(0);

    // 把第一个point拷贝到scratch1中，作为用来判断后续值是否相等的key
    System.arraycopy(value.bytes, value.offset, scratch1, 0, config.packedBytesLength);
    int cardinality = 1;
    for (int i = 1; i < count; i++) {// 遍历所有的point
      value = packedValues.apply(i);
      for (int dim = 0; dim < config.numDims; dim++) {// 遍历每一维
        final int start = dim * config.bytesPerDim;
        if (equalsPredicate.test(value.bytes, value.offset + start, scratch1, start) == false) {
          // 记录有多少个值相等
          out.writeVInt(cardinality);
          for (int j = 0; j < config.numDims; j++) {// 记录每一维除了公共前缀之外的其他数据
            out.writeBytes(
                scratch1,
                j * config.bytesPerDim + commonPrefixLengths[j],
                config.bytesPerDim - commonPrefixLengths[j]);
          }
          // 把当前值拷贝到scratch1中，作为下一轮对比的key
          System.arraycopy(value.bytes, value.offset, scratch1, 0, config.packedBytesLength);
          // 重置相等值的统计
          cardinality = 1;
          break;
        } else if (dim == config.numDims - 1) {
          cardinality++;
        }
      }
    }

    // 处理最后一批值相等的数据
    out.writeVInt(cardinality);
    for (int i = 0; i < config.numDims; i++) {
      out.writeBytes(
          scratch1,
          i * config.bytesPerDim + commonPrefixLengths[i],
          config.bytesPerDim - commonPrefixLengths[i]);
    }
  }

  private void writeHighCardinalityLeafBlockPackedValues(
      DataOutput out,
      int[] commonPrefixLengths,
      int count,
      int sortedDim,
      IntFunction<BytesRef> packedValues,
      int compressedByteOffset)
      throws IOException {
    if (config.numIndexDims != 1) {// 记录每一维的最小值和最大值
      writeActualBounds(out, commonPrefixLengths, count, packedValues);
    }
    commonPrefixLengths[sortedDim]++;
    for (int i = 0; i < count; ) {
      // do run-length compression on the byte at compressedByteOffset
      // 计算从i开始，总共有几个数据除了公共前缀之外的第一个字节是相等的
      int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
      assert runLen <= 0xff;
      BytesRef first = packedValues.apply(i);
      byte prefixByte = first.bytes[first.offset + compressedByteOffset];
      // 记录前缀
      out.writeByte(prefixByte);
      // 记录个数
      out.writeByte((byte) runLen);
      // 记录除了前缀剩下的数据
      writeLeafBlockPackedValuesRange(out, commonPrefixLengths, i, i + runLen, packedValues);
      i += runLen;
      assert i <= count;
    }
  }

  private void writeActualBounds(
      DataOutput out, int[] commonPrefixLengths, int count, IntFunction<BytesRef> packedValues)
      throws IOException {
    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      int commonPrefixLength = commonPrefixLengths[dim];
      int suffixLength = config.bytesPerDim - commonPrefixLength;
      if (suffixLength > 0) {
        BytesRef[] minMax =
            computeMinMax(
                count, packedValues, dim * config.bytesPerDim + commonPrefixLength, suffixLength);
        BytesRef min = minMax[0];
        BytesRef max = minMax[1];
        out.writeBytes(min.bytes, min.offset, min.length);
        out.writeBytes(max.bytes, max.offset, max.length);
      }
    }
  }

  /**
   * Return an array that contains the min and max values for the [offset, offset+length] interval
   * of the given {@link BytesRef}s.
   */
  private static BytesRef[] computeMinMax(
      int count, IntFunction<BytesRef> packedValues, int offset, int length) {
    assert length > 0;
    BytesRefBuilder min = new BytesRefBuilder();
    BytesRefBuilder max = new BytesRefBuilder();
    BytesRef first = packedValues.apply(0);
    min.copyBytes(first.bytes, first.offset + offset, length);
    max.copyBytes(first.bytes, first.offset + offset, length);
    for (int i = 1; i < count; ++i) {
      BytesRef candidate = packedValues.apply(i);
      if (Arrays.compareUnsigned(
              min.bytes(),
              0,
              length,
              candidate.bytes,
              candidate.offset + offset,
              candidate.offset + offset + length)
          > 0) {
        min.copyBytes(candidate.bytes, candidate.offset + offset, length);
      } else if (Arrays.compareUnsigned(
              max.bytes(),
              0,
              length,
              candidate.bytes,
              candidate.offset + offset,
              candidate.offset + offset + length)
          < 0) {
        max.copyBytes(candidate.bytes, candidate.offset + offset, length);
      }
    }
    return new BytesRef[] {min.get(), max.get()};
  }

  private void writeLeafBlockPackedValuesRange(
      DataOutput out,
      int[] commonPrefixLengths,
      int start,
      int end,
      IntFunction<BytesRef> packedValues)
      throws IOException {
    for (int i = start; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      assert ref.length == config.packedBytesLength;

      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        out.writeBytes(
            ref.bytes, ref.offset + dim * config.bytesPerDim + prefix, config.bytesPerDim - prefix);
      }
    }
  }

  private static int runLen(
      IntFunction<BytesRef> packedValues, int start, int end, int byteOffset) {
    BytesRef first = packedValues.apply(start);
    byte b = first.bytes[first.offset + byteOffset];
    for (int i = start + 1; i < end; ++i) {
      BytesRef ref = packedValues.apply(i);
      byte b2 = ref.bytes[ref.offset + byteOffset];
      assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
      if (b != b2) {
        return i - start;
      }
    }
    return end - start;
  }

  private void writeCommonPrefixes(DataOutput out, int[] commonPrefixes, byte[] packedValue)
      throws IOException {
    for (int dim = 0; dim < config.numDims; dim++) {
      out.writeVInt(commonPrefixes[dim]);
      // System.out.println(commonPrefixes[dim] + " of " + config.bytesPerDim);
      out.writeBytes(packedValue, dim * config.bytesPerDim, commonPrefixes[dim]);
    }
  }

  @Override
  public void close() throws IOException {
    finished = true;
    if (tempInput != null) {
      // NOTE: this should only happen on exception, e.g. caller calls close w/o calling finish:
      try {
        tempInput.close();
      } finally {
        tempDir.deleteFile(tempInput.getName());
        tempInput = null;
      }
    }
  }

  /**
   * Called on exception, to check whether the checksum is also corrupt in this source, and add that
   * information (checksum matched or didn't) as a suppressed exception.
   */
  private Error verifyChecksum(Throwable priorException, PointWriter writer) throws IOException {
    assert priorException != null;

    // TODO: we could improve this, to always validate checksum as we recurse, if we shared left and
    // right reader after recursing to children, and possibly within recursed children,
    // since all together they make a single pass through the file.  But this is a sizable re-org,
    // and would mean leaving readers (IndexInputs) open for longer:
    if (writer instanceof OfflinePointWriter) {
      // We are reading from a temp file; go verify the checksum:
      String tempFileName = ((OfflinePointWriter) writer).name;
      if (tempDir.getCreatedFiles().contains(tempFileName)) {
        try (ChecksumIndexInput in = tempDir.openChecksumInput(tempFileName, IOContext.READONCE)) {
          CodecUtil.checkFooter(in, priorException);
        }
      }
    }

    // We are reading from heap; nothing to add:
    throw IOUtils.rethrowAlways(priorException);
  }

  /**
   * Pick the next dimension to split.
   *
   * @param minPackedValue the min values for all dimensions
   * @param maxPackedValue the max values for all dimensions
   * @param parentSplits how many times each dim has been split on the parent levels
   * @return the dimension to split
   */
  // minPackedValue:所有数据中的每一维的最小值
  // maxPackedValue:所有数据中的每一维的最大值
  // parentSplits:在上层中各个维度成为排序维度的次数
  protected int split(byte[] minPackedValue, byte[] maxPackedValue, int[] parentSplits) {
    // First look at whether there is a dimension that has split less than 2x less than
    // the dim that has most splits, and return it if there is such a dimension and it
    // does not only have equals values. This helps ensure all dimensions are indexed.
    // 第一种: 寻找排序的维度是用来排序的次数小于最多排序次数维度的一半，并且所有的值并不是都相等的维度，这种策略是为了确保如果BKD树比较高，所有的维度都有机会用于排序
    int maxNumSplits = 0;
    for (int numSplits : parentSplits) {// 寻找目前用来排序次数最多的维度
      maxNumSplits = Math.max(maxNumSplits, numSplits);
    }
    for (int dim = 0; dim < config.numIndexDims; ++dim) {
      final int offset = dim * config.bytesPerDim;
      if (parentSplits[dim] < maxNumSplits / 2
          && comparator.compare(minPackedValue, offset, maxPackedValue, offset) != 0) {
        // 如果某个维度用来排序的次数小于最多排序次数维度的一半，并且所有的值并不是都相等
        return dim;
      }
    }

    // Find which dim has the largest span so we can split on it:
    // 第二种：寻找最大值和最小值差距最大的维度
    int splitDim = -1;
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      NumericUtils.subtract(config.bytesPerDim, dim, maxPackedValue, minPackedValue, scratchDiff);
      // scratch1是临时变量，存储当前已找到的用户排序维度的最大值和最小值的差值
      if (splitDim == -1 || comparator.compare(scratchDiff, 0, scratch1, 0) > 0) {
        System.arraycopy(scratchDiff, 0, scratch1, 0, config.bytesPerDim);
        splitDim = dim;
      }
    }

    // System.out.println("SPLIT: " + splitDim);
    return splitDim;
  }

  /** Pull a partition back into heap once the point count is low enough while recursing. */
  private HeapPointWriter switchToHeap(PointWriter source) throws IOException {
    int count = Math.toIntExact(source.count());
    try (PointReader reader = source.getReader(0, source.count());
        HeapPointWriter writer = new HeapPointWriter(config, count)) {
      for (int i = 0; i < count; i++) {
        boolean hasNext = reader.next();
        assert hasNext;
        writer.append(reader.pointValue());
      }
      source.destroy();
      return writer;
    } catch (Throwable t) {
      throw verifyChecksum(t, source);
    }
  }

  /* Recursively reorders the provided reader and writes the bkd-tree on the fly; this method is used
   * when we are writing a new segment directly from IndexWriter's indexing buffer (MutablePointsReader). */
  private void build(
      int leavesOffset,// 从第几个叶子节点开始
      int numLeaves,// 要处理几个叶子节点
      MutablePointValues reader,// 所有的point数据
      int from,// 从第几个point开始
      int to, // 从第几个point结束
      IndexOutput out,
      byte[] minPackedValue,// 当前要处理范围内的所有point的各个维度的最小值
      byte[] maxPackedValue,// 当前要处理范围内的所有point的各个维度的最大值
      int[] parentSplits, // 每一维作为split的次数
      byte[] splitPackedValues,// split的value
      byte[] splitDimensionValues,// split的dim
      long[] leafBlockFPs,
      int[] spareDocIds)
      throws IOException {

    if (numLeaves == 1) { // 叶子节点
      // leaf node
      final int count = to - from;
      assert count <= config.maxPointsInLeafNode;

      // Compute common prefixes
      // 计算每一维的公共前缀
      Arrays.fill(commonPrefixLengths, config.bytesPerDim);
      reader.getValue(from, scratchBytesRef1);
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, scratchBytesRef2);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int offset = dim * config.bytesPerDim;
          int dimensionPrefixLength = commonPrefixLengths[dim];
          commonPrefixLengths[dim] =
              Math.min(
                  dimensionPrefixLength,
                  commonPrefixComparator.compare(
                      scratchBytesRef1.bytes,
                      scratchBytesRef1.offset + offset,
                      scratchBytesRef2.bytes,
                      scratchBytesRef2.offset + offset));
        }
      }

      // Find the dimension that has the least number of unique bytes at commonPrefixLengths[dim]
      // 寻找每一维后缀的第一个字节，不一样值最少的dim
      // 这里应该是为了高效排序用的，我猜的，没有深入去看排序的逻辑
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      for (int i = from + 1; i < to; ++i) {
        for (int dim = 0; dim < config.numDims; dim++) {
          if (usedBytes[dim] != null) {
            byte b = reader.getByteAt(i, dim * config.bytesPerDim + commonPrefixLengths[dim]);
            usedBytes[dim].set(Byte.toUnsignedInt(b));
          }
        }
      }
      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (usedBytes[dim] != null) {
          final int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort by sortedDim
      // 按sortedDim排序
      MutablePointsReaderUtils.sortByDim(
          config,
          sortedDim,
          commonPrefixLengths,
          reader,
          from,
          to,
          scratchBytesRef1,
          scratchBytesRef2);

      // 统计叶子节点中有多少个不一样的值
      BytesRef comparator = scratchBytesRef1;
      BytesRef collector = scratchBytesRef2;
      reader.getValue(from, comparator);
      int leafCardinality = 1;
      for (int i = from + 1; i < to; ++i) {
        reader.getValue(i, collector);
        for (int dim = 0; dim < config.numDims; dim++) {
          final int start = dim * config.bytesPerDim;
          if (equalsPredicate.test(
                  collector.bytes,
                  collector.offset + start,
                  comparator.bytes,
                  comparator.offset + start)
              == false) {
            leafCardinality++;
            BytesRef scratch = collector;
            collector = comparator;
            comparator = scratch;
            break;
          }
        }
      }

      // Save the block file pointer:
      // 记录叶子节点在kdd中的起始位置
      leafBlockFPs[leavesOffset] = out.getFilePointer();

      // Write doc IDs
      // 记录docID
      int[] docIDs = spareDocIds;
      for (int i = from; i < to; ++i) {
        docIDs[i - from] = reader.getDocID(i);
      }
      // System.out.println("writeLeafBlock pos=" + out.getFilePointer());

      writeLeafBlockDocs(out, docIDs, 0, count);

      // Write the common prefixes:
      // 记录前缀
      reader.getValue(from, scratchBytesRef1);
      System.arraycopy(
          scratchBytesRef1.bytes, scratchBytesRef1.offset, scratch1, 0, config.packedBytesLength);
      writeCommonPrefixes(out, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues =
          new IntFunction<BytesRef>() {
            @Override
            public BytesRef apply(int i) {
              reader.getValue(from + i, scratchBytesRef1);
              return scratchBytesRef1;
            }
          };
      assert valuesInOrderAndBounds(
          config, count, sortedDim, minPackedValue, maxPackedValue, packedValues, docIDs, 0);

      // 持久化叶子节点的point数据，和前面一维数据的处理相同
      writeLeafBlockPackedValues(
          out, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);
    } else {// 内部节点
      // inner node

      final int splitDim;
      // compute the split dimension and partition around it
      if (config.numIndexDims == 1) {
        // 分割左右子树的维度
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the
        // algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the
        // bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (numLeaves != leafBlockFPs.length
            && config.numIndexDims > 2
            && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          // 计算当前的所有的point的各个维度的最大值和最小值
          computePackedValueBounds(
              reader, from, to, minPackedValue, maxPackedValue, scratchBytesRef1);
        }
        // 寻找的策略有两种，具体方法我们前面介绍过了
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      // How many leaves will be in the left tree:
      // 计算左子树的叶子节点个数，方法我们前面介绍过了
      int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);

      // How many points will be in the left tree:
      // 左子树中有多少个point
      final int mid = from + numLeftLeafNodes * config.maxPointsInLeafNode;

      // 每个维度的最小值和最大值的最长公共前缀
      final int commonPrefixLen =
          commonPrefixComparator.compare(
              minPackedValue,
              splitDim * config.bytesPerDim,
              maxPackedValue,
              splitDim * config.bytesPerDim);

      // 分裂左右子树
      MutablePointsReaderUtils.partition(
          config,
          maxDoc,
          splitDim,
          commonPrefixLen,
          reader,
          from,
          to,
          mid,
          scratchBytesRef1,
          scratchBytesRef2);

      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitOffset = rightOffset - 1;

      // set the split value
      // 分裂的value在splitPackedValues的位置
      final int address = splitOffset * config.bytesPerDim;

      // 记录分裂的维度
      splitDimensionValues[splitOffset] = (byte) splitDim;
      reader.getValue(mid, scratchBytesRef1);

      // 记录分裂的value
      System.arraycopy(
          scratchBytesRef1.bytes,
          scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          splitPackedValues,
          address,
          config.bytesPerDim);

      // 更新左右子树各个维度的最大值和最小值
      byte[] minSplitPackedValue =
          ArrayUtil.copyOfSubArray(minPackedValue, 0, config.packedIndexBytesLength);
      byte[] maxSplitPackedValue =
          ArrayUtil.copyOfSubArray(maxPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(
          scratchBytesRef1.bytes,
          scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          minSplitPackedValue,
          splitDim * config.bytesPerDim,
          config.bytesPerDim);
      System.arraycopy(
          scratchBytesRef1.bytes,
          scratchBytesRef1.offset + splitDim * config.bytesPerDim,
          maxSplitPackedValue,
          splitDim * config.bytesPerDim,
          config.bytesPerDim);

      // recurse
      // 递归处理
      parentSplits[splitDim]++;
      build(
          leavesOffset,
          numLeftLeafNodes,
          reader,
          from,
          mid,
          out,
          minPackedValue,
          maxSplitPackedValue,
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          spareDocIds);
      build(
          rightOffset,
          numLeaves - numLeftLeafNodes,
          reader,
          mid,
          to,
          out,
          minSplitPackedValue,
          maxPackedValue,
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          spareDocIds);
      parentSplits[splitDim]--;
    }
  }

  private void computePackedValueBounds(
      BKDRadixSelector.PathSlice slice, byte[] minPackedValue, byte[] maxPackedValue)
      throws IOException {
    try (PointReader reader = slice.writer.getReader(slice.start, slice.count)) {
      if (reader.next() == false) {
        return;
      }
      BytesRef value = reader.pointValue().packedValue();
      System.arraycopy(value.bytes, value.offset, minPackedValue, 0, config.packedIndexBytesLength);
      System.arraycopy(value.bytes, value.offset, maxPackedValue, 0, config.packedIndexBytesLength);
      while (reader.next()) {
        value = reader.pointValue().packedValue();
        for (int dim = 0; dim < config.numIndexDims; dim++) {
          final int startOffset = dim * config.bytesPerDim;
          if (comparator.compare(
                  value.bytes, value.offset + startOffset, minPackedValue, startOffset)
              < 0) {
            System.arraycopy(
                value.bytes,
                value.offset + startOffset,
                minPackedValue,
                startOffset,
                config.bytesPerDim);
          } else if (comparator.compare(
                  value.bytes, value.offset + startOffset, maxPackedValue, startOffset)
              > 0) {
            System.arraycopy(
                value.bytes,
                value.offset + startOffset,
                maxPackedValue,
                startOffset,
                config.bytesPerDim);
          }
        }
      }
    }
  }

  /**
   * The point writer contains the data that is going to be splitted using radix selection. /* This
   * method is used when we are merging previously written segments, in the numDims > 1 case.
   */
  private void build(
      int leavesOffset,
      int numLeaves,
      BKDRadixSelector.PathSlice points,
      IndexOutput out,
      BKDRadixSelector radixSelector,
      byte[] minPackedValue,
      byte[] maxPackedValue,
      int[] parentSplits,
      byte[] splitPackedValues,
      byte[] splitDimensionValues,
      long[] leafBlockFPs,
      int[] spareDocIds)
      throws IOException {

    if (numLeaves == 1) {

      // Leaf node: write block
      // We can write the block in any order so by default we write it sorted by the dimension that
      // has the
      // least number of unique bytes at commonPrefixLengths[dim], which makes compression more
      // efficient
      HeapPointWriter heapSource;
      if (points.writer instanceof HeapPointWriter == false) {
        // Adversarial cases can cause this, e.g. merging big segments with most of the points
        // deleted
        heapSource = switchToHeap(points.writer);
      } else {
        heapSource = (HeapPointWriter) points.writer;
      }

      int from = Math.toIntExact(points.start);
      int to = Math.toIntExact(points.start + points.count);
      // we store common prefix on scratch1
      computeCommonPrefixLength(heapSource, scratch1, from, to);

      int sortedDim = 0;
      int sortedDimCardinality = Integer.MAX_VALUE;
      FixedBitSet[] usedBytes = new FixedBitSet[config.numDims];
      for (int dim = 0; dim < config.numDims; ++dim) {
        if (commonPrefixLengths[dim] < config.bytesPerDim) {
          usedBytes[dim] = new FixedBitSet(256);
        }
      }
      // Find the dimension to compress
      for (int dim = 0; dim < config.numDims; dim++) {
        int prefix = commonPrefixLengths[dim];
        if (prefix < config.bytesPerDim) {
          int offset = dim * config.bytesPerDim;
          for (int i = from; i < to; ++i) {
            PointValue value = heapSource.getPackedValueSlice(i);
            BytesRef packedValue = value.packedValue();
            int bucket = packedValue.bytes[packedValue.offset + offset + prefix] & 0xff;
            usedBytes[dim].set(bucket);
          }
          int cardinality = usedBytes[dim].cardinality();
          if (cardinality < sortedDimCardinality) {
            sortedDim = dim;
            sortedDimCardinality = cardinality;
          }
        }
      }

      // sort the chosen dimension
      radixSelector.heapRadixSort(heapSource, from, to, sortedDim, commonPrefixLengths[sortedDim]);
      // compute cardinality
      int leafCardinality = heapSource.computeCardinality(from, to, commonPrefixLengths);

      // Save the block file pointer:
      leafBlockFPs[leavesOffset] = out.getFilePointer();
      // System.out.println("  write leaf block @ fp=" + out.getFilePointer());

      // Write docIDs first, as their own chunk, so that at intersect time we can add all docIDs w/o
      // loading the values:
      int count = to - from;
      assert count > 0 : "numLeaves=" + numLeaves + " leavesOffset=" + leavesOffset;
      assert count <= spareDocIds.length : "count=" + count + " > length=" + spareDocIds.length;
      // Write doc IDs
      int[] docIDs = spareDocIds;
      for (int i = 0; i < count; i++) {
        docIDs[i] = heapSource.getPackedValueSlice(from + i).docID();
      }
      writeLeafBlockDocs(out, docIDs, 0, count);

      // TODO: minor opto: we don't really have to write the actual common prefixes, because
      // BKDReader on recursing can regenerate it for us
      // from the index, much like how terms dict does so from the FST:

      // Write the common prefixes:
      writeCommonPrefixes(out, commonPrefixLengths, scratch1);

      // Write the full values:
      IntFunction<BytesRef> packedValues =
          new IntFunction<BytesRef>() {
            final BytesRef scratch = new BytesRef();

            {
              scratch.length = config.packedBytesLength;
            }

            @Override
            public BytesRef apply(int i) {
              PointValue value = heapSource.getPackedValueSlice(from + i);
              return value.packedValue();
            }
          };
      assert valuesInOrderAndBounds(
          config, count, sortedDim, minPackedValue, maxPackedValue, packedValues, docIDs, 0);
      writeLeafBlockPackedValues(
          out, commonPrefixLengths, count, sortedDim, packedValues, leafCardinality);

    } else {
      // Inner node: partition/recurse

      final int splitDim;
      if (config.numIndexDims == 1) {
        splitDim = 0;
      } else {
        // for dimensions > 2 we recompute the bounds for the current inner node to help the
        // algorithm choose best
        // split dimensions. Because it is an expensive operation, the frequency we recompute the
        // bounds is given
        // by SPLITS_BEFORE_EXACT_BOUNDS.
        if (numLeaves != leafBlockFPs.length
            && config.numIndexDims > 2
            && Arrays.stream(parentSplits).sum() % SPLITS_BEFORE_EXACT_BOUNDS == 0) {
          computePackedValueBounds(points, minPackedValue, maxPackedValue);
        }
        splitDim = split(minPackedValue, maxPackedValue, parentSplits);
      }

      assert numLeaves <= leafBlockFPs.length
          : "numLeaves=" + numLeaves + " leafBlockFPs.length=" + leafBlockFPs.length;

      // How many leaves will be in the left tree:
      final int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
      // How many points will be in the left tree:
      final long leftCount = numLeftLeafNodes * config.maxPointsInLeafNode;

      BKDRadixSelector.PathSlice[] slices = new BKDRadixSelector.PathSlice[2];

      final int commonPrefixLen =
          commonPrefixComparator.compare(
              minPackedValue,
              splitDim * config.bytesPerDim,
              maxPackedValue,
              splitDim * config.bytesPerDim);

      byte[] splitValue =
          radixSelector.select(
              points,
              slices,
              points.start,
              points.start + points.count,
              points.start + leftCount,
              splitDim,
              commonPrefixLen);

      final int rightOffset = leavesOffset + numLeftLeafNodes;
      final int splitValueOffset = rightOffset - 1;

      splitDimensionValues[splitValueOffset] = (byte) splitDim;
      int address = splitValueOffset * config.bytesPerDim;
      System.arraycopy(splitValue, 0, splitPackedValues, address, config.bytesPerDim);

      byte[] minSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(minPackedValue, 0, minSplitPackedValue, 0, config.packedIndexBytesLength);

      byte[] maxSplitPackedValue = new byte[config.packedIndexBytesLength];
      System.arraycopy(maxPackedValue, 0, maxSplitPackedValue, 0, config.packedIndexBytesLength);

      System.arraycopy(
          splitValue, 0, minSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);
      System.arraycopy(
          splitValue, 0, maxSplitPackedValue, splitDim * config.bytesPerDim, config.bytesPerDim);

      parentSplits[splitDim]++;
      // Recurse on left tree:
      build(
          leavesOffset,
          numLeftLeafNodes,
          slices[0],
          out,
          radixSelector,
          minPackedValue,
          maxSplitPackedValue,
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          spareDocIds);

      // Recurse on right tree:
      build(
          rightOffset,
          numLeaves - numLeftLeafNodes,
          slices[1],
          out,
          radixSelector,
          minSplitPackedValue,
          maxPackedValue,
          parentSplits,
          splitPackedValues,
          splitDimensionValues,
          leafBlockFPs,
          spareDocIds);

      parentSplits[splitDim]--;
    }
  }

  private void computeCommonPrefixLength(
      HeapPointWriter heapPointWriter, byte[] commonPrefix, int from, int to) {
    Arrays.fill(commonPrefixLengths, config.bytesPerDim);
    PointValue value = heapPointWriter.getPackedValueSlice(from);
    BytesRef packedValue = value.packedValue();
    for (int dim = 0; dim < config.numDims; dim++) {
      System.arraycopy(
          packedValue.bytes,
          packedValue.offset + dim * config.bytesPerDim,
          commonPrefix,
          dim * config.bytesPerDim,
          config.bytesPerDim);
    }
    for (int i = from + 1; i < to; i++) {
      value = heapPointWriter.getPackedValueSlice(i);
      packedValue = value.packedValue();
      for (int dim = 0; dim < config.numDims; dim++) {
        if (commonPrefixLengths[dim] != 0) {
          commonPrefixLengths[dim] =
              Math.min(
                  commonPrefixLengths[dim],
                  commonPrefixComparator.compare(
                      commonPrefix,
                      dim * config.bytesPerDim,
                      packedValue.bytes,
                      packedValue.offset + dim * config.bytesPerDim));
        }
      }
    }
  }

  // only called from assert
  private static boolean valuesInOrderAndBounds(
      BKDConfig config,
      int count,
      int sortedDim,
      byte[] minPackedValue,
      byte[] maxPackedValue,
      IntFunction<BytesRef> values,
      int[] docs,
      int docsOffset) {
    byte[] lastPackedValue = new byte[config.packedBytesLength];
    int lastDoc = -1;
    for (int i = 0; i < count; i++) {
      BytesRef packedValue = values.apply(i);
      assert packedValue.length == config.packedBytesLength;
      assert valueInOrder(
          config,
          i,
          sortedDim,
          lastPackedValue,
          packedValue.bytes,
          packedValue.offset,
          docs[docsOffset + i],
          lastDoc);
      lastDoc = docs[docsOffset + i];

      // Make sure this value does in fact fall within this leaf cell:
      assert valueInBounds(config, packedValue, minPackedValue, maxPackedValue);
    }
    return true;
  }

  // only called from assert
  private static boolean valueInOrder(
      BKDConfig config,
      long ord,
      int sortedDim,
      byte[] lastPackedValue,
      byte[] packedValue,
      int packedValueOffset,
      int doc,
      int lastDoc) {
    int dimOffset = sortedDim * config.bytesPerDim;
    if (ord > 0) {
      int cmp =
          Arrays.compareUnsigned(
              lastPackedValue,
              dimOffset,
              dimOffset + config.bytesPerDim,
              packedValue,
              packedValueOffset + dimOffset,
              packedValueOffset + dimOffset + config.bytesPerDim);
      if (cmp > 0) {
        throw new AssertionError(
            "values out of order: last value="
                + new BytesRef(lastPackedValue)
                + " current value="
                + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength)
                + " ord="
                + ord);
      }
      if (cmp == 0 && config.numDims > config.numIndexDims) {
        cmp =
            Arrays.compareUnsigned(
                lastPackedValue,
                config.packedIndexBytesLength,
                config.packedBytesLength,
                packedValue,
                packedValueOffset + config.packedIndexBytesLength,
                packedValueOffset + config.packedBytesLength);
        if (cmp > 0) {
          throw new AssertionError(
              "data values out of order: last value="
                  + new BytesRef(lastPackedValue)
                  + " current value="
                  + new BytesRef(packedValue, packedValueOffset, config.packedBytesLength)
                  + " ord="
                  + ord);
        }
      }
      if (cmp == 0 && doc < lastDoc) {
        throw new AssertionError(
            "docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
      }
    }
    System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, config.packedBytesLength);
    return true;
  }

  // only called from assert
  private static boolean valueInBounds(
      BKDConfig config, BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue) {
    for (int dim = 0; dim < config.numIndexDims; dim++) {
      int offset = config.bytesPerDim * dim;
      if (Arrays.compareUnsigned(
              packedValue.bytes,
              packedValue.offset + offset,
              packedValue.offset + offset + config.bytesPerDim,
              minPackedValue,
              offset,
              offset + config.bytesPerDim)
          < 0) {
        return false;
      }
      if (Arrays.compareUnsigned(
              packedValue.bytes,
              packedValue.offset + offset,
              packedValue.offset + offset + config.bytesPerDim,
              maxPackedValue,
              offset,
              offset + config.bytesPerDim)
          > 0) {
        return false;
      }
    }

    return true;
  }
}
