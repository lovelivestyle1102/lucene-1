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
package org.apache.lucene.util.fst;

import static org.apache.lucene.util.fst.FST.Arc.BitTable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: break this into WritableFST and ReadOnlyFST.. then
// we can have subclasses of ReadOnlyFST to handle the
// different byte[] level encodings (packed or
// not)... and things like nodeCount, arcCount are read only

// TODO: if FST is pure prefix trie we can do a more compact
// job, ie, once we are at a 'suffix only', just store the
// completion labels as a string not as a series of arcs.

// NOTE: while the FST is able to represent a non-final
// dead-end state (NON_FINAL_END_NODE=0), the layers above
// (FSTEnum, Util) have problems with this!!

/**
 * Represents an finite state machine (FST), using a compact byte[] format.
 *
 * <p>The format is similar to what's used by Morfologik
 * (https://github.com/morfologik/morfologik-stemming).
 *
 * <p>See the {@link org.apache.lucene.util.fst package documentation} for some simple examples.
 *
 * @lucene.experimental
 */
public final class FST<T> implements Accountable {

  /** Specifies allowed range of each int input label for this FST. */
  public enum INPUT_TYPE {
    BYTE1,
    BYTE2,
    BYTE4
  }

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(FST.class);

  //arc对应的label是某个term的最后一个字符
  private static final int BIT_FINAL_ARC = 1 << 0;

  //arc是Node节点中的最后一个Arc，上文中我们说到一个UnCompiledNode状态的Node可以包含多个arc
  static final int BIT_LAST_ARC = 1 << 1;

  //上一个由状态UnCompiledNode转为CompiledNode状态的Node是当前arc的target节点, 它实际是用来描述当前的arc中的label不是输入值的最后一个字符
  static final int BIT_TARGET_NEXT = 1 << 2;

  // TODO: we can free up a bit if we can nuke this:
  // arc的target是一个终止节点
  private static final int BIT_STOP_NODE = 1 << 3;

  /** This flag is set if the arc has an output. */
  //arc 是否有输出值
  public static final int BIT_ARC_HAS_OUTPUT = 1 << 4;

  //acr的目标节点有输出(final output)
  private static final int BIT_ARC_HAS_FINAL_OUTPUT = 1 << 5;

  /** Value of the arc flags to declare a node with fixed length arcs designed for binary search. */
  // We use this as a marker because this one flag is illegal by itself.
  public static final byte ARCS_FOR_BINARY_SEARCH = BIT_ARC_HAS_FINAL_OUTPUT;

  /**
   * Value of the arc flags to declare a node with fixed length arcs and bit table designed for
   * direct addressing.
   */
  static final byte ARCS_FOR_DIRECT_ADDRESSING = 1 << 6;

  /** @see #shouldExpandNodeWithFixedLengthArcs */
  static final int FIXED_LENGTH_ARC_SHALLOW_DEPTH = 3; // 0 => only root node.

  /** @see #shouldExpandNodeWithFixedLengthArcs */
  static final int FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS = 5;

  /** @see #shouldExpandNodeWithFixedLengthArcs */
  static final int FIXED_LENGTH_ARC_DEEP_NUM_ARCS = 10;

  /**
   * Maximum oversizing factor allowed for direct addressing compared to binary search when
   * expansion credits allow the oversizing. This factor prevents expansions that are obviously too
   * costly even if there are sufficient credits.
   *
   * @see #shouldExpandNodeWithDirectAddressing
   */
  private static final float DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR = 1.66f;

  // Increment version to change it
  private static final String FILE_FORMAT_NAME = "FST";

  private static final int VERSION_START = 6;

  private static final int VERSION_LITTLE_ENDIAN = 8;

  private static final int VERSION_CURRENT = VERSION_LITTLE_ENDIAN;

  // Never serialized; just used to represent the virtual
  // final node w/ no arcs:
  private static final long FINAL_END_NODE = -1;

  // Never serialized; just used to represent the virtual
  // non-final node w/ no arcs:
  private static final long NON_FINAL_END_NODE = 0;

  /** If arc has this label then that arc is final/accepted */
  public static final int END_LABEL = -1;

  final INPUT_TYPE inputType;

  // if non-null, this FST accepts the empty string and
  // produces this output
  T emptyOutput;

  /**
   * 构建的最终结果
   *
   * bytes相当于一个大的字节数组，提供正向遍历和反向遍历数组的方式，
   * bytes中存的是Node的Arc的内容，包括label，output，nextFinalOutput，target
   *
   * A {@link BytesStore}, used during building, or during reading when the FST is very large (more
   * than 1 GB). If the FST is less than 1 GB then bytesArray is set instead.
   */
  final BytesStore bytes;

  private final FSTStore fstStore;

  private long startNode = -1;

  public final Outputs<T> outputs;

  private final int version;

  /** Represents a single arc. */
  public static final class Arc<T> {

    // *** Arc fields.
    //输入值的字符
    private int label;

    //附属值或输出值
    private T output;

    //当前arc描述的字符不是输入值的最后一个字符时，会存储一个index值来指向下一个字符byte【】 current中的flag值在byte[] current的下标值
    private long target;

    /**
     * BIT_FINAL_ARC  arc对应字符是不是term最后一个字符
     * BIT_LAST_ARC   arc是不是当前节点最后一个出度
     * BIT_TARGET_NEXT 存储FST的二进制数组中紧邻的下一个字符区间数据是不是当前字符的下一个字符
     * BIT_ARC_HAS_OUTPUT arc是否有output值
     * BIT_ARC_FINAL_OUTPUT arc是否有final output值
     */
    private byte flags;

    private T nextFinalOutput;

    private long nextArc;

    private byte nodeFlags;

    // *** Fields for arcs belonging to a node with fixed length arcs.
    // So only valid when bytesPerArc != 0.
    // nodeFlags == ARCS_FOR_BINARY_SEARCH || nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

    private int bytesPerArc;

    private long posArcsStart;

    private int arcIdx;

    private int numArcs;

    // *** Fields for a direct addressing node. nodeFlags == ARCS_FOR_DIRECT_ADDRESSING.

    /**
     * Start position in the {@link FST.BytesReader} of the presence bits for a direct addressing
     * node, aka the bit-table
     */
    private long bitTableStart;

    /** First label of a direct addressing node. */
    private int firstLabel;

    /**
     * Index of the current label of a direct addressing node. While {@link #arcIdx} is the current
     * index in the label range, {@link #presenceIndex} is its corresponding index in the list of
     * actually present labels. It is equal to the number of bits set before the bit at {@link
     * #arcIdx} in the bit-table. This field is a cache to avoid to count bits set repeatedly when
     * iterating the next arcs.
     */
    private int presenceIndex;

    /**
     * 在Arc中，target是指Arc指向的Node，这个值可以是Node的ID，也就是ord，或者Node的Address
     *
     * Returns this
     * */
    public Arc<T> copyFrom(Arc<T> other) {
      label = other.label();
      target = other.target();
      flags = other.flags();
      output = other.output();
      nextFinalOutput = other.nextFinalOutput();
      nextArc = other.nextArc();
      nodeFlags = other.nodeFlags();
      bytesPerArc = other.bytesPerArc();

      // Fields for arcs belonging to a node with fixed length arcs.
      // We could avoid copying them if bytesPerArc() == 0 (this was the case with previous code,
      // and the current code
      // still supports that), but it may actually help external uses of FST to have consistent arc
      // state, and debugging
      // is easier.
      posArcsStart = other.posArcsStart();
      arcIdx = other.arcIdx();
      numArcs = other.numArcs();
      bitTableStart = other.bitTableStart;
      firstLabel = other.firstLabel();
      presenceIndex = other.presenceIndex;

      return this;
    }

    boolean flag(int flag) {
      return FST.flag(flags, flag);
    }

    public boolean isLast() {
      return flag(BIT_LAST_ARC);
    }

    public boolean isFinal() {
      return flag(BIT_FINAL_ARC);
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append(" target=").append(target());
      b.append(" label=0x").append(Integer.toHexString(label()));
      if (flag(BIT_FINAL_ARC)) {
        b.append(" final");
      }
      if (flag(BIT_LAST_ARC)) {
        b.append(" last");
      }
      if (flag(BIT_TARGET_NEXT)) {
        b.append(" targetNext");
      }
      if (flag(BIT_STOP_NODE)) {
        b.append(" stop");
      }
      if (flag(BIT_ARC_HAS_OUTPUT)) {
        b.append(" output=").append(output());
      }
      if (flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
        b.append(" nextFinalOutput=").append(nextFinalOutput());
      }
      if (bytesPerArc() != 0) {
        b.append(" arcArray(idx=")
            .append(arcIdx())
            .append(" of ")
            .append(numArcs())
            .append(")")
            .append("(")
            .append(nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING ? "da" : "bs")
            .append(")");
      }
      return b.toString();
    }

    public int label() {
      return label;
    }

    public T output() {
      return output;
    }

    /** Ord/address to target node. */
    public long target() {
      return target;
    }

    public byte flags() {
      return flags;
    }

    public T nextFinalOutput() {
      return nextFinalOutput;
    }

    /**
     * Address (into the byte[]) of the next arc - only for list of variable length arc. Or
     * ord/address to the next node if label == {@link #END_LABEL}.
     */
    long nextArc() {
      return nextArc;
    }

    /** Where we are in the array; only valid if bytesPerArc != 0. */
    public int arcIdx() {
      return arcIdx;
    }

    /**
     * Node header flags. Only meaningful to check if the value is either {@link
     * #ARCS_FOR_BINARY_SEARCH} or {@link #ARCS_FOR_DIRECT_ADDRESSING} (other value when bytesPerArc
     * == 0).
     */
    public byte nodeFlags() {
      return nodeFlags;
    }

    /** Where the first arc in the array starts; only valid if bytesPerArc != 0 */
    public long posArcsStart() {
      return posArcsStart;
    }

    /**
     * Non-zero if this arc is part of a node with fixed length arcs, which means all arcs for the
     * node are encoded with a fixed number of bytes so that we binary search or direct address. We
     * do when there are enough arcs leaving one node. It wastes some bytes but gives faster
     * lookups.
     */
    public int bytesPerArc() {
      return bytesPerArc;
    }

    /**
     * How many arcs; only valid if bytesPerArc != 0 (fixed length arcs). For a node designed for
     * binary search this is the array size. For a node designed for direct addressing, this is the
     * label range.
     */
    public int numArcs() {
      return numArcs;
    }

    /**
     * First label of a direct addressing node. Only valid if nodeFlags == {@link
     * #ARCS_FOR_DIRECT_ADDRESSING}.
     */
    int firstLabel() {
      return firstLabel;
    }

    /**
     * Helper methods to read the bit-table of a direct addressing node. Only valid for {@link Arc}
     * with {@link Arc#nodeFlags()} == {@code ARCS_FOR_DIRECT_ADDRESSING}.
     */
    static class BitTable {

      /** See {@link BitTableUtil#isBitSet(int, FST.BytesReader)}. */
      static boolean isBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.isBitSet(bitIndex, in);
      }

      /**
       * See {@link BitTableUtil#countBits(int, FST.BytesReader)}. The count of bit set is the
       * number of arcs of a direct addressing node.
       */
      static int countBits(Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.countBits(getNumPresenceBytes(arc.numArcs()), in);
      }

      /** See {@link BitTableUtil#countBitsUpTo(int, FST.BytesReader)}. */
      static int countBitsUpTo(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.countBitsUpTo(bitIndex, in);
      }

      /** See {@link BitTableUtil#nextBitSet(int, int, FST.BytesReader)}. */
      static int nextBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.nextBitSet(bitIndex, getNumPresenceBytes(arc.numArcs()), in);
      }

      /** See {@link BitTableUtil#previousBitSet(int, FST.BytesReader)}. */
      static int previousBitSet(int bitIndex, Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        in.setPosition(arc.bitTableStart);
        return BitTableUtil.previousBitSet(bitIndex, in);
      }

      /** Asserts the bit-table of the provided {@link Arc} is valid. */
      static boolean assertIsValid(Arc<?> arc, FST.BytesReader in) throws IOException {
        assert arc.bytesPerArc() > 0;
        assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
        // First bit must be set.
        assert isBitSet(0, arc, in);
        // Last bit must be set.
        assert isBitSet(arc.numArcs() - 1, arc, in);
        // No bit set after the last arc.
        assert nextBitSet(arc.numArcs() - 1, arc, in) == -1;
        return true;
      }
    }
  }

  private static boolean flag(int flags, int bit) {
    return (flags & bit) != 0;
  }

  // make a new empty FST, for building; Builder invokes this
  FST(INPUT_TYPE inputType, Outputs<T> outputs, int bytesPageBits) {
    this.inputType = inputType;

    this.outputs = outputs;

    fstStore = null;

    //结果
    bytes = new BytesStore(bytesPageBits);

    //写入的时候是顺序的，读取的时候是反向的
    // pad: ensure no node gets address 0 which is reserved to mean
    // the stop state w/ no arcs
    bytes.writeByte((byte) 0);

    emptyOutput = null;

    this.version = VERSION_CURRENT;
  }

  private static final int DEFAULT_MAX_BLOCK_BITS = Constants.JRE_IS_64BIT ? 30 : 28;

  /** Load a previously saved FST. */
  public FST(DataInput metaIn, DataInput in, Outputs<T> outputs) throws IOException {
    this(metaIn, in, outputs, new OnHeapFSTStore(DEFAULT_MAX_BLOCK_BITS));
  }

  /**
   * Load a previously saved FST; maxBlockBits allows you to control the size of the byte[] pages
   * used to hold the FST bytes.
   */
  public FST(DataInput metaIn, DataInput in, Outputs<T> outputs, FSTStore fstStore)
      throws IOException {
    bytes = null;

    this.fstStore = fstStore;

    this.outputs = outputs;

    // NOTE: only reads formats VERSION_START up to VERSION_CURRENT; we don't have
    // back-compat promise for FSTs (they are experimental), but we are sometimes able to offer it
    this.version = CodecUtil.checkHeader(metaIn, FILE_FORMAT_NAME, VERSION_START, VERSION_CURRENT);

    if (metaIn.readByte() == 1) {
      // accepts empty string
      // 1 KB blocks:
      BytesStore emptyBytes = new BytesStore(10);
      int numBytes = metaIn.readVInt();
      emptyBytes.copyBytes(metaIn, numBytes);

      // De-serialize empty-string output:
      BytesReader reader = emptyBytes.getReverseReader();
      // NoOutputs uses 0 bytes when writing its output,
      // so we have to check here else BytesStore gets
      // angry:
      if (numBytes > 0) {
        reader.setPosition(numBytes - 1);
      }
      emptyOutput = outputs.readFinalOutput(reader);
    } else {
      emptyOutput = null;
    }
    final byte t = metaIn.readByte();
    switch (t) {
      case 0:
        inputType = INPUT_TYPE.BYTE1;
        break;
      case 1:
        inputType = INPUT_TYPE.BYTE2;
        break;
      case 2:
        inputType = INPUT_TYPE.BYTE4;
        break;
      default:
        throw new CorruptIndexException("invalid input type " + t, in);
    }

    startNode = metaIn.readVLong();

    // 读取fst的信息，存储在fstStore中
    long numBytes = metaIn.readVLong();

    this.fstStore.init(in, numBytes);
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED;
    if (this.fstStore != null) {
      size += this.fstStore.ramBytesUsed();
    } else {
      size += bytes.ramBytesUsed();
    }

    return size;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(input=" + inputType + ",output=" + outputs;
  }

  void finish(long newStartNode) throws IOException {
    assert newStartNode <= bytes.getPosition();
    if (startNode != -1) {
      throw new IllegalStateException("already finished");
    }

    if (newStartNode == FINAL_END_NODE && emptyOutput != null) {
      newStartNode = 0;
    }

    startNode = newStartNode;

    bytes.finish();
  }

  public T getEmptyOutput() {
    return emptyOutput;
  }

  void setEmptyOutput(T v) {
    if (emptyOutput != null) {
      emptyOutput = outputs.merge(emptyOutput, v);
    } else {
      emptyOutput = v;
    }
  }

  public void save(DataOutput metaOut, DataOutput out) throws IOException {
    // startNode如果是-1表示还没有完成构建，拒绝持久化半成品
    if (startNode == -1) {
      throw new IllegalStateException("call finish first");
    }

    // 持久化头部
    CodecUtil.writeHeader(metaOut, FILE_FORMAT_NAME, VERSION_CURRENT);

    // TODO: really we should encode this as an arc, arriving
    // to the root node, instead of special casing here:
    if (emptyOutput != null) {// 如果存在空输入
      // Accepts empty string
      // 写个1表示有空输入
      metaOut.writeByte((byte) 1);

      // Serialize empty-string output:
      ByteBuffersDataOutput ros = new ByteBuffersDataOutput();

      outputs.writeFinalOutput(emptyOutput, ros);

      byte[] emptyOutputBytes = ros.toArrayCopy();

      int emptyLen = emptyOutputBytes.length;

      // reverse
      // 如果输出是多字节，则是逆序存储
      final int stopAt = emptyLen / 2;

      int upto = 0;
      while (upto < stopAt) {
        final byte b = emptyOutputBytes[upto];

        emptyOutputBytes[upto] = emptyOutputBytes[emptyLen - upto - 1];

        emptyOutputBytes[emptyLen - upto - 1] = b;

        upto++;
      }

      metaOut.writeVInt(emptyLen);

      metaOut.writeBytes(emptyOutputBytes, 0, emptyLen);
    } else { // 0表示不存在空输入
      metaOut.writeByte((byte) 0);
    }
    final byte t;
    if (inputType == INPUT_TYPE.BYTE1) {
      t = 0;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      t = 1;
    } else {
      t = 2;
    }

    // 存储输入的label的类型
    metaOut.writeByte(t);

    // root flag的位置
    metaOut.writeVLong(startNode);

    // 本fst是来自输入构建的
    if (bytes != null) {
      long numBytes = bytes.getPosition();
      metaOut.writeVLong(numBytes);
      bytes.writeTo(out);
    } else {
      // 本fst是来自其他已有的fst
      assert fstStore != null;
      fstStore.writeTo(out);
    }
  }

  /** Writes an automaton to a file. */
  public void save(final Path path) throws IOException {
    try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
      DataOutput out = new OutputStreamDataOutput(os);
      save(out, out);
    }
  }

  /** Reads an automaton from a file. */
  public static <T> FST<T> read(Path path, Outputs<T> outputs) throws IOException {
    try (InputStream is = Files.newInputStream(path)) {
      DataInput in = new InputStreamDataInput(new BufferedInputStream(is));
      return new FST<>(in, in, outputs);
    }
  }

  private void writeLabel(DataOutput out, int v) throws IOException {
    assert v >= 0 : "v=" + v;
    if (inputType == INPUT_TYPE.BYTE1) {
      assert v <= 255 : "v=" + v;
      out.writeByte((byte) v);
    } else if (inputType == INPUT_TYPE.BYTE2) {
      assert v <= 65535 : "v=" + v;
      out.writeShort((short) v);
    } else {
      out.writeVInt(v);
    }
  }

  /** Reads one BYTE1/2/4 label from the provided {@link DataInput}. */
  public int readLabel(DataInput in) throws IOException {
    final int v;
    if (inputType == INPUT_TYPE.BYTE1) {
      // Unsigned byte:
      v = in.readByte() & 0xFF;
    } else if (inputType == INPUT_TYPE.BYTE2) {
      // Unsigned short:
      if (version < VERSION_LITTLE_ENDIAN) {
        v = Short.reverseBytes(in.readShort()) & 0xFFFF;
      } else {
        v = in.readShort() & 0xFFFF;
      }
    } else {
      v = in.readVInt();
    }
    return v;
  }

  /** returns true if the node at this address has any outgoing arcs */
  public static <T> boolean targetHasArcs(Arc<T> arc) {
    return arc.target() > 0;
  }

  // serializes new node by appending its bytes to the end
  // of the current byte[]
  //先看下构建空的FST。addNode函数把Builder中的UnCompiledNode写入FST的bytes对象，
  // 写入的这个Node的ID就是nodeCount，这里把Node的Arc顺序写到字节数组中，
  // 由于FST的Builder是按照label的字典序调用构建的FST，所以这里的Arc也是按照label排序的
  long addNode(FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> nodeIn)
      throws IOException {
    T NO_OUTPUT = outputs.getNoOutput();

    // System.out.println("FST.addNode pos=" + bytes.getPosition() + " numArcs=" + nodeIn.numArcs);
    //如果是没有Arc的空节点
    if (nodeIn.numArcs == 0) {// 没有出边
      if (nodeIn.isFinal) {// 如果是终结节点，不带arc的终结节点并没有真正存储，所有指向该节点的arc在flag中做标记
        return FINAL_END_NODE;
      } else {
        return NON_FINAL_END_NODE;// 剪枝才会出现这种情况，不需要考虑
      }
    }

    // 当前node序列化存储的的起始位置
    final long startAddress = fstCompiler.bytes.getPosition();
    // System.out.println("  startAddr=" + startAddress);

    // 是否需要使用固定大小存储arc信息

    ///如果这里的doFixedArray是true，那么每个Arc都用一样的字节长度(后文都用长度代替)，
    // 由于有的Arc可能有output或nextFinalOutput，而有的却没有，所以每个Arc的长度可能是不一样。
    // FIXED_ARRAY方式下，每个Arc的存储长度都用最长的那个Arc的长度。
    //很明显，这种FIXED_ARRAY方式会导致Arc与Arc之间存在一些内存碎片，
    // 好处就是能快速定位Node下的第N个Arc的位置，所以在查询Node下是否有Arc的label匹配一个确定的值的时候，
    // 可以用二分查找，这样就是空间换了时间。
    //当然用这种模式也是有条件的，shouldExpand函数中给出了条件，比如Node下Arc数量要超过10，
    // Arc数量太少的话，二分的效果比起遍历没有优势，而且还浪费了一些内存，反而亏了
    final boolean doFixedLengthArcs = shouldExpandNodeWithFixedLengthArcs(fstCompiler, nodeIn);
    if (doFixedLengthArcs) {
      // System.out.println("  fixed length arcs");
      if (fstCompiler.numBytesPerArc.length < nodeIn.numArcs) {// 空间不足，扩容
        fstCompiler.numBytesPerArc = new int[ArrayUtil.oversize(nodeIn.numArcs, Integer.BYTES)];
        fstCompiler.numLabelBytesPerArc = new int[fstCompiler.numBytesPerArc.length];
      }
    }

    // 更新arc总数
    fstCompiler.arcCount += nodeIn.numArcs;

    // 最后一个arc的下标
    final int lastArc = nodeIn.numArcs - 1;

    // 当前处理的node中前一个arc序列化的起始位置，这个是用来计算每个arc的序列化长度
    long lastArcStart = fstCompiler.bytes.getPosition();

    // arc序列化长度最大值
    int maxBytesPerArc = 0;

    // arc扣除label之后的序列化长度的最大值
    int maxBytesPerArcWithoutLabel = 0;

    // 遍历处理每一个arc
    for (int arcIdx = 0; arcIdx < nodeIn.numArcs; arcIdx++) {
      //获取每一条边
      final FSTCompiler.Arc<T> arc = nodeIn.arcs[arcIdx];

      final FSTCompiler.CompiledNode target = (FSTCompiler.CompiledNode) arc.target;

      //初始化flags，标志位。利用最小化
      int flags = 0;
      // System.out.println("  arc " + arcIdx + " label=" + arc.label + " -> target=" +
      // target.node);

      // 如果是node的最后一个arc
      if (arcIdx == lastArc) {
        flags += BIT_LAST_ARC;
      }

      // 如果上一个序列化的node刚好是arc的target，并且不使用固定长度的存储方式。
      // 一定不能是固定长度的存储方式，因为固定长度的存储方式无法保证arc是相邻的，因为可能有填充空间。
      if (fstCompiler.lastFrozenNode == target.node && !doFixedLengthArcs) {
        // TODO: for better perf (but more RAM used) we
        // could avoid this except when arc is "near" the
        // last arc:
        flags += BIT_TARGET_NEXT;
      }

      if (arc.isFinal) {// 如果arc指向了终止节点
        flags += BIT_FINAL_ARC;
        if (arc.nextFinalOutput != NO_OUTPUT) {// 如果arc存在finaloutput
          flags += BIT_ARC_HAS_FINAL_OUTPUT;
        }
      } else {
        assert arc.nextFinalOutput == NO_OUTPUT;
      }

      // 只有终止节点的编号才是-1，也只有终止节点是没有arc的
      boolean targetHasArcs = target.node > 0;

      if (!targetHasArcs) {// 如果arc指向了终止节点
        flags += BIT_STOP_NODE;
      }

      if (arc.output != NO_OUTPUT) {// arc存在output
        flags += BIT_ARC_HAS_OUTPUT;
      }

      // 1. 首先写标志位flag
      fstCompiler.bytes.writeByte((byte) flags);

      // label序列化的起始位置，用来计算label大小的
      long labelStart = fstCompiler.bytes.getPosition();

      // 2. 写入label
      writeLabel(fstCompiler.bytes, arc.label);

      int numLabelBytes = (int) (fstCompiler.bytes.getPosition() - labelStart);

      // System.out.println("  write arc: label=" + (char) arc.label + " flags=" + flags + "
      // target=" + target.node + " pos=" + bytes.getPosition() + " output=" +
      // outputs.outputToString(arc.output));

      // 3.如果有output值的话
      if (arc.output != NO_OUTPUT) {
        outputs.write(arc.output, fstCompiler.bytes);
        // System.out.println("    write output");
      }

       // 4. 如果有finalOutput
      if (arc.nextFinalOutput != NO_OUTPUT) {
        // System.out.println("    write final output");
        outputs.writeFinalOutput(arc.nextFinalOutput, fstCompiler.bytes);
      }

      // 5.如果有target
      if (targetHasArcs && (flags & BIT_TARGET_NEXT) == 0) {
        assert target.node > 0;
        // System.out.println("    write target");
        fstCompiler.bytes.writeVLong(target.node);
      }

      // just write the arcs "like normal" on first pass, but record how many bytes each one took
      // and max byte size:
      if (doFixedLengthArcs) {// 如果要使用固定长度的方式存储arc，需要计算一些信息
        int numArcBytes = (int) (fstCompiler.bytes.getPosition() - lastArcStart);
        fstCompiler.numBytesPerArc[arcIdx] = numArcBytes;
        fstCompiler.numLabelBytesPerArc[arcIdx] = numLabelBytes;
        lastArcStart = fstCompiler.bytes.getPosition();
        maxBytesPerArc = Math.max(maxBytesPerArc, numArcBytes);
        maxBytesPerArcWithoutLabel =
            Math.max(maxBytesPerArcWithoutLabel, numArcBytes - numLabelBytes);
        // System.out.println("    arcBytes=" + numArcBytes + " labelBytes=" + numLabelBytes);
      }
    }

    // TODO: try to avoid wasteful cases: disable doFixedLengthArcs in that case
    /*
     *
     * LUCENE-4682: what is a fair heuristic here?
     * It could involve some of these:
     * 1. how "busy" the node is: nodeIn.inputCount relative to frontier[0].inputCount?
     * 2. how much binSearch saves over scan: nodeIn.numArcs
     * 3. waste: numBytes vs numBytesExpanded
     *
     * the one below just looks at #3
    if (doFixedLengthArcs) {
      // rough heuristic: make this 1.25 "waste factor" a parameter to the phd ctor????
      int numBytes = lastArcStart - startAddress;
      int numBytesExpanded = maxBytesPerArc * nodeIn.numArcs;
      if (numBytesExpanded > numBytes*1.25) {
        doFixedLengthArcs = false;
      }
    }
    */

    if (doFixedLengthArcs) {
      // 因为构建的输入是有序的，所以最后一个arc的label-第一个arc的label可以判断label的最大差值
      // 这个差值会用来算需要记录在这个差值范围内那些lablel存在的位图 （直接寻址使用）
      assert maxBytesPerArc > 0;
      // 2nd pass just "expands" all arcs to take up a fixed byte size

      int labelRange = nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label + 1;
      assert labelRange > 0;

      //  shouldExpandNodeWithDirectAddressing是根据直接寻址和二分法所需的空间做权衡决定使用哪种存储方式
      if (shouldExpandNodeWithDirectAddressing(
          fstCompiler, nodeIn, maxBytesPerArc, maxBytesPerArcWithoutLabel, labelRange)) {
        // 直接寻址的存储方式
        writeNodeForDirectAddressing(
            fstCompiler, nodeIn, startAddress, maxBytesPerArcWithoutLabel, labelRange);
        fstCompiler.directAddressingNodeCount++;
      } else {// 二分法的存储方式
        writeNodeForBinarySearch(fstCompiler, nodeIn, startAddress, maxBytesPerArc);
        fstCompiler.binarySearchNodeCount++;
      }
    }

    final long thisNodeAddress = fstCompiler.bytes.getPosition() - 1;
    fstCompiler.bytes.reverse(startAddress, thisNodeAddress);
    fstCompiler.nodeCount++;
    return thisNodeAddress;
  }

  /**
   * Returns whether the given node should be expanded with fixed length arcs. Nodes will be
   * expanded depending on their depth (distance from the root node) and their number of arcs.
   *
   * <p>Nodes with fixed length arcs use more space, because they encode all arcs with a fixed
   * number of bytes, but they allow either binary search or direct addressing on the arcs (instead
   * of linear scan) on lookup by arc label.
   */
  private boolean shouldExpandNodeWithFixedLengthArcs(
      FSTCompiler<T> fstCompiler, FSTCompiler.UnCompiledNode<T> node) {
    return fstCompiler.allowFixedLengthArcs
        && ((node.depth <= FIXED_LENGTH_ARC_SHALLOW_DEPTH
                && node.numArcs >= FIXED_LENGTH_ARC_SHALLOW_NUM_ARCS)
            || node.numArcs >= FIXED_LENGTH_ARC_DEEP_NUM_ARCS);
  }

  /**
   * Returns whether the given node should be expanded with direct addressing instead of binary
   * search.
   *
   * <p>Prefer direct addressing for performance if it does not oversize binary search byte size too
   * much, so that the arcs can be directly addressed by label.
   *
   * @see FSTCompiler#getDirectAddressingMaxOversizingFactor()
   */
  private boolean shouldExpandNodeWithDirectAddressing(
      FSTCompiler<T> fstCompiler,
      FSTCompiler.UnCompiledNode<T> nodeIn,
      int numBytesPerArc,
      int maxBytesPerArcWithoutLabel,
      int labelRange) {
    // Anticipate precisely the size of the encodings.
    int sizeForBinarySearch = numBytesPerArc * nodeIn.numArcs;
    int sizeForDirectAddressing =
        getNumPresenceBytes(labelRange)
            + fstCompiler.numLabelBytesPerArc[0]
            + maxBytesPerArcWithoutLabel * nodeIn.numArcs;

    // Determine the allowed oversize compared to binary search.
    // This is defined by a parameter of FST Builder (default 1: no oversize).
    int allowedOversize =
        (int) (sizeForBinarySearch * fstCompiler.getDirectAddressingMaxOversizingFactor());
    int expansionCost = sizeForDirectAddressing - allowedOversize;

    // Select direct addressing if either:
    // - Direct addressing size is smaller than binary search.
    //   In this case, increment the credit by the reduced size (to use it later).
    // - Direct addressing size is larger than binary search, but the positive credit allows the
    // oversizing.
    //   In this case, decrement the credit by the oversize.
    // In addition, do not try to oversize to a clearly too large node size
    // (this is the DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR parameter).
    if (expansionCost <= 0
        || (fstCompiler.directAddressingExpansionCredit >= expansionCost
            && sizeForDirectAddressing
                <= allowedOversize * DIRECT_ADDRESSING_MAX_OVERSIZE_WITH_CREDIT_FACTOR)) {
      fstCompiler.directAddressingExpansionCredit -= expansionCost;
      return true;
    }
    return false;
  }

  /**
   *
   * 二分法存储方式其实就是把线性查找的存储方式中每个arc的存储空间都按照最大的arc使用的存储空间对齐填充，
   * 所以逻辑就是先计算二分法存储空间的总大小，从后往前分配固定大小的空间给所有的arc。
   *
   * @param fstCompiler
   * @param nodeIn
   * @param startAddress
   * @param maxBytesPerArc
   */
  private void writeNodeForBinarySearch(
      FSTCompiler<T> fstCompiler,
      FSTCompiler.UnCompiledNode<T> nodeIn,
      long startAddress,
      int maxBytesPerArc) {
    // Build the header in a buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    // 写入node中arc的元信息head
    fstCompiler
        .fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_BINARY_SEARCH)
        .writeVInt(nodeIn.numArcs)
        .writeVInt(maxBytesPerArc);
    int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

    // Expand the arcs in place, backwards.
    long srcPos = fstCompiler.bytes.getPosition();
    long destPos = startAddress + headerLen + nodeIn.numArcs * maxBytesPerArc;
    assert destPos >= srcPos;
    if (destPos > srcPos) {
      fstCompiler.bytes.skipBytes((int) (destPos - srcPos));
      // 固定长度的存储逻辑比较简单，按照单个arc所需的最大空间存储每个arc信息，
      // 再加上arc的label是有序的，这样就可以使用二分法根据label查找arc的信息
      for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
        destPos -= maxBytesPerArc;
        int arcLen = fstCompiler.numBytesPerArc[arcIdx];
        srcPos -= arcLen;
        if (srcPos != destPos) {
          assert destPos > srcPos
              : "destPos="
                  + destPos
                  + " srcPos="
                  + srcPos
                  + " arcIdx="
                  + arcIdx
                  + " maxBytesPerArc="
                  + maxBytesPerArc
                  + " arcLen="
                  + arcLen
                  + " nodeIn.numArcs="
                  + nodeIn.numArcs;
          fstCompiler.bytes.copyBytes(srcPos, destPos, arcLen);
        }
      }
    }

    // Write the header.
    fstCompiler.bytes.writeBytes(
        startAddress, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
  }

  private void writeNodeForDirectAddressing(
      FSTCompiler<T> fstCompiler,
      FSTCompiler.UnCompiledNode<T> nodeIn,
      long startAddress,
      int maxBytesPerArcWithoutLabel,
      int labelRange) {
    // Expand the arcs backwards in a buffer because we remove the labels.
    // So the obtained arcs might occupy less space. This is the reason why this
    // whole method is more complex.
    // Drop the label bytes since we can infer the label based on the arc index,
    // the presence bits, and the first label. Keep the first label.
    // 1字节的标志位：ARCS_FOR_DIRECT_ADDRESSING
    // vint类型的labelRange：最多5字节
    // vint类型的maxBytesPerArcWithoutLabel：最多5字节
    int headerMaxLen = 11;
    // 只需要记录第一个label，配合labelRange，可以使用一个位图来记录所有的label，numPresenceBytes就是位图需要的字节数
    int numPresenceBytes = getNumPresenceBytes(labelRange);
    // 当前fst的位置，要从这个位置往前读
    long srcPos = fstCompiler.bytes.getPosition();
    // 第一个label + 所有arc的空间
    int totalArcBytes =
        fstCompiler.numLabelBytesPerArc[0] + nodeIn.numArcs * maxBytesPerArcWithoutLabel;
    //  当前node需要的总空间
    int bufferOffset = headerMaxLen + numPresenceBytes + totalArcBytes;
    // node的序列化数据先存储到buffer中
    byte[] buffer = fstCompiler.fixedLengthArcsBuffer.ensureCapacity(bufferOffset).getBytes();
    // Copy the arcs to the buffer, dropping all labels except first one.
    // 从fst中获取所有已经序列化的arc信息，存储到buffer中
    for (int arcIdx = nodeIn.numArcs - 1; arcIdx >= 0; arcIdx--) {
      // bufferOffset始终指向当前arc应该在buffer中的起始位置，注意这是这里没有预留存储label的空间
      bufferOffset -= maxBytesPerArcWithoutLabel;
      // 当前arc的序列化长度
      int srcArcLen = fstCompiler.numBytesPerArc[arcIdx];
      // srcPos定位到当前arc的起始位置
      srcPos -= srcArcLen;
      int labelLen = fstCompiler.numLabelBytesPerArc[arcIdx];
      // Copy the flags.
      // 复制flag
      fstCompiler.bytes.copyBytes(srcPos, buffer, bufferOffset, 1);
      // Skip the label, copy the remaining.
      // 跳过label，拷贝其他信息
      int remainingArcLen = srcArcLen - 1 - labelLen;
      if (remainingArcLen != 0) {
        fstCompiler.bytes.copyBytes(
            srcPos + 1 + labelLen, buffer, bufferOffset + 1, remainingArcLen);
      }
      if (arcIdx == 0) {// 只需要存储第一个label
        // Copy the label of the first arc only.
        bufferOffset -= labelLen;// 预留label空间
        fstCompiler.bytes.copyBytes(srcPos + 1, buffer, bufferOffset, labelLen);
      }
    }
    assert bufferOffset == headerMaxLen + numPresenceBytes;

    // Build the header in the buffer.
    // It is a false/special arc which is in fact a node header with node flags followed by node
    // metadata.
    // node中关于arc的元信息head写入fixedLengthArcsBuffer
    fstCompiler
        .fixedLengthArcsBuffer
        .resetPosition()
        .writeByte(ARCS_FOR_DIRECT_ADDRESSING)
        .writeVInt(labelRange) // labelRange instead of numArcs.
        .writeVInt(
            maxBytesPerArcWithoutLabel); // maxBytesPerArcWithoutLabel instead of maxBytesPerArc.
    int headerLen = fstCompiler.fixedLengthArcsBuffer.getPosition();

    // Prepare the builder byte store. Enlarge or truncate if needed.
    // 下面是准备把直接寻址的序列化信息拷贝到fstCompiler.bytes中
    // 直接寻址完整拷贝到 fstCompiler.bytes 的结束位置
    long nodeEnd = startAddress + headerLen + numPresenceBytes + totalArcBytes;
    // 当前fstCompiler.bytes的位置
    long currentPosition = fstCompiler.bytes.getPosition();
    if (nodeEnd >= currentPosition) { // 说明直接寻址占用的空间比线性查找的空间大，需要把位置往后挪
      fstCompiler.bytes.skipBytes((int) (nodeEnd - currentPosition));
    } else { // 说明直接寻址占用的空间比线性查找的空间小，需要把位置往前挪
      fstCompiler.bytes.truncate(nodeEnd);
    }
    assert fstCompiler.bytes.getPosition() == nodeEnd;

    // Write the header.
    // head写入fst
    long writeOffset = startAddress;
    fstCompiler.bytes.writeBytes(
        writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), 0, headerLen);
    writeOffset += headerLen;

    // Write the presence bits
    // 写入arc的位图信息
    writePresenceBits(fstCompiler, nodeIn, writeOffset, numPresenceBytes);
    writeOffset += numPresenceBytes;

    // Write the first label and the arcs.
    // 写入第一个label和其他arc信息
    fstCompiler.bytes.writeBytes(
        writeOffset, fstCompiler.fixedLengthArcsBuffer.getBytes(), bufferOffset, totalArcBytes);
  }

  private void writePresenceBits(
      FSTCompiler<T> fstCompiler,
      FSTCompiler.UnCompiledNode<T> nodeIn,
      long dest,
      int numPresenceBytes) {
    long bytePos = dest;
    // 第一个arc肯定是存在的
    byte presenceBits = 1; // The first arc is always present.
    // 记录位图的下标
    int presenceIndex = 0;
    // 记录前一个label
    int previousLabel = nodeIn.arcs[0].label;
    for (int arcIdx = 1; arcIdx < nodeIn.numArcs; arcIdx++) {
      int label = nodeIn.arcs[arcIdx].label;
      assert label > previousLabel;
      // 当前arc的label所属的下标
      presenceIndex += label - previousLabel;
      // 跳过位图的空byte
      while (presenceIndex >= Byte.SIZE) {
        // 记录位图
        fstCompiler.bytes.writeByte(bytePos++, presenceBits);
        presenceBits = 0;
        presenceIndex -= Byte.SIZE;
      }
      // Set the bit at presenceIndex to flag that the corresponding arc is present.
      // 设置对应位图的标记
      presenceBits |= 1 << presenceIndex;
      previousLabel = label;
    }
    assert presenceIndex == (nodeIn.arcs[nodeIn.numArcs - 1].label - nodeIn.arcs[0].label) % 8;
    assert presenceBits != 0; // The last byte is not 0.
    assert (presenceBits & (1 << presenceIndex)) != 0; // The last arc is always present.
    fstCompiler.bytes.writeByte(bytePos++, presenceBits);
    assert bytePos - dest == numPresenceBytes;
  }

  /**
   * Gets the number of bytes required to flag the presence of each arc in the given label range,
   * one bit per arc.
   */
  private static int getNumPresenceBytes(int labelRange) {
    assert labelRange >= 0;
    return (labelRange + 7) >> 3;
  }

  /**
   * Reads the presence bits of a direct-addressing node. Actually we don't read them here, we just
   * keep the pointer to the bit-table start and we skip them.
   */
  private void readPresenceBytes(Arc<T> arc, BytesReader in) throws IOException {
    assert arc.bytesPerArc() > 0;
    assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
    arc.bitTableStart = in.getPosition();
    in.skipBytes(getNumPresenceBytes(arc.numArcs()));
  }

  /** Fills virtual 'start' arc, ie, an empty incoming arc to the FST's start node */
  public Arc<T> getFirstArc(Arc<T> arc) {
    T NO_OUTPUT = outputs.getNoOutput();

    //初始化为空
    if (emptyOutput != null) {
      // 如果存在空输入，则这是BIT_FINAL_ARC 和 BIT_LAST_ARC
      arc.flags = BIT_FINAL_ARC | BIT_LAST_ARC;
      arc.nextFinalOutput = emptyOutput;
      if (emptyOutput != NO_OUTPUT) {
        arc.flags = (byte) (arc.flags() | BIT_ARC_HAS_FINAL_OUTPUT);
      }
    } else {
      arc.flags = BIT_LAST_ARC;
      arc.nextFinalOutput = NO_OUTPUT;
    }

    arc.output = NO_OUTPUT;

    // If there are no nodes, ie, the FST only accepts the
    // empty string, then startNode is 0
    arc.target = startNode;

    return arc;
  }

  /**
   * Follows the <code>follow</code> arc and reads the last arc of its target; this changes the
   * provided <code>arc</code> (2nd arg) in-place and returns it.
   *
   * @return Returns the second argument (<code>arc</code>).
   */
  Arc<T> readLastTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    // System.out.println("readLast");
    if (!targetHasArcs(follow)) {
      // System.out.println("  end node");
      assert follow.isFinal();
      arc.label = END_LABEL;
      arc.target = FINAL_END_NODE;
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_LAST_ARC;
      arc.nodeFlags = arc.flags;
      return arc;
    } else {
      in.setPosition(follow.target());
      byte flags = arc.nodeFlags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
        // Special arc which is actually a node header for fixed length arcs.
        // Jump straight to end to find the last arc.
        arc.numArcs = in.readVInt();
        arc.bytesPerArc = in.readVInt();
        // System.out.println("  array numArcs=" + arc.numArcs + " bpa=" + arc.bytesPerArc);
        if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
          readPresenceBytes(arc, in);
          arc.firstLabel = readLabel(in);
          arc.posArcsStart = in.getPosition();
          readLastArcByDirectAddressing(arc, in);
        } else {
          arc.arcIdx = arc.numArcs() - 2;
          arc.posArcsStart = in.getPosition();
          readNextRealArc(arc, in);
        }
      } else {
        arc.flags = flags;
        // non-array: linear scan
        arc.bytesPerArc = 0;
        // System.out.println("  scan");
        while (!arc.isLast()) {
          // skip this arc:
          readLabel(in);
          if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
            outputs.skipOutput(in);
          }
          if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
            outputs.skipFinalOutput(in);
          }
          if (arc.flag(BIT_STOP_NODE)) {
          } else if (arc.flag(BIT_TARGET_NEXT)) {
          } else {
            readUnpackedNodeTarget(in);
          }
          arc.flags = in.readByte();
        }
        // Undo the byte flags we read:
        in.skipBytes(-1);
        arc.nextArc = in.getPosition();
        readNextRealArc(arc, in);
      }
      assert arc.isLast();
      return arc;
    }
  }

  private long readUnpackedNodeTarget(BytesReader in) throws IOException {
    return in.readVLong();
  }

  /**
   * Follow the <code>follow</code> arc and read the first arc of its target; this changes the
   * provided <code>arc</code> (2nd arg) in-place and returns it.
   *
   * @return Returns the second argument (<code>arc</code>).
   */
  //从follow指向的target中获取第一个arc，存储在arc变量中
  public Arc<T> readFirstTargetArc(Arc<T> follow, Arc<T> arc, BytesReader in) throws IOException {
    // int pos = address;
    // System.out.println("    readFirstTarget follow.target=" + follow.target + " isFinal=" +
    // follow.isFinal());
    // 如果follow指向的是可接受节点，构造一个arc，用END_LABEL标记
    if (follow.isFinal()) {
      // Insert "fake" final first arc:
      arc.label = END_LABEL;
      arc.output = follow.nextFinalOutput();
      arc.flags = BIT_FINAL_ARC;
      if (follow.target() <= 0) {
        arc.flags |= BIT_LAST_ARC;
      } else {
        // NOTE: nextArc is a node (not an address!) in this case:
        // 可以看到深度优先遍历的时候，arc.nextArc指向的是下一个节点。
        // 这是为了处理某个输入是另一个输入前缀的情况，这种情况，深度优先遍历的下一个arc是在下一个节点中。
        arc.nextArc = follow.target();
      }
      arc.target = FINAL_END_NODE;
      arc.nodeFlags = arc.flags;
      // System.out.println("    insert isFinal; nextArc=" + follow.target + " isLast=" +
      // arc.isLast() + " output=" + outputs.outputToString(arc.output));
      return arc;
    } else {
      // 如果follow不是可接受节点，则读取真实的第一个arc
      return readFirstRealTargetArc(follow.target(), arc, in);
    }
  }

  public Arc<T> readFirstRealTargetArc(long nodeAddress, Arc<T> arc, final BytesReader in)
      throws IOException {
    // 定位到node的地址
    in.setPosition(nodeAddress);
    // System.out.println("   flags=" + arc.flags);

    // 读取flag
    byte flags = arc.nodeFlags = in.readByte();

    // 节点使用固定长度存储arc
    if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
      // System.out.println("  fixed length arc");
      // Special arc which is actually a node header for fixed length arcs.
      // 读取固定长度存储模式的各个头部信息
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.arcIdx = -1;
      if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
        readPresenceBytes(arc, in);
        arc.firstLabel = readLabel(in);
        arc.presenceIndex = -1;
      }
      arc.posArcsStart = in.getPosition();
      // System.out.println("  bytesPer=" + arc.bytesPerArc + " numArcs=" + arc.numArcs + "
      // arcsStart=" + pos);
    } else {
      // node的起始位置就是第一个arc的位置
      // arc.nextArc指向了第一个arc的位置
      arc.nextArc = nodeAddress;
      arc.bytesPerArc = 0;
    }

    // 这样查找arc的下一个arc就得到了第一个arc
    return readNextRealArc(arc, in);
  }

  /**
   * Returns whether <code>arc</code>'s target points to a node in expanded format (fixed length
   * arcs).
   */
  boolean isExpandedTarget(Arc<T> follow, BytesReader in) throws IOException {
    if (!targetHasArcs(follow)) {
      return false;
    } else {
      in.setPosition(follow.target());
      byte flags = in.readByte();
      return flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING;
    }
  }

  /** In-place read; returns the arc. */
  public Arc<T> readNextArc(Arc<T> arc, BytesReader in) throws IOException {
    if (arc.label() == END_LABEL) {
      // This was a fake inserted "final" arc
      if (arc.nextArc() <= 0) {
        throw new IllegalArgumentException("cannot readNextArc when arc.isLast()=true");
      }
      return readFirstRealTargetArc(arc.nextArc(), arc, in);
    } else {
      return readNextRealArc(arc, in);
    }
  }

  /** Peeks at next arc's label; does not alter arc. Do not call this if arc.isLast()! */
  int readNextArcLabel(Arc<T> arc, BytesReader in) throws IOException {
    assert !arc.isLast();

    if (arc.label() == END_LABEL) {
      // System.out.println("    nextArc fake " + arc.nextArc);
      // Next arc is the first arc of a node.
      // Position to read the first arc label.

      in.setPosition(arc.nextArc());
      byte flags = in.readByte();
      if (flags == ARCS_FOR_BINARY_SEARCH || flags == ARCS_FOR_DIRECT_ADDRESSING) {
        // System.out.println("    nextArc fixed length arc");
        // Special arc which is actually a node header for fixed length arcs.
        int numArcs = in.readVInt();
        in.readVInt(); // Skip bytesPerArc.
        if (flags == ARCS_FOR_BINARY_SEARCH) {
          in.readByte(); // Skip arc flags.
        } else {
          in.skipBytes(getNumPresenceBytes(numArcs));
        }
      }
    } else {
      if (arc.bytesPerArc() != 0) {
        // System.out.println("    nextArc real array");
        // Arcs have fixed length.
        if (arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH) {
          // Point to next arc, -1 to skip arc flags.
          in.setPosition(arc.posArcsStart() - (1 + arc.arcIdx()) * arc.bytesPerArc() - 1);
        } else {
          assert arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING;
          // Direct addressing node. The label is not stored but rather inferred
          // based on first label and arc index in the range.
          assert BitTable.assertIsValid(arc, in);
          assert BitTable.isBitSet(arc.arcIdx(), arc, in);
          int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
          assert nextIndex != -1;
          return arc.firstLabel() + nextIndex;
        }
      } else {
        // Arcs have variable length.
        // System.out.println("    nextArc real list");
        // Position to next arc, -1 to skip flags.
        in.setPosition(arc.nextArc() - 1);
      }
    }
    return readLabel(in);
  }

  public Arc<T> readArcByIndex(Arc<T> arc, final BytesReader in, int idx) throws IOException {
    assert arc.bytesPerArc() > 0;
    assert arc.nodeFlags() == ARCS_FOR_BINARY_SEARCH;
    assert idx >= 0 && idx < arc.numArcs();
    in.setPosition(arc.posArcsStart() - idx * arc.bytesPerArc());
    arc.arcIdx = idx;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /**
   * Reads a present direct addressing node arc, with the provided index in the label range.
   *
   * @param rangeIndex The index of the arc in the label range. It must be present. The real arc
   *     offset is computed based on the presence bits of the direct addressing node.
   */
  public Arc<T> readArcByDirectAddressing(Arc<T> arc, final BytesReader in, int rangeIndex)
      throws IOException {
    assert BitTable.assertIsValid(arc, in);
    assert rangeIndex >= 0 && rangeIndex < arc.numArcs();
    assert BitTable.isBitSet(rangeIndex, arc, in);
    int presenceIndex = BitTable.countBitsUpTo(rangeIndex, arc, in);
    return readArcByDirectAddressing(arc, in, rangeIndex, presenceIndex);
  }

  /**
   * Reads a present direct addressing node arc, with the provided index in the label range and its
   * corresponding presence index (which is the count of presence bits before it).
   */
  private Arc<T> readArcByDirectAddressing(
      Arc<T> arc, final BytesReader in, int rangeIndex, int presenceIndex) throws IOException {
    in.setPosition(arc.posArcsStart() - presenceIndex * arc.bytesPerArc());
    arc.arcIdx = rangeIndex;
    arc.presenceIndex = presenceIndex;
    arc.flags = in.readByte();
    return readArc(arc, in);
  }

  /**
   * Reads the last arc of a direct addressing node. This method is equivalent to call {@link
   * #readArcByDirectAddressing(Arc, BytesReader, int)} with {@code rangeIndex} equal to {@code
   * arc.numArcs() - 1}, but it is faster.
   */
  public Arc<T> readLastArcByDirectAddressing(Arc<T> arc, final BytesReader in) throws IOException {
    assert BitTable.assertIsValid(arc, in);
    int presenceIndex = BitTable.countBits(arc, in) - 1;
    return readArcByDirectAddressing(arc, in, arc.numArcs() - 1, presenceIndex);
  }

  /** Never returns null, but you should never call this if arc.isLast() is true. */
  public Arc<T> readNextRealArc(Arc<T> arc, final BytesReader in) throws IOException {

    // TODO: can't assert this because we call from readFirstArc
    // assert !flag(arc.flags, BIT_LAST_ARC);

    switch (arc.nodeFlags()) {
      // 二分查找存储的方式，知道下标可以直接定位
      case ARCS_FOR_BINARY_SEARCH:
        assert arc.bytesPerArc() > 0;
        arc.arcIdx++;
        assert arc.arcIdx() >= 0 && arc.arcIdx() < arc.numArcs();
        in.setPosition(arc.posArcsStart() - arc.arcIdx() * arc.bytesPerArc());
        arc.flags = in.readByte();
        break;

      // 直接寻址
      case ARCS_FOR_DIRECT_ADDRESSING:
        assert BitTable.assertIsValid(arc, in);
        assert arc.arcIdx() == -1 || BitTable.isBitSet(arc.arcIdx(), arc, in);
        int nextIndex = BitTable.nextBitSet(arc.arcIdx(), arc, in);
        return readArcByDirectAddressing(arc, in, nextIndex, arc.presenceIndex + 1);

      // 线性遍历
      default:
        // Variable length arcs - linear search.
        assert arc.bytesPerArc() == 0;
        // 定位到下一个arc的位置
        in.setPosition(arc.nextArc());
        arc.flags = in.readByte();
    }
    // 读取arc的信息
    return readArc(arc, in);
  }

  /**
   * Reads an arc. <br>
   * Precondition: The arc flags byte has already been read and set; the given BytesReader is
   * positioned just after the arc flags byte.
   */
  // 注意，这里是已经定位到了in中arc所在的位置了
  private Arc<T> readArc(Arc<T> arc, BytesReader in) throws IOException {
    // 直接寻址的方式是的label需要计算
    if (arc.nodeFlags() == ARCS_FOR_DIRECT_ADDRESSING) {
      // 直接寻址的label是第一个label的值加上arc的下标
      arc.label = arc.firstLabel() + arc.arcIdx();
    } else {
      arc.label = readLabel(in);
    }

    // 读取output
    if (arc.flag(BIT_ARC_HAS_OUTPUT)) {
      arc.output = outputs.read(in);
    } else {
      arc.output = outputs.getNoOutput();
    }

    // 读取finalOutput
    if (arc.flag(BIT_ARC_HAS_FINAL_OUTPUT)) {
      arc.nextFinalOutput = outputs.readFinalOutput(in);
    } else {
      arc.nextFinalOutput = outputs.getNoOutput();
    }

    // 这种情况，下一个节点的位置是存储在一起的
    if (arc.flag(BIT_STOP_NODE)) {
      if (arc.flag(BIT_FINAL_ARC)) {
        arc.target = FINAL_END_NODE;
      } else {
        // 一般逻辑不会出现，剪枝才有可能
        arc.target = NON_FINAL_END_NODE;
      }
      arc.nextArc = in.getPosition(); // Only useful for list.
    } else if (arc.flag(BIT_TARGET_NEXT)) {
      // 这种情况，下一个节点的位置是存储在一起的
      arc.nextArc = in.getPosition(); // Only useful for list.
      // TODO: would be nice to make this lazy -- maybe
      // caller doesn't need the target and is scanning arcs...
      if (!arc.flag(BIT_LAST_ARC)) {
        // 需要使用遍历的方式查找
        if (arc.bytesPerArc() == 0) {
          // must scan
          seekToNextNode(in);
        } else {
          // 固定长度的arc存储方式可以通过计算下标直接定位
          int numArcs =
              arc.nodeFlags == ARCS_FOR_DIRECT_ADDRESSING
                  ? BitTable.countBits(arc, in)
                  : arc.numArcs();
          in.setPosition(arc.posArcsStart() - arc.bytesPerArc() * numArcs);
        }
      }
      arc.target = in.getPosition();
    } else {
      arc.target = readUnpackedNodeTarget(in);
      arc.nextArc = in.getPosition(); // Only useful for list.
    }
    return arc;
  }

  static <T> Arc<T> readEndArc(Arc<T> follow, Arc<T> arc) {
    if (follow.isFinal()) {
      if (follow.target() <= 0) {
        arc.flags = FST.BIT_LAST_ARC;
      } else {
        arc.flags = 0;
        // NOTE: nextArc is a node (not an address!) in this case:
        arc.nextArc = follow.target();
      }
      arc.output = follow.nextFinalOutput();
      arc.label = FST.END_LABEL;
      return arc;
    } else {
      return null;
    }
  }

  // TODO: could we somehow [partially] tableize arc lookups
  // like automaton?

  /**
   * 要查询FST就需要初始化bytes对象，在FST的构造函数中，会用一个DataInput来初始化bytes。
   * 遍历FST的函数中，getFirstArc的主要作用是拿到FST的根节点的ID，拿到根节点的ID以后，
   * 就可以用readFirstTargetArc函数或者readFirstRealTargetArc函数来读取从Node出发的第一个Arc的信息，
   * readNextArc则可以读取Arc紧邻的下一个Arc，这些都内容都比较简单。只是需要注意的是，Arc的isLast()为true的时候，
   * 说明已经遍历到了从Node出发的最后一个Arc，isFinal()为true的时候，说明这个Arc的target是终止节点。
   *
   * Finds an arc leaving the incoming arc, replacing the arc in place. This returns null if the arc
   * was not found, else the incoming arc.
   */
  public Arc<T> findTargetArc(int labelToMatch, Arc<T> follow, Arc<T> arc, BytesReader in)
      throws IOException {

    // 如果要找的结束的label
    if (labelToMatch == END_LABEL) {
      // 如果要找的是label是END_LABEL的话，必须follow是可接受节点
      if (follow.isFinal()) {
        if (follow.target() <= 0) {
          arc.flags = BIT_LAST_ARC;
        } else {
          arc.flags = 0;
          // NOTE: nextArc is a node (not an address!) in this case:
          arc.nextArc = follow.target();
        }
        arc.output = follow.nextFinalOutput();
        arc.label = END_LABEL;
        arc.nodeFlags = arc.flags;
        return arc;
      } else {
        return null;
      }
    }

    // 要查找的节点没有arc，直接返回null
    if (!targetHasArcs(follow)) {
      return null;
    }

    // 定位到target的位置
    in.setPosition(follow.target());

    // System.out.println("fta label=" + (char) labelToMatch);

    // 读取flag
    byte flags = arc.nodeFlags = in.readByte();

    // 1.以直接寻址的方式查找
    if (flags == ARCS_FOR_DIRECT_ADDRESSING) {
      // 读取用以直接寻址的头部信息
      arc.numArcs = in.readVInt(); // This is in fact the label range.
      arc.bytesPerArc = in.readVInt();
      readPresenceBytes(arc, in);
      arc.firstLabel = readLabel(in);
      arc.posArcsStart = in.getPosition();

      // 按直接寻址的方式查找，先从位图判断是否存在该label的arc，如果存在则获取存储的下标，直接定位到arc位置读取信息
      int arcIndex = labelToMatch - arc.firstLabel();
      if (arcIndex < 0 || arcIndex >= arc.numArcs()) {
        return null; // Before or after label range.
      } else if (!BitTable.isBitSet(arcIndex, arc, in)) {
        return null; // Arc missing in the range.
      }
      return readArcByDirectAddressing(arc, in, arcIndex);
    } else if (flags == ARCS_FOR_BINARY_SEARCH) { // 2.以二分查找的方式查找
      // 读取用以二分查找的头部信息
      arc.numArcs = in.readVInt();
      arc.bytesPerArc = in.readVInt();
      arc.posArcsStart = in.getPosition();

      // Array is sparse; do binary search:
      // 二分查找的实现
      int low = 0;
      int high = arc.numArcs() - 1;
      while (low <= high) {
        // System.out.println("    cycle");
        int mid = (low + high) >>> 1;
        // +1 to skip over flags
        // +1是略过flag，定位到label的位置
        in.setPosition(arc.posArcsStart() - (arc.bytesPerArc() * mid + 1));
        int midLabel = readLabel(in);
        final int cmp = midLabel - labelToMatch;
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {// 找到了，则下标设置为目标的前一个arc的下标，然后使用readNextRealArc方法读取
          arc.arcIdx = mid - 1;
          // System.out.println("    found!");
          return readNextRealArc(arc, in);
        }
      }
      return null;
    }

    // Linear scan
    // 3.线性查找：先读取到第一个arc，然后使用  readNextRealArc 方法来遍历
    readFirstRealTargetArc(follow.target(), arc, in);

    while (true) {
      // System.out.println("  non-bs cycle");
      // TODO: we should fix this code to not have to create
      // object for the output of every arc we scan... only
      // for the matching arc, if found
      if (arc.label() == labelToMatch) {
        // System.out.println("    found!");
        return arc;
      } else if (arc.label() > labelToMatch) {
        return null;
      } else if (arc.isLast()) {
        return null;
      } else {
        readNextRealArc(arc, in);
      }
    }
  }

  private void seekToNextNode(BytesReader in) throws IOException {

    while (true) {

      final int flags = in.readByte();

      readLabel(in);

      if (flag(flags, BIT_ARC_HAS_OUTPUT)) {
        outputs.skipOutput(in);
      }

      if (flag(flags, BIT_ARC_HAS_FINAL_OUTPUT)) {
        outputs.skipFinalOutput(in);
      }

      if (!flag(flags, BIT_STOP_NODE) && !flag(flags, BIT_TARGET_NEXT)) {
        readUnpackedNodeTarget(in);
      }

      if (flag(flags, BIT_LAST_ARC)) {
        return;
      }
    }
  }

  /** Returns a {@link BytesReader} for this FST, positioned at position 0. */
  public BytesReader getBytesReader() {
    if (this.fstStore != null) {
      return this.fstStore.getReverseBytesReader();
    } else {
      return bytes.getReverseReader();
    }
  }

  /** Reads bytes stored in an FST. */
  public abstract static class BytesReader extends DataInput {

    /** Get current read position. */
    public abstract long getPosition();

    /** Set current read position. */
    public abstract void setPosition(long pos);

    /** Returns true if this reader uses reversed bytes under-the-hood. */
    public abstract boolean reversed();
  }
}
