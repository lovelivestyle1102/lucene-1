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

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary output) from pre-sorted terms with
 * outputs. The FST becomes an FSA if you use NoOutputs. The FST is written on-the-fly into a
 * compact serialized format byte array, which can be saved to / loaded from a Directory or used
 * directly for traversal. The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
 *
 * <p>The parameterized type T is the output type. See the subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene 4.2). FSTs containing more than 2.1B
 * nodes are also now possible, however they cannot be packed.
 *
 * @lucene.experimental
 */
public class FSTCompiler<T> {

  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

  // 用来判断node的唯一性，序列化是从尾部开始的，如果要序列化的node已经序列化过了，则直接复用，从而达到了共享后缀的目的
  private final NodeHash<T> dedupHash;

  final FST<T> fst;

  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  //0
  // simplistic pruning: we prune node (and all following
  // nodes) if less than this number of terms go through it:
  private final int minSuffixCount1;

  //0
  // better pruning: we prune node (and all following
  // nodes) if the prior node has less than this number of
  // terms go through it:
  private final int minSuffixCount2;

  //true
  private final boolean doShareNonSingletonNodes;

  private final int shareMaxTailLength;

  //上次输入
  private final IntsRefBuilder lastInput = new IntsRefBuilder();

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  // 还没有序列化的节点都存储在frontier中，第0个是root节点，在最后进行序列化
  private UnCompiledNode<T>[] frontier;

  // Used for the BIT_TARGET_NEXT optimization (whereby
  // instead of storing the address of the target node for
  // a given arc, we mark a single bit noting that the next
  // node in the byte[] is the target node):
  // 上一个序列化的node
  long lastFrozenNode;

  // Reused temporarily while building the FST:
  // 使用固定长度的方式存储arc的时候使用
  int[] numBytesPerArc = new int[4];
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];

  // 如果使用的是固定长度的arc存储方式，会先把一些信息存储在这个buffer中，最后拷贝到fst中
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  // 一些统计信息
  long arcCount;
  long nodeCount;
  long binarySearchNodeCount;
  long directAddressingNodeCount;

  // node所有arc的存储
  final boolean allowFixedLengthArcs;
  final float directAddressingMaxOversizingFactor;
  long directAddressingExpansionCredit;

  // 序列化的数据存储在bytes中,其实最终的fst存储在该实例中的byte[] current;
  final BytesStore bytes;

  /**
   * Instantiates an FST/FSA builder with default settings and pruning options turned off. For more
   * tuning and tweaking, see {@link Builder}.
   */
  public FSTCompiler(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
    this(inputType, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15, 1f);
  }

  private FSTCompiler(
      FST.INPUT_TYPE inputType,
      int minSuffixCount1,
      int minSuffixCount2,
      boolean doShareSuffix,
      boolean doShareNonSingletonNodes,
      int shareMaxTailLength,
      Outputs<T> outputs,
      boolean allowFixedLengthArcs,
      int bytesPageBits,
      float directAddressingMaxOversizingFactor) {
    this.minSuffixCount1 = minSuffixCount1;

    this.minSuffixCount2 = minSuffixCount2;

    this.doShareNonSingletonNodes = doShareNonSingletonNodes;

    this.shareMaxTailLength = shareMaxTailLength;

    this.allowFixedLengthArcs = allowFixedLengthArcs;

    this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;

    fst = new FST<>(inputType, outputs, bytesPageBits);

    bytes = fst.bytes;

    assert bytes != null;

    if (doShareSuffix) {
      dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
    } else {
      dedupHash = null;
    }

    NO_OUTPUT = outputs.getNoOutput();

    //待处理节点列表
    @SuppressWarnings({"rawtypes", "unchecked"})
    final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;

    for (int idx = 0; idx < frontier.length; idx++) {
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * Fluent-style constructor for FST {@link FSTCompiler}.
   *
   * <p>Creates an FST/FSA builder with all the possible tuning and construction tweaks. Read
   * parameter documentation carefully.
   */
  public static class Builder<T> {

    private final INPUT_TYPE inputType;
    private final Outputs<T> outputs;
    private int minSuffixCount1;
    private int minSuffixCount2;
    private boolean shouldShareSuffix = true;
    private boolean shouldShareNonSingletonNodes = true;
    private int shareMaxTailLength = Integer.MAX_VALUE;
    private boolean allowFixedLengthArcs = true;
    private int bytesPageBits = 15;
    private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

    /**
     * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
     *     enumeration. Shorter types will consume less memory. Strings (character sequences) are
     *     represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
     * @param outputs The output type for each input sequence. Applies only if building an FST. For
     *     FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
     *     singleton output object.
     */
    public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
      this.inputType = inputType;
      this.outputs = outputs;
    }

    /**
     * If pruning the input graph during construction, this threshold is used for telling if a node
     * is kept or pruned. If transition_count(node) &gt;= minSuffixCount1, the node is kept.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount1(int minSuffixCount1) {
      this.minSuffixCount1 = minSuffixCount1;
      return this;
    }

    /**
     * Better pruning: we prune node (and all following nodes) if the prior node has less than this
     * number of terms go through it.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount2(int minSuffixCount2) {
      this.minSuffixCount2 = minSuffixCount2;
      return this;
    }

    /**
     * If {@code true}, the shared suffixes will be compacted into unique paths. This requires an
     * additional RAM-intensive hash map for lookups in memory. Setting this parameter to {@code
     * false} creates a single suffix path for all input sequences. This will result in a larger
     * FST, but requires substantially less memory and CPU during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareSuffix(boolean shouldShareSuffix) {
      this.shouldShareSuffix = shouldShareSuffix;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to true to ensure FST is fully
     * minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareNonSingletonNodes(boolean shouldShareNonSingletonNodes) {
      this.shouldShareNonSingletonNodes = shouldShareNonSingletonNodes;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to Integer.MAX_VALUE to ensure FST
     * is fully minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@link Integer#MAX_VALUE}.
     */
    public Builder<T> shareMaxTailLength(int shareMaxTailLength) {
      this.shareMaxTailLength = shareMaxTailLength;
      return this;
    }

    /**
     * Pass {@code false} to disable the fixed length arc optimization (binary search or direct
     * addressing) while building the FST; this will make the resulting FST smaller but slower to
     * traverse.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
      this.allowFixedLengthArcs = allowFixedLengthArcs;
      return this;
    }

    /**
     * How many bits wide to make each byte[] block in the BytesStore; if you know the FST will be
     * large then make this larger. For example 15 bits = 32768 byte pages.
     *
     * <p>Default = 15.
     */
    public Builder<T> bytesPageBits(int bytesPageBits) {
      this.bytesPageBits = bytesPageBits;
      return this;
    }

    /**
     * Overrides the default the maximum oversizing of fixed array allowed to enable direct
     * addressing of arcs instead of binary search.
     *
     * <p>Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
     * only binary search nodes will be created.
     *
     * <p>This factor does not determine whether to encode a node with a list of variable length
     * arcs or with fixed length arcs. It only determines the effective encoding of a node that is
     * already known to be encoded with fixed length arcs.
     *
     * <p>Default = 1.
     */
    public Builder<T> directAddressingMaxOversizingFactor(float factor) {
      this.directAddressingMaxOversizingFactor = factor;
      return this;
    }

    /** Creates a new {@link FSTCompiler}. */
    public FSTCompiler<T> build() {
      FSTCompiler<T> fstCompiler =
          new FSTCompiler<>(
              inputType,
              minSuffixCount1,
              minSuffixCount2,
              shouldShareSuffix,
              shouldShareNonSingletonNodes,
              shareMaxTailLength,
              outputs,
              allowFixedLengthArcs,
              bytesPageBits,
              directAddressingMaxOversizingFactor);
      return fstCompiler;
    }
  }

  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getTermCount() {
    return frontier[0].inputCount;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1 + nodeCount;
  }

  public long getArcCount() {
    return arcCount;
  }

  public long getMappedStateCount() {
    return dedupHash == null ? 0 : nodeCount;
  }

  private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
    final long node;
    // 当前字节buffer的起始位置
    long bytesPosStart = bytes.getPosition();

    // 整个if是为了判断是否需要共享节点 ，这部分的逻辑决定了是否要使用共享后缀
    if (dedupHash != null // 能够共享的前提是存在dedupHash
        && (doShareNonSingletonNodes /*doShareNonSingletonNodes表示是否需要共享节点*/
            || nodeIn.numArcs <= 1)  /*如果节点只有一个出边*/
        && tailLength <= shareMaxTailLength) { // 在配置的最长公共共享后缀范围之内
      if (nodeIn.numArcs == 0) {// 如果节点没有边，一般是final节点
        // 真正序列化是通过 fst.addNode，返回的node是节点在fst中flag的位置
        node = fst.addNode(this, nodeIn);

        lastFrozenNode = node;
      } else {
        // dedupHash 会判断nodeIn是否序列化过，如果序列化过则共享已序列化的即可，否则使用fst.addNode进行序列化
        // 这里是共享后缀的实现
        node = dedupHash.add(this, nodeIn);
      }
    } else {
      // 不需要处理共享的情况，直接是用fst.addNode序列化
      node = fst.addNode(this, nodeIn);
    }
    assert node != -2;

    // 序列化之后buffer的位置
    long bytesPosEnd = bytes.getPosition();

    // 说明新序列化节点了
    if (bytesPosEnd != bytesPosStart) {
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }

    nodeIn.clear();

    // 序列化之后的节点只有一个编号（在fst中的起始位置）
    final CompiledNode fn = new CompiledNode();

    fn.node = node;

    return fn;
  }

  /**
   *
   * 如果prefixLenPlus1大于0，说明从prefixLenPlus1开始后面的内容已经不会再改变了。可以冻结了
   *
   *
   * 确定不变的部分可以进行序列化，序列化是从后往前处理的
   * 这个方法涉及到剪枝的逻辑判断，在lucene自己的使用场景中，都没有用到剪枝，所以这里我们也不会展开讲剪枝的部分
   *
   * @param prefixLenPlus1
   * @throws IOException
   */
  private void freezeTail(int prefixLenPlus1) throws IOException {
    // System.out.println("  compileTail " + prefixLenPlus1);
    // root是第0个节点，root是最后进行序列化的，所以最多序列化第一个节点
    final int downTo = Math.max(1, prefixLenPlus1);

    // 从上次输入字符最后一个字符开始到当前最小公共长度，从后往前处理
    for (int idx = lastInput.length(); idx >= downTo; idx--) {

      boolean doPrune = false;

      //是否需要编译
      boolean doCompile = false;

      //一次获取未处理的最后一个字符
      final UnCompiledNode<T> node = frontier[idx];

      final UnCompiledNode<T> parent = frontier[idx - 1];

      //如果Node的出度小于minSuffixCount1，会被删除，minSuffixCount1默认为0
      if (node.inputCount < minSuffixCount1) {
        doPrune = true;
        doCompile = true;
      } else if (idx > prefixLenPlus1) {
        // prune if parent's inputCount is less than suffixMinCount2
        //如果Node的父节点出度小于minSuffixCount2，会被删除，minSuffixCount2默认为0
        if (parent.inputCount < minSuffixCount2
            || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) {
          // my parent, about to be compiled, doesn't make the cut, so
          // I'm definitely pruned

          // if minSuffixCount2 is 1, we keep only up
          // until the 'distinguished edge', ie we keep only the
          // 'divergent' part of the FST. if my parent, about to be
          // compiled, has inputCount 1 then we are already past the
          // distinguished edge.  NOTE: this only works if
          // the FST outputs are not "compressible" (simple
          // ords ARE compressible).
          doPrune = true;
        } else {
          // my parent, about to be compiled, does make the cut, so
          // I'm definitely not pruned
          doPrune = false;
        }
        doCompile = true;
      } else {
        // if pruning is disabled (count is 0) we can always
        // compile current node
        doCompile = minSuffixCount2 == 0;
      }

      // System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx="
      // + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune="
      // + doPrune);

      //判断Node的出度小于minSuffixCount2
      if (node.inputCount < minSuffixCount2
          || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
        // drop all arcs
        //删除Node的所有后续Node
        for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
          @SuppressWarnings({"rawtypes", "unchecked"})
          final UnCompiledNode<T> target = (UnCompiledNode<T>) node.arcs[arcIdx].target;
          target.clear();
        }
        node.numArcs = 0;
      }

      if (doPrune) {
        // this node doesn't make it -- deref it
        //删除当前Node，并删除从父节点出发到当前Node的Arc
        node.clear();
        parent.deleteLast(lastInput.intAt(idx - 1), node);
      } else {

        if (minSuffixCount2 != 0) {
          compileAllTargets(node, lastInput.length() - idx);
        }

        //获取node的out值
        final T nextFinalOutput = node.output;

        //是否为最终节点
        // We "fake" the node as being final if it has no
        // outgoing arcs; in theory we could leave it
        // as non-final (the FST can represent this), but
        // FSTEnum, Util, etc., have trouble w/ non-final
        // dead-end states:
        final boolean isFinal = node.isFinal || node.numArcs == 0;

        if (doCompile) {
          // this node makes it and we now compile it.  first,
          // compile any targets that were previously
          // undecided:
          //用compileNode 函数将Node写入FST，得到一个CompiledNode，替换掉Arc的target
          parent.replaceLast(
              lastInput.intAt(idx - 1),
              //compileNode可能返回的是一个已经写入FST的Node的ID
              //这样就达到共享Node的目的
              compileNode(node, 1 + lastInput.length() - idx),
              nextFinalOutput,
              isFinal);
        } else {
          // replaceLast just to install
          // nextFinalOutput/isFinal onto the arc
          //node不写入FST，这里只是更新了Arc的nextFinalOutput和isFinal俩个字段
          //最后调用finish的时候再全部写入FST
          parent.replaceLast(lastInput.intAt(idx - 1), node, nextFinalOutput, isFinal);
          // this node will stay in play for now, since we are
          // undecided on whether to prune it.  later, it
          // will be either compiled or pruned, so we must
          // allocate a new node:
          frontier[idx] = new UnCompiledNode<>(this, idx);
        }
      }
    }
  }

  // for debugging
  /*
  private String toString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      return b.toString();
    }
  }
  */

  /**
   * 需要保证调用add的intput是有序的
   *
   * Add the next input/output pair. The provided input must be sorted after the previous one
   * according to {@link IntsRef#compareTo}. It's also OK to add the same input twice in a row with
   * different outputs, as long as {@link Outputs} implements the {@link Outputs#merge} method. Note
   * that input is fully consumed after this method is returned (so caller is free to reuse), but
   * output is not. So if your outputs are changeable (eg {@link ByteSequenceOutputs} or {@link
   * IntSequenceOutputs}) then you cannot reuse across calls.
   */
  public void add(IntsRef input, T output) throws IOException {
    /*
    if (DEBUG) {
      BytesRef b = new BytesRef(input.length);
      for(int x=0;x<input.length;x++) {
        b.bytes[x] = (byte) input.ints[x];
      }
      b.length = input.length;
      if (output == NO_OUTPUT) {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b);
      } else {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b + " output=" + fst.outputs.outputToString(output));
      }
    }
    */

    // De-dup NO_OUTPUT since it must be a singleton:
    if (output.equals(NO_OUTPUT)) {
      output = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0
        : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(output);

    // System.out.println("\nadd: " + input);
    // 空的输入，一般不会出现这种特殊情况。
    if (input.length == 0) {
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].inputCount++;
      frontier[0].isFinal = true;
      fst.setEmptyOutput(output);
      return;
    }

    // compare shared prefix length
    //比较公共前缀，查找公共前缀位置
    int pos1 = 0;

    //起始位置
    int pos2 = input.offset;

    //最小公共长度
    final int pos1Stop = Math.min(lastInput.length(), input.length);

    //遍历当前term与上个term的公共前缀的长度
    while (true) {
      // 第一个是根节点，无条件inputCount++。增加
      frontier[pos1].inputCount++;

      // System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" +
      // frontier[pos1]);
      // 如果比对到了末尾或者当前的字符不相等，则已经找到了最长公共前缀。直到第一个不想等的地方跳出循环
      if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }

    // 非最长公共前缀的第一个字符的位置
    final int prefixLenPlus1 = pos1 + 1;

    // 如果frontier大小不足，则需要扩容（+1是有个根节点）
    if (frontier.length < input.length + 1) {
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);

      //重新拷贝
      for (int idx = frontier.length; idx < next.length; idx++) {
        next[idx] = new UnCompiledNode<>(this, idx);
      }

      frontier = next;
    }

    // minimize/compile states from previous input's
    // orphan'd suffix
    // 冻结前一个输入不会变化的部分，前一个输入从prefixLen Plus 1开始的部分肯定是不会再改变的
    freezeTail(prefixLenPlus1);

    // init tail states for current input
    //从后往前，方便添加target和统计inputcount
    // 非公共前缀部分加入frontier，这里就保证的前缀是共享的，只有一份
    for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
      //相邻俩个Node增加Arc，并设置label的值
      frontier[idx - 1].addArc(input.ints[input.offset + idx - 1], frontier[idx]);
      frontier[idx].inputCount++;
    }

    //判断是否是最后一个节点
    final UnCompiledNode<T> lastNode = frontier[input.length];

    // 在保证输入有序的前提下，下面这个逻辑的if永远是true
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
      //当前term的最后一个节点就是final节点
      lastNode.isFinal = true;
      lastNode.output = NO_OUTPUT;
    }

    // push conflicting outputs forward, only as far as needed
    // 第0个是root节点 更新每个arc的输出
    // 更新output值，这里是从前往后
    for (int idx = 1; idx < prefixLenPlus1; idx++) {
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx - 1];

      // parentNode的最后一个边的输出就是目前位置公共前缀的输出值，需要被调整
      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      // 当前输入和上一个输入在当前节点的输出值的前缀（数值类型就是min）
      final T commonOutputPrefix;

      // 扣除公共输出之后，剩下的输出值
      final T wordSuffix;

      if (lastOutput != NO_OUTPUT) {
        //计算公共的Arc的output与当前的output的公共前缀，取俩个中最小的
        //从头开始当前父节点的lastOutput和当前output进行比较，取最小值作为公共前缀
        commonOutputPrefix = fst.outputs.common(output, lastOutput);

        //output除去前缀之外的剩余后缀
        assert validOutput(commonOutputPrefix);

        //重置公共Arc的output，设置commonOutputPrefix，这是新的前缀。实际是lastOutput - commonOutputPrefix
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);

        assert validOutput(wordSuffix);

        // 更新公共前缀的输出值，父节点设置最小值
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);

        // 剩余的输出值记录到当前的节点的所有的arc中
        node.prependOutput(wordSuffix);
      } else {
        //如果公共的Arc没有output，则当前output不需要进行任何操作
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      //当前output的内容减去公共Arc的output
      output = fst.outputs.subtract(output, commonOutputPrefix);

      assert validOutput(output);
    }

    if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs
      // 相同的输入，结果做merge，默认的没有实现merge方法，有需要需要自定义
      lastNode.output = fst.outputs.merge(lastNode.output, output);
    } else {
      // 剩下的输出值都放在第一个新增arc上
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1 - 1].setLastOutput(
          input.ints[input.offset + prefixLenPlus1 - 1], output);
    }

    // save last input
    // 保存上一个处理完的输入，为了和下一个输入算最长前缀
    lastInput.copyInts(input);

    // System.out.println("  count[0]=" + frontier[0].inputCount);
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST. NOTE: this will return null if nothing is accepted by the FST. */
  public FST<T> compile() throws IOException {

    final UnCompiledNode<T> root = frontier[0];

    // minimize nodes in the last word's suffix
    freezeTail(0);

    // 剪枝操作不用理
    if (root.inputCount < minSuffixCount1
        || root.inputCount < minSuffixCount2
        || root.numArcs == 0) {
      if (fst.emptyOutput == null) {
        return null;
      } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
        // empty string got pruned
        return null;
      }
    } else {
      if (minSuffixCount2 != 0) {
        compileAllTargets(root, lastInput.length());
      }
    }

    // 完成构建
    // if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + "
    // root.output=" + root.output);
    fst.finish(compileNode(root, lastInput.length()).node);

    return fst;
  }

  private void compileAllTargets(UnCompiledNode<T> node, int tailLength) throws IOException {
    for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
      final Arc<T> arc = node.arcs[arcIdx];
      if (!arc.target.isCompiled()) {
        // not yet compiled
        @SuppressWarnings({"rawtypes", "unchecked"})
        final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
        if (n.numArcs == 0) {
          // System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
          arc.isFinal = n.isFinal = true;
        }
        arc.target = compileNode(n, tailLength - 1);
      }
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) arc. */
  static class Arc<T> {
    // 边的输入
    int label; // really an "unsigned" byte
    // 指向的节点
    Node target;
    // 是否指向final节点
    boolean isFinal;
    // 边上的输出，这个输出是所有经过该arc的路径共享的
    T output;
    // nextFinalOutput就是这个arc指向的node的output，它只对以该node为终止节点的查找才有用，区别于arc的output
    T nextFinalOutput;
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  interface Node {
    boolean isCompiled();
  }

  public long fstRamBytesUsed() {
    return fst.ramBytesUsed();
  }

  static final class CompiledNode implements Node {
    long node;

    @Override
    public boolean isCompiled() {
      return true;
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) Node. */
  static final class UnCompiledNode<T> implements Node {
    // 节点所属的FSTCompiler
    final FSTCompiler<T> owner;

    // 节点总共有多少个arc，指的是出边
    int numArcs;

    // 所有的arc，注意是按顺序存储的
    Arc<T>[] arcs;

    // 节点的输出
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    T output;

    // 是不是终止节点
    boolean isFinal;

    // 经过这个节点的路径个数，注意这里不是从图中看到的入边的个数，因为FST使用共同的后缀
    long inputCount;

    /** This node's depth, starting from the automaton root. */
    // 节点距离root节点的距离，用来控制所有的arc是否使用相同长度存储的参数。
    final int depth;

    /**
     * @param depth The node's depth starting from the automaton root. Needed for LUCENE-2934 (node
     *     expansion based on conditions other than the fanout size).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    UnCompiledNode(FSTCompiler<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    @Override
    public boolean isCompiled() {
      return false;
    }

    void clear() {
      numArcs = 0;
      isFinal = false;
      output = owner.NO_OUTPUT;
      inputCount = 0;

      // We don't clear the depth here because it never changes
      // for nodes on the frontier (even when reused).
    }

    // 获取最后边的输出
    T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs - 1].label == labelToMatch;
      return arcs[numArcs - 1].output;
    }

    // 新增一条出边
    // label: arc的输入
    // target: arc的目标节点
    void addArc(int label, Node target) {
      assert label >= 0;
      assert numArcs == 0 || label > arcs[numArcs - 1].label
          : "arc[numArcs-1].label="
              + arcs[numArcs - 1].label
              + " new label="
              + label
              + " numArcs="
              + numArcs;
      // 是否需要扩容
      if (numArcs == arcs.length) {
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs, numArcs + 1);
        for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
          newArcs[arcIdx] = new Arc<>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }

    // 更新最后一条出边
    void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      //最后的出度
      final Arc<T> arc = arcs[numArcs - 1];

      assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;

      arc.target = target;
      // assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }

    // 删除最后一条出边
    void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs - 1].label;
      assert target == arcs[numArcs - 1].target;
      numArcs--;
    }

    // 设置最后一条出边的输出值
    void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;

      //最后一条边
      final Arc<T> arc = arcs[numArcs - 1];

      assert arc.label == labelToMatch;

      //更新output值
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    // 更新所有出边的输出值
    void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);

      // 更新所有出边的输出值
      for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {

        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);

        assert owner.validOutput(arcs[arcIdx].output);
      }

      // dog: 3
      // dogs: 2
      // 这种情况，dog是dogs的前缀，并且输出比dogs大，共享前缀输出是2，剩下的1需要保存在节点中，
      // 这里的if就是处理上面描述的这种情况
      if (isFinal) {
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }

  /**
   * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
   */
  static class FixedLengthArcsBuffer {

    // Initial capacity is the max length required for the header of a node with fixed length arcs:
    // header(byte) + numArcs(vint) + numBytes(vint)
    private byte[] bytes = new byte[11];
    private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

    /** Ensures the capacity of the internal byte array. Enlarges it if needed. */
    FixedLengthArcsBuffer ensureCapacity(int capacity) {
      if (bytes.length < capacity) {
        bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
        bado.reset(bytes);
      }
      return this;
    }

    FixedLengthArcsBuffer resetPosition() {
      bado.reset(bytes);
      return this;
    }

    FixedLengthArcsBuffer writeByte(byte b) {
      bado.writeByte(b);
      return this;
    }

    FixedLengthArcsBuffer writeVInt(int i) {
      try {
        bado.writeVInt(i);
      } catch (IOException e) { // Never thrown.
        throw new RuntimeException(e);
      }
      return this;
    }

    int getPosition() {
      return bado.getPosition();
    }

    /** Gets the internal byte array. */
    byte[] getBytes() {
      return bytes;
    }
  }
}
