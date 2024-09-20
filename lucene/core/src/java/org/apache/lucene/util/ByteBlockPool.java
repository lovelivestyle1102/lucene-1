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
package org.apache.lucene.util;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.Arrays;
import java.util.List;

/**
 * Class that Posting and PostingVector use to write byte streams into shared fixed-size byte[]
 * arrays. The idea is to allocate slices of increasing lengths For example, the first slice is 5
 * bytes, the next slice is 14, etc. We start by writing our bytes into the first 5 bytes. When we
 * hit the end of the slice, we allocate the next slice and then write the address of the new slice
 * into the last 4 bytes of the previous slice (the "forwarding address").
 *
 * <p>Each slice is filled with 0's initially, and we mark the end with a non-zero byte. This way
 * the methods that are writing into the slice don't need to record its length and instead allocate
 * a new slice once they hit a non-zero byte.
 *
 * @lucene.internal
 */
public final class  ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  // 控制buffer的大小，也就是每行的大小。
  public static final int BYTE_BLOCK_SHIFT = 15;

  // buffer的大小
  public static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;

  // 用来做位运算定位buffer中的位置
  public static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    public DirectAllocator() {
      this(BYTE_BLOCK_SIZE);
    }

    public DirectAllocator(int blockSize) {
      super(blockSize);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {}
  }

  /** A simple {@link Allocator} that never recycles, but tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;

    public DirectTrackingAllocator(Counter bytesUsed) {
      this(BYTE_BLOCK_SIZE, bytesUsed);
    }

    public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end - start) * blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  }
  ;

  /**
   * 连续存放bytes的容器，是个二维数组
   *
   * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this
   * outside of this class.
   */
  // 真正用来存数据的空间。buffers[i]可以理解成第i个buffer
  public byte[][] buffers = new byte[10][];

  /**
   * 当前在第几行
   *
   * index into the buffers array pointing to the current buffer used as the head
   */
  private int bufferUpto = -1; // Which buffer we are upto

  /**
   * 当前在第几列
   *
   * Where we are in head buffer
   */
  // 在buffer中的偏移量（从二维数组的角度就是第几列）
  public int byteUpto = BYTE_BLOCK_SIZE;

  /**
   *  当前正在使用的那一行的指针
   *
   * Current head buffer
   */
  // 当前正在使用的buffer
  public byte[] buffer;

  /**
   *
   *
   * Current head offset
   */
  // 当前正在使用的buffer的起始位置在整个pool中的偏移量
  public int byteOffset = -BYTE_BLOCK_SIZE;

  // 用来创建新buffer的，也就是字节数组
  private final Allocator allocator;

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }

  /**
   * 重置pool
   *
   * Resets the pool to its initial state reusing the first buffer and fills all buffers with <code>
   * 0</code> bytes before they reused or passed to {@link Allocator#recycleByteBlocks(byte[][],
   * int, int)}. Calling {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }

  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling {@link
   * ByteBlockPool#nextBuffer()} is not needed after reset.
   *
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>. This
   *     should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling {@link
   *     ByteBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before
   *     ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  // zeroFillBuffers：是否把所有的buffer都填充为0
  // reuseFirst：是否需要保留复用第一个buffer
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    // 如果pool使用过，才需要进行重置
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      // 为所有的用过的buffer填充0
      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        // 最后一个使用到的buffer只填充用过的部分
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }

      // 除了第一个buffer，其他buffer都回收
      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, 1 + bufferUpto, null);
      }

      // 如果需要复用第一个buffer，则重置相关参数
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        byteUpto = 0;
        byteOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        byteUpto = BYTE_BLOCK_SIZE;
        byteOffset = -BYTE_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   *
   * 移动当前游标到下一行，每行就是个buffer
   *
   * Advances the pool to its next buffer. This method should be called once after the constructor
   * to initialize the pool. In contrast to the constructor a {@link ByteBlockPool#reset()} call
   * will advance the pool to its first buffer immediately.
   */
  public void nextBuffer() {
    // 如果buffer数组满了，则进行扩容
    if (1 + bufferUpto == buffers.length) {
      byte[][] newBuffers =
          new byte[ArrayUtil.oversize(buffers.length + 1, NUM_BYTES_OBJECT_REF)][];

      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);

      buffers = newBuffers;
    }

    // 创建下一个buffer
    // 这里重点关注下，创建新buffer之后所有位置参数都定位到新buffer，所以放弃了上一个buffer的剩余空间
    buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();

    //移动到下一个，标记已经使用过的
    bufferUpto++;

    //重置写入位置
    byteUpto = 0;

    byteOffset += BYTE_BLOCK_SIZE;
  }

  /**
   * 给定一个长度，分配一个新slice
   *
   * Allocates a new slice with the given size.
   *
   * @see ByteBlockPool#FIRST_LEVEL_SIZE
   */
  public int newSlice(final int size) {
    // 如果当前buffer剩余空间无法容纳新的slice，则创建一个新的buffer
    if (byteUpto > BYTE_BLOCK_SIZE - size) nextBuffer();

    final int upto = byteUpto;

    byteUpto += size;

    // slice结束的哨兵，要理解成level=0，哨兵是  16|level
    buffer[byteUpto - 1] = 16;

    return upto;
  }

  // Size of each slice.  These arrays should be at most 16
  // elements (index is encoded with 4 bits).  First array
  // is just a compact way to encode X+1 with a max.  Second
  // array is the length of each slice, ie first slice is 5
  // bytes, next slice is 14 bytes, etc.

  /**
   *  快速定位
   *
   * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY} to quickly navigate
   * to the next slice level.
   */
  // 下标是当前的level，值是下一个level的值。可以看到是从level 0开始的，到level 9之后就不变了。
  public static final int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};

  /**
   *  每层slice容量
   *
   * An array holding the level sizes for byte slices.
   */
  // 下标是level，值是level对应的slice的大小。
  public static final int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};

  /**
   * The first level size for new slices
   *
   * @see ByteBlockPool#newSlice(int)
   */
  public static final int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  /**
   * 给定一个buffer，以及buffer的偏移，分配一个新slice
   *
   * Creates a new byte slice with the given starting size and returns the slices offset in the
   * pool.
   */
  public int allocSlice(final byte[] slice, final int upto) {

    // 当前slice 的upto是非0的，把这个数字拿出来跟0b1111做个交操作，得到一个level
    final int level = slice[upto] & 15;

    //去NEXT_LEVEL_ARRAY 找下一个level
    final int newLevel = NEXT_LEVEL_ARRAY[level];

    //根据这个level去得到需要分配多大空间给这个slice
    final int newSize = LEVEL_SIZE_ARRAY[newLevel];

    // Maybe allocate another block
    //当然有可能出现物理上不够用的情况，那就需要扩到下一行到下一个buffer
    if (byteUpto > BYTE_BLOCK_SIZE - newSize) {
      //注意如果出现这个操作byteUpto，byteOffset，buffer都会因为移动到下一行而变掉
      nextBuffer();
    }

    //当前blockPool写入buffer的相对偏移，通常upto的下一个就是
    final int newUpto = byteUpto;

    //算出在整个pool里面的绝对偏移
    final int offset = newUpto + byteOffset;

    //相对偏移向右移动newSize个单位
    byteUpto += newSize;

    // Copy forward the past 3 bytes (which we are about to overwrite with the forwarding address).
    // We actually copy 4 bytes at once since VarHandles make it cheap.
    int past3Bytes = ((int) BitUtil.VH_LE_INT.get(slice, upto - 3)) & 0xFFFFFF;

    // Ensure we're not changing the content of `buffer` by setting 4 bytes instead of 3. This
    // should never happen since the next `newSize` bytes must be equal to 0.
    assert buffer[newUpto + 3] == 0;

    BitUtil.VH_LE_INT.set(buffer, newUpto, past3Bytes);

    // Write forwarding address at end of last slice:
    BitUtil.VH_LE_INT.set(slice, upto - 3, offset);

    // 往新的byteUpto的前一位里面塞入level信息
    // Write new level:
    buffer[byteUpto - 1] = (byte) (16 | newLevel);

    //返回可以写入元素的位置，也就是那个x所在的位置
    return newUpto + 3;
  }

  /**
   * 把一个二进制流放到pool指定位置里
   *
   * Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice. This
   * will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses the
   * provided {@link BytesRefBuilder} to copy bytes over.
   */
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    // offset是在哪个buffer
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];

    // offset在buffer中的位置
    int pos = (int) (offset & BYTE_BLOCK_MASK);

    // 如果所需内容在一个buffer内
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly
      // without copying
      // 直接设置buffer的offset和length
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      // 跨buffer通过readBytes方法读取
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  // Fill in a BytesRef from term's length & bytes encoded in
  // byte block
  public void setBytesRef(BytesRef term, int textStart) {
    // 定位到 textStart 属于哪个buffer
    final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];

    // 定位到 textStart 在buffer中的位置
    int pos = textStart & BYTE_BLOCK_MASK;


    if ((bytes[pos] & 0x80) == 0) {// 如果是一个字节的长度
      // length is 1 byte
      term.length = bytes[pos];
      term.offset = pos + 1;
    } else { // 2字节长度
      // length is 2 bytes
      term.length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      term.offset = pos + 2;
    }
    assert term.length >= 0;
  }

  /**
   * 把一个二进制对象追加到pool里
   *
   * Appends the bytes in the provided {@link BytesRef} at the current position.
   */
  public void append(final BytesRef bytes) {
    int bytesLeft = bytes.length;
    int offset = bytes.offset;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) {
        // fits within current buffer
        System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else {
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }

  /**
   * Reads bytes out of the pool starting at the given offset with the given length into the given
   * byte array at offset <code>off</code>.
   *
   * <p>Note: this method allows to copy across block boundaries.
   */
  public void readBytes(final long offset, final byte[] bytes, int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    while (bytesLeft > 0) {
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Set the given {@link BytesRef} so that its content is equal to the {@code ref.length} bytes
   * starting at {@code offset}. Most of the time this method will set pointers to internal
   * data-structures. However, in case a value crosses a boundary, a fresh copy will be returned. On
   * the contrary to {@link #setBytesRef(BytesRef, int)}, this does not expect the length to be
   * encoded with the data.
   */
  public void setRawBytesRef(BytesRef ref, final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + ref.length <= BYTE_BLOCK_SIZE) {
      ref.bytes = buffers[bufferIndex];
      ref.offset = pos;
    } else {
      ref.bytes = new byte[ref.length];
      ref.offset = 0;
      readBytes(offset, ref.bytes, 0, ref.length);
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }
}
