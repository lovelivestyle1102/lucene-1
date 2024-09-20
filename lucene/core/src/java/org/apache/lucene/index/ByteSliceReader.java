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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;

/* IndexInput that knows how to read the byte slices written
 * by Posting and PostingVector.  We read the bytes in
 * each slice until we hit the end of that slice at which
 * point we read the forwarding address of the next slice
 * and then jump to it.*/
final class ByteSliceReader extends DataInput {
  // slice所在的ByteBlockPool
  ByteBlockPool pool;

  // 使用pool中的哪个buffer
  int bufferUpto;

  // 当前使用的buffer
  byte[] buffer;

  public int upto;

  // 当前slice的结束位置，如果还有下一个slice的话，limit后面四个字节就是下个slice的起始位置
  int limit;

  // 当前读到的slice是哪个level
  int level;

  // buffer在pool中的起始位置的offset
  public int bufferOffset;

  // 这个数据源的slice链链表的最后一个slice的结束位置
  public int endIndex;

  // startIndex是在pool中第一个slice的起始位置
  public void init(ByteBlockPool pool, int startIndex, int endIndex) {

    assert endIndex - startIndex >= 0;

    assert startIndex >= 0;

    assert endIndex >= 0;

    this.pool = pool;

    this.endIndex = endIndex;

    // 初始level是0
    level = 0;

    // 根据startIndex计算当前使用的pool中的哪个buffer
    bufferUpto = startIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

    //final ByteBlockPool bytePool;
    buffer = pool.buffers[bufferUpto];

    // 在buffer中的offset
    upto = startIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    // 第一个slice的大小
    final int firstSize = ByteBlockPool.LEVEL_SIZE_ARRAY[0];

    if (startIndex + firstSize >= endIndex) {// 说明只有一个slice
      // There is only this one slice to read
      limit = endIndex & ByteBlockPool.BYTE_BLOCK_MASK;
      // 最后四个字节是下一个slice的起始位置，不属于正文内容。
      // 经过这一步，limit开始的四个字节就是下一个slice的位置
    } else limit = upto + firstSize - 4;
  }

  public boolean eof() {
    assert upto + bufferOffset <= endIndex;
    return upto + bufferOffset == endIndex;
  }

  @Override
  public byte readByte() {
    assert !eof();
    assert upto <= limit;
    if (upto == limit) nextSlice();
    return buffer[upto++];
  }

  public long writeTo(DataOutput out) throws IOException {
    long size = 0;
    while (true) {
      if (limit + bufferOffset == endIndex) {
        assert endIndex - bufferOffset >= upto;
        out.writeBytes(buffer, upto, limit - upto);
        size += limit - upto;
        break;
      } else {
        out.writeBytes(buffer, upto, limit - upto);
        size += limit - upto;
        nextSlice();
      }
    }

    return size;
  }

  public void nextSlice() {

    // Skip to our next slice
    // limit开始的四个字节就是下一个slice的位置
    final int nextIndex = (int) BitUtil.VH_LE_INT.get(buffer, limit);

    // 下一个slice的level
    level = ByteBlockPool.NEXT_LEVEL_ARRAY[level];

    // 下一个slice的大小
    final int newSize = ByteBlockPool.LEVEL_SIZE_ARRAY[level];

    bufferUpto = nextIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

    buffer = pool.buffers[bufferUpto];
    upto = nextIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    if (nextIndex + newSize >= endIndex) {
      // We are advancing to the final slice
      assert endIndex - nextIndex > 0;
      // 如果是最后一个slice
      limit = endIndex - bufferOffset;
    } else {
      // This is not the final slice (subtract 4 for the
      // forwarding address at the end of this new slice)
      // 最后四个字节是下一个slice的起始位置，不属于正文内容。
      // 经过这一步，limit开始的四个字节就是下一个slice的位置
      limit = upto + newSize - 4;
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    while (len > 0) {
      final int numLeft = limit - upto;
      if (numLeft < len) {
        // Read entire slice
        System.arraycopy(buffer, upto, b, offset, numLeft);
        offset += numLeft;
        len -= numLeft;
        nextSlice();
      } else {
        // This slice is the last one
        System.arraycopy(buffer, upto, b, offset, len);
        upto += len;
        break;
      }
    }
  }

  @Override
  public void skipBytes(long numBytes) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    while (numBytes > 0) {
      final int numLeft = limit - upto;
      if (numLeft < numBytes) {
        numBytes -= numLeft;
        nextSlice();
      } else {
        upto += numBytes;
        break;
      }
    }
  }
}
