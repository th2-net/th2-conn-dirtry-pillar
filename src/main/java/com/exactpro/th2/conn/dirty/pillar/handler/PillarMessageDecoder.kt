/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.conn.dirty.pillar.handler

import com.exactpro.th2.conn.dirty.pillar.handler.util.MessageType
import io.netty.buffer.ByteBuf
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

class MsgHeader(byteBuf: ByteBuf) {
    val type: Int
    val length: Int

    init {
        byteBuf.readerIndex(0)
        type = byteBuf.readUnsignedShortLE()
        length = byteBuf.readUnsignedShortLE()
    }
}

class StreamId(byteBuf: ByteBuf){
    val envId: Short
    val sessNum: Int
    var streamType: Byte
    val userId: Int
    val subId: Int

    init {
        envId = byteBuf.readUnsignedByte()
        sessNum = byteBuf.readMedium()
        streamType = byteBuf.readByte()
        userId = byteBuf.readUnsignedShort()
        subId = byteBuf.readByte().toInt()
    }
}

class LoginResponse(byteBuf: ByteBuf) {
    val username: String
    val status: Short

    init {
        byteBuf.markReaderIndex()
        username = byteBuf.readCharSequence(16, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar())
        byteBuf.readerIndex(20)
        status = byteBuf.readUnsignedByte()
        require(byteBuf.readerIndex() == MessageType.LOGIN_RESPONSE.length){ "There are bytes left in buffer to read" }
    }
}

class StreamAvail(byteBuf: ByteBuf) {
    val streamId: StreamId
    val nextSeq: BigDecimal
    val access: Short

    init {
        byteBuf.markReaderIndex()
        streamId = StreamId(byteBuf)

        byteBuf.readerIndex(12)
        val bytes = ByteArray(8)
        byteBuf.readBytes(bytes)
        bytes.reverse()
        nextSeq = BigInteger(1, bytes).toBigDecimal()

        byteBuf.readerIndex(20)
        access = byteBuf.readUnsignedByte()

        require(byteBuf.readerIndex() == MessageType.STREAM_AVAIL.length){ "There are bytes left in buffer to read" }
    }
}

class OpenResponse(byteBuf: ByteBuf) {
    val streamId: StreamId
    val status: Short
    val access: Short

    init {
        byteBuf.markReaderIndex()
        streamId = StreamId(byteBuf)
        byteBuf.readerIndex(12)
        status = byteBuf.readUnsignedByte()
        byteBuf.readerIndex(13)
        access = byteBuf.readUnsignedByte()

        require(byteBuf.readerIndex() == MessageType.OPEN_RESPONSE.length){ "There are bytes left in buffer to read" }
    }
}

class CloseResponse(byteBuf: ByteBuf){
    val streamId: StreamId
    val status: Short

    init {
        byteBuf.markReaderIndex()
        streamId = StreamId(byteBuf)
        byteBuf.readerIndex(12)
        status = byteBuf.readUnsignedByte()

        require(byteBuf.readerIndex() == MessageType.CLOSE_RESPONSE.length){ "There are bytes left in buffer to read" }
    }
}

class SeqMsg(byteBuf: ByteBuf) {
    val streamId: StreamId
    val seq: BigDecimal
    val reserved1: Int
    val timestamp: LocalDateTime

    init {
        byteBuf.markReaderIndex()
        streamId = StreamId(byteBuf)

        byteBuf.readerIndex(12)
        val bytes = ByteArray(8)
        byteBuf.readBytes(bytes)
        bytes.reverse()
        seq = BigInteger(1, bytes).toBigDecimal()

        byteBuf.readerIndex(20)
        reserved1 = byteBuf.readMedium()
        byteBuf.readerIndex(24)

        val time = byteBuf.readLongLE().toULong()
        val milliseconds = time / 1_000_000UL
        val nanoseconds = time % 1_000_000_000UL
        timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds.toLong()), ZoneOffset.UTC).withNano(
            nanoseconds.toInt())
        require(byteBuf.readerIndex() == MessageType.SEQMSG.length){ "There are bytes left in buffer to read" }
    }
}


