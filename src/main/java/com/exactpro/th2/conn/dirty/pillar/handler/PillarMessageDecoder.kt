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
import io.netty.buffer.ByteBufUtil
import org.checkerframework.checker.units.qual.Length
import java.lang.Exception
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
        try {
            type = byteBuf.readUnsignedShortLE()
            length = byteBuf.readUnsignedShortLE()
        }
        catch (e: Exception){
            throw Exception ("Unable to read header in message: ${ByteBufUtil.hexDump(byteBuf)}", e)
        }
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
        var offset = byteBuf.readerIndex()
        username = byteBuf.readCharSequence(16, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar())
        offset += 16
        byteBuf.readerIndex(offset)
        status = byteBuf.readUnsignedByte()
        require(byteBuf.readerIndex() == MessageType.LOGIN_RESPONSE.length){ "There are bytes left in buffer to read" }
    }

    override fun toString(): String {
        return "$username $status"
    }
}

class StreamAvail(byteBuf: ByteBuf) {
    val streamId: StreamId
    val nextSeq: BigDecimal
    val access: Short

    init {
        var offset = byteBuf.readerIndex()
        streamId = StreamId(byteBuf)

        offset += 8
        byteBuf.readerIndex(offset)
        val bytes = ByteArray(8)
        byteBuf.readBytes(bytes)
        bytes.reverse()
        nextSeq = BigInteger(1, bytes).toBigDecimal()

        offset += 8
        byteBuf.readerIndex(offset)
        access = byteBuf.readUnsignedByte()

        require(byteBuf.readerIndex() == MessageType.STREAM_AVAIL.length) { "There are bytes left in buffer to read" }
    }

    override fun toString(): String {
        return "${streamId.envId} ${streamId.sessNum} ${streamId.streamType} ${streamId.userId} ${streamId.subId} $nextSeq $access"
    }
}

class OpenResponse(byteBuf: ByteBuf) {
    val streamId: StreamId
    val status: Short
    val access: Short

    init {
        var offset = byteBuf.readerIndex()
        streamId = StreamId(byteBuf)

        offset += 8
        byteBuf.readerIndex(offset)
        status = byteBuf.readUnsignedByte()

        offset++
        byteBuf.readerIndex(offset)
        access = byteBuf.readUnsignedByte()
        require(byteBuf.readerIndex() == MessageType.OPEN_RESPONSE.length){ "There are bytes left in buffer to read" }
    }

    override fun toString(): String {
        return "${streamId.envId} ${streamId.sessNum} ${streamId.streamType} ${streamId.userId} ${streamId.subId} $status $access"
    }
}

class CloseResponse(byteBuf: ByteBuf){
    val streamId: StreamId
    val status: Short

    init {
        var offset = byteBuf.readerIndex()
        streamId = StreamId(byteBuf)

        offset += 8
        byteBuf.readerIndex(offset)
        status = byteBuf.readUnsignedByte()

        require(byteBuf.readerIndex() == MessageType.CLOSE_RESPONSE.length){ "There are bytes left in buffer to read" }
    }

    override fun toString(): String {
        return "${streamId.envId} ${streamId.sessNum} ${streamId.streamType} ${streamId.userId} ${streamId.subId} $status"
    }
}

class SeqMsg(byteBuf: ByteBuf, length: Int) {
    val streamId: StreamId
    val seq: BigDecimal
    val reserved1: ByteBuf
    val timestamp: LocalDateTime
    val payload: MsgHeader

    init {
        var offset = byteBuf.readerIndex()
        streamId = StreamId(byteBuf)

        offset += 8
        byteBuf.readerIndex(offset)
        val bytes = ByteArray(8)
        byteBuf.readBytes(bytes)
        bytes.reverse()
        seq = BigInteger(1, bytes).toBigDecimal()

        offset += 8
        reserved1 = byteBuf.copy(offset, length - MessageType.SEQMSG.length)

        offset += length - MessageType.SEQMSG.length
        byteBuf.readerIndex(offset)

        val time = byteBuf.readLongLE().toULong()
        val milliseconds = time / 1_000_000UL
        val nanoseconds = time % 1_000_000_000UL
        timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds.toLong()), ZoneOffset.UTC).withNano(
            nanoseconds.toInt())
        offset += 8
        byteBuf.readerIndex(offset)
        payload = MsgHeader(byteBuf)

        require(byteBuf.readerIndex() == length){ "There are bytes left in buffer to read" }
    }

    override fun toString(): String {
        return "${streamId.envId} ${streamId.sessNum} ${streamId.streamType} ${streamId.userId} ${streamId.subId} $seq $reserved1 $timestamp"
    }
}


