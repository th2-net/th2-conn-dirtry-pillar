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

package com.exactpro.th2.conn.ditry.pillar.handler

import com.exactpro.sf.util.DateTimeUtility
import com.exactpro.th2.conn.ditry.pillar.handler.util.*
import io.netty.buffer.ByteBuf
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime

class MsgHeader(byteBuf: ByteBuf) {
    val type: Int
    private val length: Int

    init {
        type = byteBuf.readShort().toInt()
        length = byteBuf.readShort().toInt()
    }

    fun msgHeader(): Map<String, String> {
        val header = mutableMapOf<String, String>()
        header[TYPE_FIELD_NAME] = type.toString()
        header[LENGTH_FIELD_NAME] = length.toString()
        return header
    }
}

class StreamId(byteBuf: ByteBuf){
    val envId: Short
    val sessNum: Int
    val streamType: Byte
    val userId: UShort
    val subId: Int

    init {
            byteBuf.readerIndex(4)
            envId = byteBuf.readUnsignedByte()
            sessNum = byteBuf.readMedium()
            streamType = byteBuf.readByte()
            userId = byteBuf.readShort().toUShort()
            subId = byteBuf.readByte().toInt()
    }

    fun streamId(): Map<String, String> {
        val stream = mutableMapOf<String, String>()
        stream[ENV_ID_FIELD_NAME] = envId.toString()
        stream[SESS_NUM_FIELD_NAME] = sessNum.toString()
        stream[STREAM_TYPE_FIELD_NAME] = streamType.toString()
        stream[USER_ID_FIELD_NAME] = userId.toString()
        stream[SUB_ID_FIELD_NAME] = subId.toString()
        return stream
    }
}

class LoginResponse(byteBuf: ByteBuf) {
    private val charset = StandardCharsets.US_ASCII
    private val header: MsgHeader
    private val username: String
    private val status: Byte

    init {
        byteBuf.markReaderIndex()
        byteBuf.readerIndex(0)
        header = MsgHeader(byteBuf)
        byteBuf.readerIndex(4)
        username = byteBuf.readCharSequence(16, charset).toString().trimEnd(0.toChar())
        byteBuf.readerIndex(20)
        status = byteBuf.readUnsignedByte().toByte()
    }

    fun loginResponse(): Map <String, String>{
        val lResponse = mutableMapOf<String, String>()
        lResponse.putAll(header.msgHeader())
        lResponse[USERNAME_FIELD_NAME] = username
        lResponse[STATUS_FIELD_NAME] = status.toString()
        return lResponse
    }
}

class StreamAvail(byteBuf: ByteBuf) {
    private val header: MsgHeader
    private val streamId: StreamId
    private val nextSeq: BigDecimal
    private val access: Int

    init {
        byteBuf.markReaderIndex()
        byteBuf.readerIndex(0)
        header = MsgHeader(byteBuf)
        byteBuf.readerIndex(4)
        streamId = StreamId(byteBuf)
        byteBuf.readerIndex(12)
        val bytes = ByteArray(8)
        byteBuf.readBytes(bytes)
        bytes.reverse()
        nextSeq = BigInteger(1, bytes).toBigDecimal()
        byteBuf.readerIndex(20)
        access = byteBuf.readUnsignedByte().toInt()
    }

    fun streamAvail(): Map <String, String>{
        val strAvail = mutableMapOf<String, String>()
        strAvail.putAll(header.msgHeader())
        strAvail.putAll(streamId.streamId())
        strAvail[NEXT_SEQ_FIELD_NAME] = nextSeq.toString()
        strAvail[ACCESS_FIELD_NAME] = access.toString()
        return strAvail
    }
}

class OpenResponse(byteBuf: ByteBuf) {
    private val header: MsgHeader
    private val streamId: StreamId
    private val status: Int
    private val access: Int

    init {
        byteBuf.markReaderIndex()
        byteBuf.readerIndex(0)
        header = MsgHeader(byteBuf)
        byteBuf.readerIndex(4)
        streamId = StreamId(byteBuf)
        byteBuf.readerIndex(12)
        status = byteBuf.readUnsignedByte().toInt()
        byteBuf.readerIndex(13)
        access = byteBuf.readUnsignedByte().toInt()
    }

    fun openResponse(): Map<String, String> {
        val oResponse = mutableMapOf<String, String>()
        oResponse.putAll(header.msgHeader())
        oResponse.putAll(streamId.streamId())
        oResponse[STATUS_FIELD_NAME] = status.toString()
        oResponse[ACCESS_FIELD_NAME] = access.toString()
        return oResponse
    }
}

class CloseResponse(byteBuf: ByteBuf){
    private val header: MsgHeader
    private val streamId: StreamId
    private val status: Int

    init {
        byteBuf.markReaderIndex()
        byteBuf.readerIndex(0)
        header = MsgHeader(byteBuf)
        byteBuf.readerIndex(4)
        streamId = StreamId(byteBuf)
        byteBuf.readerIndex(12)
        status = byteBuf.readUnsignedByte().toInt()
    }

    fun closeResponse(): Map<String, String> {
        val oResponse = mutableMapOf<String, String>()
        oResponse.putAll(header.msgHeader())
        oResponse.putAll(streamId.streamId())
        oResponse[STATUS_FIELD_NAME] = status.toString()
        return oResponse
    }
}

class SeqMsg(byteBuf: ByteBuf) {
    private val header: MsgHeader
    private val streamId: StreamId
    private val seqmsg: Int
    private val reserved1: Int
    private val timestamp: LocalDateTime

    init {
        byteBuf.markReaderIndex()
        byteBuf.readerIndex(0)
        header = MsgHeader(byteBuf)
        byteBuf.readerIndex(4)
        streamId = StreamId(byteBuf)
        seqmsg = byteBuf.readByte().toInt()
        byteBuf.readerIndex(20)
        reserved1 = byteBuf.readMedium()
        byteBuf.readerIndex(24)
        val time = byteBuf.readLongLE().toULong()
        val milliseconds = time / 1_000_000UL
        val nanoseconds = time % 1_000_000_000UL
        timestamp = DateTimeUtility.toLocalDateTime(milliseconds.toLong(), nanoseconds.toInt())
    }

    fun seqMsg(): Map<String, String> {
        val sMsg = mutableMapOf<String, String>()
        sMsg.putAll(header.msgHeader())
        sMsg.putAll(streamId.streamId())
        sMsg[SEQMSG_ID_FIELD_NAME] = seqmsg.toString()
        sMsg[RESERVED1_FIELD_NAME] = reserved1.toString()
        sMsg[TIMESTAMP_FIELD_NAME] = timestamp.toString()
        return sMsg
    }
}


