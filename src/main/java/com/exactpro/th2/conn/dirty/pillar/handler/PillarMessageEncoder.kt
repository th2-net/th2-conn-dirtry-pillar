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

import com.exactpro.th2.conn.dirty.pillar.handler.util.Access
import com.exactpro.th2.conn.dirty.pillar.handler.util.MODE_LOSSY
import com.exactpro.th2.conn.dirty.pillar.handler.util.MessageType
import com.exactpro.th2.conn.dirty.pillar.handler.util.StreamType
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.ZoneOffset

class Heartbeat{
    val heartbeat: ByteBuf = Unpooled.buffer(MessageType.HEARTBEAT.length)

    init {
        heartbeat.writeShortLE(MessageType.HEARTBEAT.type)
        heartbeat.writeShortLE(MessageType.HEARTBEAT.length)
    }
}

class StreamIdEncode(var streamId: StreamId) {
    val streamIdBuf: ByteBuf = Unpooled.buffer(8)

    init {
        streamIdBuf.writeByte(streamId.envId.toInt())
        streamIdBuf.writeMedium(streamId.sessNum)
        streamIdBuf.writeByte(streamId.streamType.toInt())
        streamIdBuf.writeShort(streamId.userId)
        streamIdBuf.writeByte(streamId.subId)
    }
}

class Login(settings: PillarHandlerSettings) {
    private val type: Int = MessageType.LOGIN.type
    private val length: Int = MessageType.LOGIN.length
    private val username: ByteArray
    private val password: ByteArray
    private val mic: ByteArray
    private val version: ByteArray

    init {
        username = settings.username.encodeToByteArray()
        require(username.size <= 16 && username.isNotEmpty()) { "Size of username exceeds allowed size or equal to zero." }
        password = settings.password.encodeToByteArray()
        require(password.size <= 32 && password.isNotEmpty()) { "Size of password exceeds allowed size or equal to zero." }
        mic = settings.mic.encodeToByteArray()
        require(mic.size <= 4 && mic.isNotEmpty()) { "Size of mic exceeds allowed size or equal to zero." }
        version = settings.version.encodeToByteArray()
        require(version.size <= 20 && version.isNotEmpty()) { "Size of version exceeds allowed size or equal to zero." }
    }

    fun login (): ByteBuf{
        val loginMessage: ByteBuf = Unpooled.buffer(length)

        loginMessage.markWriterIndex()
        loginMessage.writeShortLE(type)
        loginMessage.writeShortLE(length)

        loginMessage.writerIndex(4)
        loginMessage.writeBytes(username)

        loginMessage.writerIndex(20)
        loginMessage.writeBytes(password)

        loginMessage.writerIndex(52)
        loginMessage.writeBytes(mic)

        loginMessage.writerIndex(56)
        loginMessage.writeBytes(version)

        loginMessage.writerIndex(76)

        require ( loginMessage.writerIndex() == length){ "Message size exceeded." }
        return loginMessage
    }
}

class Open(private val streamId: StreamId,
           private val startSeq: BigDecimal){
    private val type: Int = MessageType.OPEN.type
    private val length: Int = MessageType.OPEN.length

    fun open(): ByteBuf{
        val openMessage: ByteBuf = Unpooled.buffer(length)

        openMessage.markWriterIndex()
        openMessage.writeShortLE(type)
        openMessage.writeShortLE(length)

        openMessage.writerIndex(4)
        openMessage.writeBytes(StreamIdEncode(streamId).streamIdBuf)

        openMessage.writerIndex(12)
        openMessage.writeLongLE(startSeq.toLong())
        openMessage.writerIndex(20)
        openMessage.writeLongLE(startSeq.toLong()+1) //TODO

        openMessage.writerIndex(28)
        if (streamId.streamType == StreamType.REF.value || streamId.streamType == StreamType.GT.value)
            openMessage.writeByte(Access.READ.value.toInt())
        else if (streamId.streamType == StreamType.TG.value)
            openMessage.writeByte(Access.WRITE.value.toInt())

        openMessage.writeByte(MODE_LOSSY)

        require (openMessage.writerIndex() == length){ "Message size exceeded." }
        return openMessage
    }
}

class SeqMsgToSend(private val seqmsg: SeqMsg){
    private val type: Int = MessageType.SEQMSG.type
    private val length: Int = MessageType.SEQMSG.length

    fun seqMsg(): ByteBuf {
        val seqMsgMessage: ByteBuf = Unpooled.buffer(length)

        seqMsgMessage.markWriterIndex()
        seqMsgMessage.writeShortLE(type)
        seqMsgMessage.writeShortLE(length)

        seqMsgMessage.writerIndex(4)
        seqmsg.streamId.streamType = StreamType.TG.value
        seqMsgMessage.writeBytes(StreamIdEncode(seqmsg.streamId).streamIdBuf)
        seqMsgMessage.writerIndex(12)
        seqMsgMessage.writeByte(seqmsg.seq.toInt())
        seqMsgMessage.writerIndex(20)
        seqMsgMessage.writeInt(seqmsg.reserved1)

        seqMsgMessage.writerIndex(24)
        val time = LocalDateTime.now()
        val seconds = time.toEpochSecond(ZoneOffset.UTC).toULong()
        val nanoseconds = time.nano.toULong()
        seqMsgMessage.writeLongLE((seconds * 1_000_000_000UL + nanoseconds).toLong())

        require (seqMsgMessage.writerIndex() == length){ "Message size exceeded." }
        return seqMsgMessage
    }
}

class Close(private  val streamId: ByteBuf) {
    private val type: Int = MessageType.CLOSE.type
    private val length: Int = MessageType.CLOSE.length

    fun close(): ByteBuf {
        val closeMessage: ByteBuf = Unpooled.buffer(length)

        closeMessage.writeShortLE(type)
        closeMessage.writeShortLE(length)
        closeMessage.writeBytes(streamId)

        require (closeMessage.writerIndex() == length){ "Message size exceeded." }
        return closeMessage
    }
}
