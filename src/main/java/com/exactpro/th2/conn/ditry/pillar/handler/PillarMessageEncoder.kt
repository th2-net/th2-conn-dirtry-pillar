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

import com.exactpro.th2.conn.ditry.pillar.handler.util.Access
import com.exactpro.th2.conn.ditry.pillar.handler.util.MODE_LOSSY
import com.exactpro.th2.conn.ditry.pillar.handler.util.MessageType
import com.exactpro.th2.conn.ditry.pillar.handler.util.StreamType
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

class Heartbeat{
    private val type: Int = MessageType.HEARTBEAT.type
    private val length: Int = MessageType.HEARTBEAT.length

    fun heartbeat (): ByteBuf {
        val beat: ByteBuf = Unpooled.buffer(length)
        beat.writeShort(type)
        beat.writeShort(length)
        return beat
    }
}

class StreamIdEncode(var streamId: StreamId){
    private val streamIdBuf: ByteBuf = Unpooled.buffer(8)

    fun streamId(): ByteBuf{
        streamIdBuf.writeByte(streamId.envId.toInt())
        streamIdBuf.writeMedium(streamId.sessNum)
        streamIdBuf.writeByte(streamId.streamType.toInt())
        streamIdBuf.writeShort(streamId.userId.toInt())
        streamIdBuf.writeByte(streamId.subId)
        return streamIdBuf
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
        password = settings.password.encodeToByteArray()
        mic = settings.mic.encodeToByteArray()
        version = settings.version.encodeToByteArray()
    }

    fun login (): ByteBuf{
        val loginMessage: ByteBuf = Unpooled.buffer(length)
        loginMessage.markWriterIndex()

        loginMessage.writeShort(type)
        loginMessage.writeShort(length)

        loginMessage.writeBytes(username)

        loginMessage.writerIndex(20)
        loginMessage.writeBytes(password)

        loginMessage.writerIndex(52)
        loginMessage.writeBytes(mic)

        loginMessage.writerIndex(56)
        loginMessage.writeBytes(version)
        return loginMessage
    }
}

class Open(private val streamId: ByteBuf,
           private val startSeq: Int,
           private val streamType: Int){
    private val type: Int = MessageType.OPEN.type
    private val length: Int = MessageType.OPEN.length

    fun open(): ByteBuf{
        val openMessage: ByteBuf = Unpooled.buffer(length)
        openMessage.markWriterIndex()

        openMessage.writeShort(type)
        openMessage.writeShort(length)

        openMessage.writeBytes(streamId)

        openMessage.writerIndex(12)
        openMessage.writeLongLE(startSeq.toLong())

        openMessage.writerIndex(20)
        openMessage.writeLongLE(startSeq.toLong()+1)

        openMessage.writerIndex(28)
        if (streamType == StreamType.REF.value || streamType == StreamType.GT.value)
            openMessage.writeByte(Access.READ.value.toInt())
        else if (streamType == StreamType.TG.value)
            openMessage.writeByte(Access.WRITE.value.toInt())

        openMessage.writerIndex(29)
        openMessage.writeByte(MODE_LOSSY)

        return openMessage
    }
}

class Close(private  val streamId: ByteBuf) {
    private val type: Int = MessageType.CLOSE.type
    private val length: Int = MessageType.CLOSE.length

    fun close(): ByteBuf {
        val closeMessage: ByteBuf = Unpooled.buffer(length)
        closeMessage.markWriterIndex()

        closeMessage.writeShort(type)
        closeMessage.writeShort(length)

        closeMessage.writeBytes(streamId)

        return closeMessage
    }
}
