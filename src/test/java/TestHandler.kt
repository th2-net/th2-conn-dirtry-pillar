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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.conn.dirty.pillar.handler.*
import com.exactpro.th2.conn.dirty.pillar.handler.util.LENGTH_FIELD_NAME
import com.exactpro.th2.conn.dirty.pillar.handler.util.TYPE_FIELD_NAME
import com.exactpro.th2.conn.dirty.tcp.core.TaskSequencePool
import com.exactpro.th2.conn.dirty.tcp.core.api.IContext
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandlerSettings
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Context
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.mock
import com.nhaarman.mockitokotlin2.mock
import org.junit.jupiter.api.Assertions.assertNull
import java.io.InputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.concurrent.Executor

class TestHandler {
    private val address: InetSocketAddress = mock(InetSocketAddress::class.java)
    private val secure = true
    private val sessionAlias: String = "sessionAlias"
    private val reconnectDelay: Long = 1000L
    private val handler: IProtocolHandler = mock(IProtocolHandler::class.java)
    private val mangler: IProtocolMangler = mock(IProtocolMangler::class.java)
    private val onEvent: (event: Event, parentEventId: EventID) -> Unit = mock {}
    private val onMessage: (RawMessage) -> Unit = mock { }
    private val eventLoopGroup: EventLoopGroup = NioEventLoopGroup()
    private val executor: Executor = mock(Executor::class.java)
    private val sequencePool = TaskSequencePool(executor)
    private val parentEventId = EventID.newBuilder().setId("root").build()!!

    private var channel = Channel(
        address,
        secure,
        sessionAlias,
        reconnectDelay,
        handler,
        mangler,
        onEvent,
        onMessage,
        eventLoopGroup,
        sequencePool,
        parentEventId
    )
    private val handlerSettings = PillarHandlerSettings()
    private val readDictionary: (DictionaryType) -> InputStream = mock { }
    private val sendEvent: (Event) -> Unit = mock { }
    private var context: IContext<IProtocolHandlerSettings> = Context(handlerSettings, readDictionary, sendEvent)
    private var settings = PillarHandlerSettings()

    @Test
    fun `sending LoginResponse`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(514)
            .writeShortLE(21)
            .writeBytes("username".encodeToByteArray())
            .writerIndex(20)
            .writeByte(0)

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        val message = pillarHandler.onReceive(buffer)
        assertEquals(21, buffer.readerIndex())

        val metadata = pillarHandler.onIncoming(message!!)
        message.readerIndex(4)
        val loginResponseMsg = LoginResponse(message)

        assertEquals(514, metadata[TYPE_FIELD_NAME]!!.toInt())
        assertEquals(21, metadata[LENGTH_FIELD_NAME]!!.toInt())
        assertEquals("username", loginResponseMsg.username)
        assertEquals(0, loginResponseMsg.status)

        buffer.writeShortLE(515)
            .writeShortLE(21)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(BigDecimal.valueOf(23).toInt())
            .writerIndex(41)
            .writeByte(1)

        pillarHandler.onReceive(buffer)
        assertEquals(42, buffer.readerIndex())

        var copyBuf = buffer.copy(21, 21)
        copyBuf.readerIndex(4)
        var streamAvail = StreamAvail(copyBuf)
        assertEquals(5, streamAvail.streamId.envId)
        assertEquals(4259845, streamAvail.streamId.sessNum)
        assertEquals(15, streamAvail.streamId.streamType)
        assertEquals(40287, streamAvail.streamId.userId)
        assertEquals(4, streamAvail.streamId.subId)
        assertEquals(BigDecimal.valueOf(23), streamAvail.nextSeq)
        assertEquals(1, streamAvail.access)


        buffer.writeShortLE(515)
            .writeShortLE(21)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(BigDecimal.valueOf(23).toInt())
            .writerIndex(62)
            .writeByte(2)

        pillarHandler.onReceive(buffer)
        assertEquals(63, buffer.readerIndex())

        copyBuf = buffer.copy(42, 21)
        copyBuf.readerIndex(4)
        streamAvail = StreamAvail(copyBuf)
        assertEquals(5, streamAvail.streamId.envId)
        assertEquals(4259845, streamAvail.streamId.sessNum)
        assertEquals(15, streamAvail.streamId.streamType)
        assertEquals(40287, streamAvail.streamId.userId)
        assertEquals(4, streamAvail.streamId.subId)
        assertEquals(BigDecimal.valueOf(23), streamAvail.nextSeq)
        assertEquals(2, streamAvail.access)
    }

    @Test
    fun `sending StreamAvail`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(515)
            .writeShortLE(21)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(BigDecimal.valueOf(23).toInt())
            .writerIndex(20)
            .writeByte(1)

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        pillarHandler.onReceive(buffer)
        assertEquals(21, buffer.readerIndex())
        buffer.readerIndex(4)
        val streamAvail = StreamAvail(buffer)
        assertEquals(5, streamAvail.streamId.envId)
        assertEquals(4259845, streamAvail.streamId.sessNum)
        assertEquals(15, streamAvail.streamId.streamType)
        assertEquals(40287, streamAvail.streamId.userId)
        assertEquals(4, streamAvail.streamId.subId)
        assertEquals(BigDecimal.valueOf(23), streamAvail.nextSeq)
        assertEquals(1, streamAvail.access)
    }

    @Test
    fun `sending OpenResponse`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(518)
            .writeShortLE(14)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(0)
            .writeByte(2)

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        val message = pillarHandler.onReceive(buffer)
        assertEquals(14, buffer.readerIndex())

        message!!.readerIndex(4)
        val openResponseMsg = OpenResponse(message)

        assertEquals(5, openResponseMsg.streamId.envId)
        assertEquals(4259845, openResponseMsg.streamId.sessNum)
        assertEquals(15, openResponseMsg.streamId.streamType)
        assertEquals(40287, openResponseMsg.streamId.userId)
        assertEquals(4, openResponseMsg.streamId.subId)
        assertEquals(0, openResponseMsg.status)
        assertEquals(2, openResponseMsg.access)
    }

    @Test
    fun `sending CloseResponse`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(520)
            .writeShortLE(13)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(0)

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        val message = pillarHandler.onReceive(buffer)
        assertEquals(13, buffer.readerIndex())

        val metadata = pillarHandler.onIncoming(message!!)
        message.readerIndex(4)
        val closeResponseMsg = CloseResponse(message)

        assertEquals(520.toString(), metadata[TYPE_FIELD_NAME])
        assertEquals(13.toString(), metadata[LENGTH_FIELD_NAME])

        assertEquals(5, closeResponseMsg.streamId.envId)
        assertEquals(4259845, closeResponseMsg.streamId.sessNum)
        assertEquals(15, closeResponseMsg.streamId.streamType)
        assertEquals(40287, closeResponseMsg.streamId.userId)
        assertEquals(4, closeResponseMsg.streamId.subId)
        assertEquals(0, closeResponseMsg.status)
    }

    @Test
    fun `sending SeqMsg`() {
        val time = LocalDateTime.parse("2021-12-27T13:39:14.524104")
        val seconds = time.toEpochSecond(ZoneOffset.UTC).toULong()
        val nanoseconds = time.nano.toULong()

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(2309)
            .writeShortLE(32)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(13)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(BigDecimal.valueOf(23).toInt())
            .writerIndex(20)
            .writeMedium(0)
            .writerIndex(24)
            .writeLongLE((seconds * 1_000_000_000UL + nanoseconds).toLong())

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        pillarHandler.onReceive(buffer)
        assertEquals(32, buffer.readerIndex())

        buffer.readerIndex(4)
        val seqMsg = SeqMsg(buffer)

        assertEquals(5, seqMsg.streamId.envId)
        assertEquals(4259845, seqMsg.streamId.sessNum)
        assertEquals(13, seqMsg.streamId.streamType)
        assertEquals(40287, seqMsg.streamId.userId)
        assertEquals(4, seqMsg.streamId.subId)
        assertEquals(BigDecimal.valueOf(23), seqMsg.seq)
        assertEquals(0, seqMsg.reserved1)
        assertEquals("2021-12-27T13:39:14.524104", seqMsg.timestamp.toString())
    }

    @Test
    fun `assembling Login`() {
        val settings = PillarHandlerSettings()
        val login = Login(settings).login()

        assertEquals(76, login.writerIndex())

        assertEquals(513, login.readUnsignedShortLE())
        assertEquals(76, login.readUnsignedShortLE())
        assertEquals("username", login.readCharSequence(16, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar()))
        assertEquals("password", login.readCharSequence(32, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar()))
        assertEquals("mic", login.readCharSequence(4, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar()))
        assertEquals("1.1", login.readCharSequence(20, StandardCharsets.US_ASCII).toString().trimEnd(0.toChar()))
    }

    @Test
    fun `assembling Open`() {
        val stream = byteArrayOf(
            5, 65, 0, 5, 15, -99, 95, 4
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(stream)
        val open = Open(StreamId(buffer), 23, 9999).open()

        assertEquals(30, open.writerIndex())

        assertEquals(517, open.readUnsignedShortLE())
        assertEquals(30, open.readUnsignedShortLE())
        assertEquals(5, open.readUnsignedByte())
        assertEquals(4259845, open.readMedium())
        assertEquals(15, open.readByte())
        assertEquals(40287, open.readUnsignedShort())
        assertEquals(4, open.readByte().toInt())
        open.readerIndex(12)
        val bytes = ByteArray(8)
        open.readBytes(bytes)
        bytes.reverse()
        assertEquals(BigDecimal.valueOf(23), BigInteger(1, bytes).toBigDecimal())
        open.readerIndex(28)
        assertEquals(2, open.readUnsignedByte())
        open.readerIndex(29)
        assertEquals(0, open.readUnsignedByte())
    }

    @Test
    fun `assembling SeqMsg`() {
        val time = LocalDateTime.parse("2021-12-27T13:39:14.524104")
        val seconds = time.toEpochSecond(ZoneOffset.UTC).toULong()
        val nanoseconds = time.nano.toULong()

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(2309)
            .writeShortLE(32)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)
            .writeByte(BigDecimal.valueOf(23).toInt())
            .writerIndex(20)
            .writeMedium(0)
            .writerIndex(24)
            .writeLongLE((seconds * 1_000_000_000UL + nanoseconds).toLong())

        buffer.readerIndex(4)
        val seqMsg = SeqMsg(buffer)
        val seqMsgToSend = SeqMsgToSend(1, seqMsg.streamId).seqMsg()

        assertEquals(32, seqMsgToSend.writerIndex())

        assertEquals(2309, seqMsgToSend.readUnsignedShortLE())
        assertEquals(32, seqMsgToSend.readUnsignedShortLE())
        assertEquals(5, seqMsgToSend.readUnsignedByte())
        assertEquals(4259845, seqMsgToSend.readMedium())
        assertEquals(15, seqMsgToSend.readByte())
        assertEquals(40287, seqMsgToSend.readUnsignedShort())
        assertEquals(4, seqMsgToSend.readByte().toInt())
    }

    @Test
    fun `assembling Close`() {
        val stream = byteArrayOf(
            5, 65, 0, 5, 15, -99, 95, 4
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(stream)
        val streamId = StreamIdEncode(StreamId(buffer))
        val close = Close(streamId.streamIdBuf).close()

        assertEquals(12, close.writerIndex())

        assertEquals(519, close.readUnsignedShortLE())
        assertEquals(12, close.readUnsignedShortLE())
        assertEquals(5, close.readUnsignedByte())
        assertEquals(4259845, close.readMedium())
        assertEquals(15, close.readByte())
        assertEquals(40287, close.readUnsignedShort())
        assertEquals(4, close.readByte().toInt())
    }

    @Test
    fun `invalid size message`() {
        val buffer: ByteBuf = Unpooled.buffer(4)
        buffer.writeShortLE(2309)
            .writeByte(32)
        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        assertNull(pillarHandler.onReceive(buffer))
        assertEquals(0, buffer.readerIndex())
    }

    @Test
    fun `empty message`() {
        val buffer: ByteBuf = Unpooled.buffer()
        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        assertNull(pillarHandler.onReceive(buffer))
        assertEquals(0, buffer.readerIndex())
    }

    @Test
    fun `message size is less than 4`() {
        val buffer: ByteBuf = Unpooled.buffer(3)
        buffer.writeShortLE(514)
            .writeByte(21)
        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        assertNull(pillarHandler.onReceive(buffer))
        assertEquals(0, buffer.readerIndex())
    }

    @Test
    fun `send onOutgoing`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(517)
            .writeShortLE(21)
            .writeByte(5)
            .writeMedium(4259845)
            .writeByte(15)
            .writeShort(40287)
            .writeByte(4)

        val pillarHandler = PillarHandler(context)
        val metadata = mutableMapOf<String, String>()
        metadata[TYPE_FIELD_NAME] = 500.toString()
        metadata[LENGTH_FIELD_NAME] = 21.toString()

        val onOutgoing = pillarHandler.onOutgoing(buffer, metadata)

        assertEquals(500.toString(), onOutgoing[TYPE_FIELD_NAME])
        assertEquals(21.toString(), onOutgoing[LENGTH_FIELD_NAME])
    }


    @Test
    fun `invalid type message`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(500)
            .writeShortLE(32)
        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        assertNull(pillarHandler.onReceive(buffer))
        assertEquals(0, buffer.readerIndex())
    }

    @Test
    fun `invalid heartbeat interval`() {
        handlerSettings.heartbeatInterval = 0
        val exception = assertThrows<IllegalArgumentException> { PillarHandler(context) }
        assertEquals("Heartbeat sending interval must be greater than zero.", exception.message)
    }

    @Test
    fun `invalid status in LoginResponse`() {
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeShortLE(514)
            .writeShortLE(21)
            .writeBytes("username".encodeToByteArray())
            .writerIndex(20)
            .writeByte(85)

        val pillarHandler = PillarHandler(context)
        pillarHandler.channel = channel
        val message = pillarHandler.onReceive(buffer)
        val exception = assertThrows<IllegalStateException> { pillarHandler.onIncoming(message!!) }
        assertEquals("Received STREAM_NOT_OPEN status.", exception.message)
    }

    @Test
    fun `invalid username Login`() {
        settings.username = "username_username"
        settings.password = String()
        val exception = assertThrows<IllegalArgumentException> { Login(settings) }
        assertEquals("Size of username exceeds allowed size or equal to zero.", exception.message)
    }

    @Test
    fun `invalid password Login`() {
        settings.password = String()
        val exception = assertThrows<IllegalArgumentException> { Login(settings) }
        assertEquals("Size of password exceeds allowed size or equal to zero.", exception.message)
    }

    @Test
    fun `invalid mic Login`() {
        settings.mic = "mic_mic"
        val exception = assertThrows<IllegalArgumentException> { Login(settings) }
        assertEquals("Size of mic exceeds allowed size or equal to zero.", exception.message)
    }

    @Test
    fun `invalid version Login`() {
        settings.version = String()
        val exception = assertThrows<IllegalArgumentException> { Login(settings) }
        assertEquals("Size of version exceeds allowed size or equal to zero.", exception.message)
    }
}