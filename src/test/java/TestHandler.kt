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
import com.exactpro.th2.conn.dirty.tcp.core.ActionStreamExecutor
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import com.exactpro.th2.conn.ditry.pillar.handler.*
import com.exactpro.th2.conn.ditry.pillar.handler.util.*
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
import java.math.BigDecimal
import java.net.InetSocketAddress
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
    private val onError: (stream: String, error: Throwable) -> Unit = mock {}
    private val actionExecutor = ActionStreamExecutor(executor, 1000, onError)
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
        actionExecutor,
        parentEventId
    )
    private var settings = PillarHandlerSettings()

    @Test
    fun `sending LoginResponse with extra bytes`() {
        val raw = byteArrayOf(
            2, 2, //type,
            0, 21, //length
            117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
            0, //status
            117, 115, 101 //extra bytes
        )

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val message = pillarHandler.onReceive(buffer)
        val metadata = pillarHandler.onIncoming(message!!)
        val loginResponseMsg = LoginResponse(message)

        assertEquals(514.toString(), metadata[TYPE_FIELD_NAME])
        assertEquals(21.toString(), metadata[LENGTH_FIELD_NAME])

        assertEquals("username", loginResponseMsg.username)
        assertEquals(0, loginResponseMsg.status)
    }

    @Test
    fun `sending StreamAvail`() {
        val raw = byteArrayOf(
            2, 3, //type,
            0, 21, //length
            5, 65, 0, 5, 15, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            23, 0, 0, 0, 0, 0, 0, 0, //next_seq
            6 //access
        )

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val message = pillarHandler.onReceive(buffer)
        val streamAvail = StreamAvail(message!!)

        assertEquals(515, streamAvail.header.type)
        assertEquals(21, streamAvail.header.length)
        assertEquals(5, streamAvail.streamId.envId)
        assertEquals(4259845, streamAvail.streamId.sessNum)
        assertEquals(15, streamAvail.streamId.streamType)
        assertEquals(40287, streamAvail.streamId.userId)
        assertEquals(4, streamAvail.streamId.subId)
    }

    @Test
    fun `sending OpenResponse`() {
        val raw = byteArrayOf(
            2, 6, //type,
            0, 14, //length
            5, 65, 0, 5, 15, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            0, //status
            2 //access
        )

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val message = pillarHandler.onReceive(buffer)
        val metadata = pillarHandler.onIncoming(message!!)
        val openResponseMsg = OpenResponse(message)

        assertEquals(518.toString(), metadata[TYPE_FIELD_NAME])
        assertEquals(14.toString(), metadata[LENGTH_FIELD_NAME])

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
        val raw = byteArrayOf(
            2, 8, //type,
            0, 13, //length
            5, 65, 0, 5, 15, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            0
        )

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val message = pillarHandler.onReceive(buffer)
        val metadata = pillarHandler.onIncoming(message!!)
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
        val raw = byteArrayOf(
            9, 5, //type
            0, 32, //length
            5, 65, 0, 5, 13, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            23, 0, 0, 0, 0, 0, 0, 0, //seq
            0, 0, 0, 0, //reserved1
            64, -59, -122, 125, 62, -96, -60, 22 //timestamp
        )

        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val message = pillarHandler.onReceive(buffer)
        val seqMsg = SeqMsg(message!!)

        assertEquals(2309, seqMsg.header.type)
        assertEquals(32, seqMsg.header.length)
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
        val raw = byteArrayOf(
            2, 1,   //type:513
            0, 76,  //length:76
            117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
            112, 97, 115, 115, 119, 111, 114, 100, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //password
            109, 105, 99, 0, //mic
            49, 46, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0 // version
        )

        assertEquals(76, login.writerIndex())

        for (i in raw.indices)
            assertEquals(raw[i], login.array()[i])
    }

    @Test
    fun `assembling Open`() {
        val stream = byteArrayOf(
            5, 65, 0, 5, 15, -99, 95, 4
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(stream)
        val open = Open(StreamId(buffer), BigDecimal.valueOf(23)).open()

        val raw = byteArrayOf(
            2, 5,   //type:517,
            0, 30,  //length:30
            5, 65, 0, 5, 15, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            23, 0, 0, 0, 0, 0, 0, 0, //start_seq
            24, 0, 0, 0, 0, 0, 0, 0, //end_seq
            2, //access
            0, //mode
        )

        assertEquals(30, open.writerIndex())

        for (i in raw.indices)
            assertEquals(raw[i], open.array()[i])
    }

    @Test
    fun `assembling SeqMsg`() {
        val rawSend = byteArrayOf(
            9, 5, //type
            0, 32, //length
            5, 65, 0, 5, 13, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            23, 0, 0, 0, 0, 0, 0, 0, //seq
            0, 0, 0, 0, //reserved1
            64, -59, -122, 125, 62, -96, -60, 22 //timestamp
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(rawSend)
        val seqMsg = SeqMsg(buffer)

        val seqMsgToSend = SeqMsgToSend(seqMsg).seqMsg()

        val raw = byteArrayOf(
            9, 5, //type
            0, 32, //length
            5, 65, 0, 5, 15, -99, 95, 4, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
            23, 0, 0, 0, 0, 0, 0, 0, //seq
            0, 0, 0, 0 //reserved1
        )

        assertEquals(32, seqMsgToSend.writerIndex())

        for (i in raw.indices)
            assertEquals(raw[i], seqMsgToSend.array()[i])
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

        val raw = byteArrayOf(
            2, 7,
            0, 12,
            5, 65, 0, 5, 15, -99, 95, 4,
        )

        assertEquals(12, close.writerIndex())

        for (i in raw.indices)
            assertEquals(raw[i], close.array()[i])
    }

    @Test
    fun `invalid size message`() {
        val raw = byteArrayOf(
            9, 5, //type
            0, 32, //length
        )
        val buffer: ByteBuf = Unpooled.buffer(4)
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        assertNull(pillarHandler.onReceive(buffer))
    }

    @Test
    fun `empty message`() {
        val buffer: ByteBuf = Unpooled.buffer()
        val pillarHandler = PillarHandler(channel, settings)
        assertNull(pillarHandler.onReceive(buffer))
    }

    @Test
    fun `message size is less than 4`() {
        val raw = byteArrayOf(
            9, 5, 0
        )
        val buffer: ByteBuf = Unpooled.buffer(3)
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        assertNull(pillarHandler.onReceive(buffer))
    }

    @Test
    fun `send onOutgoing`() {
        val raw = byteArrayOf(
            2, 2, 0, 21, 5, 65, 0, 5, 15, -99, 95, 4
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        val metadata = mutableMapOf<String, String>()
        metadata[TYPE_FIELD_NAME] = 517.toString()
        metadata[LENGTH_FIELD_NAME] = 21.toString()

        val onOutgoing = pillarHandler.onOutgoing(buffer, metadata)

        assertEquals(517.toString(), onOutgoing[TYPE_FIELD_NAME])
        assertEquals(21.toString(), onOutgoing[LENGTH_FIELD_NAME])
    }


    @Test
    fun `invalid type message`() {
        val raw = byteArrayOf(
            9, 9, //type
            0, 32, //length
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
        assertNull(pillarHandler.onReceive(buffer))
    }

    @Test
    fun `invalid heartbeat interval`() {
        settings.heartbeatInterval = 0
        val exception = assertThrows<IllegalArgumentException> { PillarHandler(channel, settings) }
        assertEquals("Heartbeat sending interval must be greater than zero.", exception.message)
    }

    @Test
    fun `invalid status in LoginResponse`() {
        val raw = byteArrayOf(
            2, 2, //type,
            0, 21, //length
            117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
            85, //status
        )
        val buffer: ByteBuf = Unpooled.buffer()
        buffer.writeBytes(raw)
        val pillarHandler = PillarHandler(channel, settings)
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