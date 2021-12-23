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
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.mock
import com.nhaarman.mockitokotlin2.mock
import java.net.InetSocketAddress
import java.util.concurrent.Executor


class TestHandler {
    private val address: InetSocketAddress = mock (InetSocketAddress::class.java)
    val secure = true
    private val sessionAlias: String = "sessionAlias"
    private val reconnectDelay: Long = 1000L
    private val handler: IProtocolHandler = mock (IProtocolHandler::class.java)
    private val mangler: IProtocolMangler = mock (IProtocolMangler::class.java)
    private val onEvent: (event: Event, parentEventId: EventID) -> Unit = mock{}
    private val onMessage: (RawMessage) -> Unit = mock {  }
    val eventLoopGroup: EventLoopGroup = NioEventLoopGroup()
    private val executor: Executor = mock (Executor::class.java)
    private val onError: (stream: String, error: Throwable) -> Unit = mock{}
    private val actionExecutor = ActionStreamExecutor(executor, onError)
    val parentEventId = EventID.newBuilder().setId("root").build()!!

    private var channel = Channel(address, secure, sessionAlias, reconnectDelay, handler, mangler, onEvent, onMessage, eventLoopGroup, actionExecutor, parentEventId)

    @Nested
    inner class Positive {
        @Test
        fun `sending LoginResponse with extra bytes`() {
            val raw = byteArrayOf(
                2, 2, //type,
                0, 21, //length
                117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
                0, //status
                117, 115, 101
            )

            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val returnMetadata = pillarHandler.onIncoming(data!!)

            assertEquals(514.toString(), returnMetadata[TYPE_FIELD_NAME])
            assertEquals(21.toString(), returnMetadata[LENGTH_FIELD_NAME])
        }

        @Test
        fun `sending StreamAvail`() {
            val raw = byteArrayOf(
                2, 3, //type,
                0, 21, //length
                5, 65, 0, 5, 4, -99, 95, 15, //stream_id: env_id(1), sess_num(3), stream_type, user_id(2), sub_id(1)
                23, 0, 0, 0, 0, 0, 0, 0, //next_seq
                6 //access
            )

            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val returnMetadata = StreamAvail(data!!).streamAvail()

            assertEquals(515.toString(), returnMetadata[TYPE_FIELD_NAME])
            assertEquals(21.toString(), returnMetadata[LENGTH_FIELD_NAME])
        }

        @Test
        fun `sending OpenResponse`() {
            val raw = byteArrayOf(
                2, 6, //type,
                0, 14, //length
                5, 65, 0, 5, 4, -99, 95, 15, //stream_id: env_id(1), ses_num(3), stream_type(1) user_id(2), sub_id(1),
                0, //status
                2 //access
            )

            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val  returnMetadata = pillarHandler.onIncoming(data!!)

            assertEquals(518.toString(), returnMetadata[TYPE_FIELD_NAME])
            assertEquals(14.toString(), returnMetadata[LENGTH_FIELD_NAME])
        }

        @Test
        fun `sending CloseResponse`() {
            val raw = byteArrayOf(
                2, 8, //type,
                0, 13, //length
                5, 65, 0, 5, 4, -99, 95, 15, //stream_id: env_id(1), ses_num(4),  user_id(2), stream_type(1)
                0
            )

            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val returnMetadata = pillarHandler.onIncoming(data!!)

            assertEquals(520.toString(), returnMetadata[TYPE_FIELD_NAME])
            assertEquals(13.toString(), returnMetadata[LENGTH_FIELD_NAME])
        }

        @Test
        fun `sending SeqMsg`() {
            val raw = byteArrayOf(
                9, 5, //type
                0, 32, //length
                5, 65, 0, 5, 4, -99, 95, 15, //stream_id: env_id(1), ses_num(4),  user_id(2), stream_type(1)
                23, 0, 0, 0, 0, 0, 0, 0, //seq
                0, 0, 0, 0, //reserved1
                1, 88, -67, -81, 0, 0, 0, 0, //timestamp
            )

            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val returnMetadata = pillarHandler.onIncoming(data!!)

            assertEquals(2309.toString(), returnMetadata[TYPE_FIELD_NAME])
            assertEquals(32.toString(), returnMetadata[LENGTH_FIELD_NAME])
        }

        @Test
        fun `assembling Login`() {
            val settings = PillarHandlerSettings()
            val login = Login(settings).login().array()
            val raw = byteArrayOf(
                2, 1,   //type:513,
                0, 76,  //length:76
                117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
                112, 97, 115, 115, 119, 111, 114, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //password
                109, 105, 99, 0, //mic
                49, 46, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,// version
            )

            for (i in login.indices)
                assertEquals(raw[i], login[i])
        }

        @Test
        fun `assembling Open`() {
            val stream = byteArrayOf(
                0, 0, 0, 0, 5, 65, 0, 5, 4, -99, 95, 15
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(stream)
            val streamId = StreamIdEncode(StreamId(buffer))
            val open = Open(streamId.streamId(), 23, 15).open().array()

            val raw = byteArrayOf(
                2, 5,   //type:517,
                0, 30,  //length:30
                5, 65, 0, 5, 4, -99, 95, 15, //stream_id: ses_num(3), env_id(1), user_id(2), sub_id(1), stream_type(1)
                23, 0, 0, 0, 0, 0, 0, 0, //start_seq
                24, 0, 0, 0, 0, 0, 0, 0, //end_seq
                2, //access
                0, //mode
            )

            for (i in open.indices)
                assertEquals(raw[i], open[i])
        }

        @Test
        fun `assembling Close`() {

            val stream = byteArrayOf(
                0, 0, 0, 0, 5, 65, 0, 5, 4, -99, 95, 15
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(stream)
            val streamId = StreamIdEncode(StreamId(buffer))
            val close = Close(streamId.streamId()).close().array()

            val raw = byteArrayOf(
                2, 7,
                0, 12,
                5, 65, 0, 5, 4, -99, 95, 15
            )

            for (i in close.indices)
                assertEquals(raw[i], close[i])
        }

        @Test
        fun `invalid size message`() {
            val raw = byteArrayOf(
                9, 5, //type
                0, 32, //length
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            assertEquals(null, pillarHandler.onReceive(buffer))
        }

        @Test
        fun `empty message`() {
            val buffer: ByteBuf = Unpooled.buffer()
            val pillarHandler = PillarHandler(channel)
            assertEquals(null, pillarHandler.onReceive(buffer))
        }

        @Test
        fun `send onOutgoing`() {
            val raw = byteArrayOf(
                5, 65, 0, 5, 4, -99, 95, 15
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val metadata = mutableMapOf<String, String>()
            metadata[TYPE_FIELD_NAME] = 12.toString()
            metadata[LENGTH_FIELD_NAME] = 12.toString()

            assertEquals(12.toString(), pillarHandler.onOutgoing(buffer, metadata)[TYPE_FIELD_NAME])
        }
    }

    @Nested
    inner class Negative {

        @Test
        fun `invalid type message`() {
            val raw = byteArrayOf(
                9, 9, //type
                0, 32, //length
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val exception = assertThrows<Exception> { pillarHandler.onReceive(buffer) }
            assertEquals("Message type is not supported: 2313", exception.message)
        }

        @Test
        fun `invalid heartbeat interval`() {
            val pillarHandler = PillarHandler(channel)
            pillarHandler.settings.heartbeatInterval = 0L
            val exception = assertThrows<Exception> { pillarHandler.initCommand() }
            assertEquals("Heartbeat sending interval must be greater than zero", exception.message)
        }

        @Test
        fun `invalid LoginResponse`() {
            val raw = byteArrayOf(
                2, 2, //type,
                0, 21, //length
                117, 115, 101, 114, 110, 97, 109, 101, 0, 0, 0, 0, 0, 0, 0, 0, //username
                85, //status
            )
            val buffer: ByteBuf = Unpooled.buffer()
            buffer.writeBytes(raw)
            val pillarHandler = PillarHandler(channel)
            val data = pillarHandler.onReceive(buffer)
            val exception = assertThrows<Exception> { pillarHandler.onIncoming(data!!) }
            assertEquals("Received STREAM_NOT_OPEN status.", exception.message)
        }
    }
}