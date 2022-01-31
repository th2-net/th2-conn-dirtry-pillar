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

import com.exactpro.th2.conn.dirty.pillar.handler.util.*
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IContext
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandlerSettings
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
class PillarHandler(private val context: IContext<IProtocolHandlerSettings>): IProtocolHandler {

    private var state = AtomicReference(State.SESSION_CLOSE)
    private val executor = Executors.newScheduledThreadPool(1)
    private var clientFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private var serverFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private lateinit var streamId: StreamIdEncode
    private var settings: PillarHandlerSettings
    lateinit var channel: IChannel
    private var startRead = AtomicReference(0)
    private var endRead = AtomicReference(0)
    private var startWrite = AtomicReference(0)
    private var endWrite = AtomicReference(0)
    private var readingConnection = AtomicReference(false)
    private var writingConnection = AtomicReference(false)

    init{
        settings = context.settings as PillarHandlerSettings
        require(settings.heartbeatInterval > 0) { "Heartbeat sending interval must be greater than zero." }
        require(settings.streamAvailInterval > 0) { "StreamAvail sending interval must be greater than zero." }
    }

    override fun onOpen() {
        if (state.compareAndSet(State.SESSION_CLOSE, State.SESSION_CREATED)) {
            LOGGER.info { "Setting a new state -> ${state.get()}." }

            channel = context.channel
            channel.send(Login(settings).login(), messageMetadata(MessageType.LOGIN), IChannel.SendMode.MANGLE)
            LOGGER.info { "Message has been sent to server - Login." }
            serverFuture?.cancel(false)
            serverFuture =
                executor.schedule(this::reconnect, settings.streamAvailInterval, TimeUnit.MILLISECONDS)

            clientFuture?.cancel(false)
            clientFuture = executor.scheduleWithFixedDelay(this::startSendHeartBeats, settings.heartbeatInterval, settings.heartbeatInterval, TimeUnit.MILLISECONDS)
            LOGGER.info { "Message has been sent to server - HeartBeats." }

            LOGGER.info { "Handler is connected." }
        } else LOGGER.info { "Failed to set a new state. ${State.SESSION_CLOSE} -> ${state.get()}." }
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        val bufferLength = buffer.readableBytes()
        if (bufferLength == 0) {
            LOGGER.warn { "Cannot parse empty buffer." }
            return null
        }

        if (bufferLength < 4){
            LOGGER.warn { "Not enough bytes to read header." }
            return null
        }

        buffer.markReaderIndex()
        val messageType = buffer.readUnsignedShortLE()
        val messageLength = buffer.readUnsignedShortLE()
        buffer.resetReaderIndex()

        if (!MessageType.contains(messageType)){
            LOGGER.error { "Message type is not supported. Type: $messageType, length: $messageLength" }
            return null
        }

        if(bufferLength < messageLength) {
            LOGGER.warn { "Buffer length is less than the declared one: $bufferLength -> $messageLength." }
            return null
        }

        if(bufferLength > messageLength) {
            LOGGER.info { "Buffer length is longer than the declared one: $bufferLength -> $messageLength." }
            return buffer.readSlice(messageLength)
        }

        buffer.readerIndex(buffer.writerIndex())
        return buffer.retainedSlice(buffer.readerIndex() - messageLength, messageLength)
    }

    override fun onIncoming(message: ByteBuf): Map<String, String> {
        val msgHeader = MsgHeader(message)

        when (val msgType = msgHeader.type) {
            MessageType.LOGIN_RESPONSE.type -> {
                return checkLoginResponse(message)
            }

            MessageType.STREAM_AVAIL.type -> {
                return checkStreamAvail(message)
            }

            MessageType.OPEN_RESPONSE.type -> {
                return checkOpenResponse(message)
            }

            MessageType.SEQMSG.type -> {
                return checkSeqMsg(message)
            }

            MessageType.CLOSE_RESPONSE.type -> {
                return checkCloseResponse(message)
            }

            else -> error("Message type is not supported: $msgType.")
        }
    }

    private fun checkLoginResponse(message: ByteBuf): Map<String, String> {
        val loginResponse = LoginResponse(message)

        LOGGER.info { "Type message - LoginResponse: $loginResponse" }
        when (val status = Status.getStatus(loginResponse.status)) {
            Status.OK -> {
                LOGGER.info { "Login successful. Server start sending heartbeats." }

                if (state.compareAndSet(
                        State.SESSION_CREATED,
                        State.LOGGED_IN
                    )
                ) {
                    LOGGER.info("Setting a new state -> ${state.get()}.")
                    serverFuture?.cancel(false)
                    serverFuture =
                        executor.schedule(this::receivedHeartBeats, settings.streamAvailInterval, TimeUnit.MILLISECONDS)
                } else LOGGER.info { "Failed to set a new state. ${State.LOGGED_IN} -> ${state.get()}." }
            }
            Status.NOT_LOGGED_IN -> {
                if (!state.compareAndSet(State.SESSION_CREATED, State.SESSION_CLOSE))
                    LOGGER.info { "Failed to set a new state. ${State.SESSION_CLOSE} -> ${state.get()}." }
                stopSendHeartBeats()
                LOGGER.info("Received `not logged in` status. Fall in to error state.")
                sendClose()
            }
            else -> error("Received $status status.")
        }
        return messageMetadata(MessageType.LOGIN_RESPONSE)
    }

    private fun checkStreamAvail(message: ByteBuf): Map<String, String> {
        executor.awaitTermination(0,TimeUnit.MILLISECONDS)
        serverFuture?.cancel(false)
        serverFuture =
            executor.schedule(this::receivedHeartBeats, settings.streamAvailInterval, TimeUnit.MILLISECONDS)

        val streamAvail = StreamAvail(message)

        LOGGER.info { "Type message - StreamAvail: $streamAvail" }
        message.readerIndex(4)
        streamId = StreamIdEncode(StreamId(message))

        var open: ByteBuf = Unpooled.buffer()

        if (streamAvail.streamId.streamType == StreamType.REF.value || streamAvail.streamId.streamType == StreamType.GT.value){
            val streamType = Access.READ.value.toInt()
            startRead.getAndSet(streamAvail.nextSeq.toInt())
            endRead.getAndSet(streamAvail.nextSeq.toInt()+1)
            open = Open(
                streamId.streamId,
                startRead.get(),
                endRead.get()
            ).open()
            readingConnection.getAndSet(true)
        }
        else if (streamAvail.streamId.streamType == StreamType.TG.value) {
            val streamType = Access.WRITE.value.toInt()
            startWrite.getAndSet(streamAvail.nextSeq.toInt())
            endWrite.getAndSet(streamAvail.nextSeq.toInt()+1) //TODO
            open = Open(
                streamId.streamId,
                startWrite.get(),
                endWrite.get()
            ).open()
            writingConnection.getAndSet(true)
        }

        channel.send(
            open,
            messageMetadata(MessageType.OPEN), IChannel.SendMode.MANGLE
        )

        LOGGER.info { "Message has been sent to server - Open."}
        return messageMetadata(MessageType.STREAM_AVAIL)
    }

    private fun checkOpenResponse(message: ByteBuf): Map<String, String> {

        val openResponse = OpenResponse(message)
        LOGGER.info { "Type message - OpenResponse: $openResponse" }

        when (Status.getStatus(openResponse.status)) {
            Status.OK -> {
                LOGGER.info("Open successful.")
                if (readingConnection.get()) {
                    LOGGER.info { "Open for READ" }
                    readingConnection.getAndSet(false)
                }

                if (writingConnection.get()) {
                    LOGGER.info { "Open for WRITE" }
                    var i = startWrite.get()
                    while (i < endWrite.get()) {
                        LOGGER.info { "Message has been sent to server - Heartbeat" }
                        channel.send(
                            //SeqMsgToSend(i, openResponse.streamId).seqMsg(),
                            Heartbeat().heartbeat,
                            messageMetadata(MessageType.SEQMSG),
                            IChannel.SendMode.MANGLE
                        )
                        i++
                    }
                    writingConnection.getAndSet(false)
                }
            }

            Status.NO_STREAM_PERMISSION -> LOGGER.warn { "No stream permission." }
            else -> error("This is not an OpenResponse status.")
        }

        return messageMetadata(MessageType.OPEN_RESPONSE)
    }

    private fun checkSeqMsg(message: ByteBuf): Map<String, String> {
        val seqMsg = SeqMsg(message)

        LOGGER.info { "Type message - SeqMsg: $seqMsg" }

        startRead.getAndSet(1)
        if (startRead == endRead){
            channel.send(
                Close(streamId.streamIdBuf).close(),
                messageMetadata(MessageType.CLOSE),
                IChannel.SendMode.MANGLE
            )
            LOGGER.info { "Message has been sent to server - Close." }
        }

        return messageMetadata(MessageType.SEQMSG)
    }

    private fun checkCloseResponse(message: ByteBuf): Map<String, String> {
        val closeResponse = CloseResponse(message)

        LOGGER.info { "Type message - CloseResponse: $closeResponse" }

        when (Status.getStatus(closeResponse.status)) {
            Status.OK -> LOGGER.info("Open successful.")
            Status.STREAM_NOT_OPEN -> LOGGER.warn { "Stream not open." }
            else -> error("This is not an CloseResponse status.")
        }

        return messageMetadata(MessageType.CLOSE_RESPONSE)
    }

    override fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Map<String, String> {
        val offset = message.readerIndex()
        val msgHeader = MsgHeader(message)
        val type = metadata[TYPE_FIELD_NAME]!!.toInt()
        if (msgHeader.type!= type){
            val buffer: ByteBuf = message.copy(4, message.readableBytes()-4)
            message.readerIndex(offset)
            message.writerIndex(offset)
            message.writeShortLE(type)
            message.writeShortLE(metadata[LENGTH_FIELD_NAME]!!.toInt())
            message.writeBytes(buffer)
        }
        return metadata
    }

    override fun onClose() {
        sendClose()
        if (state.compareAndSet(State.LOGGED_OUT, State.SESSION_CLOSE)) {
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            LOGGER.info { "Handler is disconnected." }
        } else LOGGER.info { "Failed to set a new state ${State.SESSION_CLOSE}." }
    }

    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(3000, TimeUnit.MILLISECONDS)) executor.shutdownNow()
    }

    private fun sendClose() {
        if (state.compareAndSet(State.LOGGED_IN, State.LOGGED_OUT)) {
            state.getAndSet(State.LOGGED_OUT)
            LOGGER.info { "Setting a new state -> ${state.get()}." }

        } else LOGGER.info { "Failed to set a new state ${State.LOGGED_OUT}." }
    }

    private fun startSendHeartBeats() {
        channel.send(Heartbeat().heartbeat, messageMetadata(MessageType.HEARTBEAT), IChannel.SendMode.MANGLE)
    }

    private fun stopSendHeartBeats() {
        clientFuture?.cancel(false)
    }

    private fun reconnect() {
        LOGGER.info { "Reconnect to server." }
        if (state.compareAndSet(State.NOT_HEARTBEAT, State.SESSION_CREATED)) {
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            channel.send(Login(settings).login(), messageMetadata(MessageType.LOGIN), IChannel.SendMode.MANGLE)
            LOGGER.info { "Message has been sent to server - Login." }
        } else LOGGER.info { "Failed to set a new state ${State.SESSION_CREATED}." }
    }

    private fun receivedHeartBeats() {
        serverFuture?.cancel(false)
        if (state.compareAndSet(state.get(), State.NOT_HEARTBEAT)) {
            LOGGER.warn { "Server stopped sending heartbeat." }
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            reconnect()
        } else LOGGER.info { "Failed to set a new state ${State.NOT_HEARTBEAT}." }
    }

    private fun messageMetadata(messageType: MessageType): Map<String, String>{
        val metadata = mutableMapOf<String, String>()
        metadata[TYPE_FIELD_NAME] = messageType.type.toString()
        metadata[LENGTH_FIELD_NAME] = messageType.length.toString()
        return metadata
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}
