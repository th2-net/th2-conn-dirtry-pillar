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
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class PillarHandler(private val context: IContext<IProtocolHandlerSettings>): IProtocolHandler {

    private var state = AtomicReference(State.SESSION_CLOSE)
    private val executor = Executors.newScheduledThreadPool(1)
    private var clientFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private var serverFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private lateinit var streamIdRead: StreamIdEncode
    private lateinit var streamIdWrite: StreamIdEncode
    private var settings: PillarHandlerSettings
    lateinit var channel: IChannel
    private var connectRead = AtomicReference(OpenType.CLOSE)
    private var connectWrite = AtomicReference(OpenType.CLOSE)
    private var serverSeqNum = AtomicInteger(0)
    private var clientSeqNum = AtomicInteger(0)
    private val seqNum = 1000

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

            LOGGER.info { "Handler is connected." }
        } else LOGGER.info { "Failed to set a new state. ${State.SESSION_CLOSE} -> ${state.get()}." }
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {
        val bufferLength = buffer.readableBytes()
        val offset = buffer.readerIndex()
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
            buffer.readerIndex(buffer.writerIndex())
            return buffer.retainedSlice(offset, messageLength)
        }

        buffer.readerIndex(buffer.writerIndex())
        return buffer.retainedSlice(offset, messageLength)
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
                return checkSeqMsg(message, msgHeader)
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
                channel.send(Login(settings).login(), messageMetadata(MessageType.LOGIN), IChannel.SendMode.MANGLE)
            }
            else -> error("Received $status status.")
        }
        return messageMetadata(MessageType.LOGIN_RESPONSE)
    }

    private fun checkStreamAvail(message: ByteBuf): Map<String, String> {
        serverFuture?.cancel(false)
        serverFuture =
            executor.schedule(this::receivedHeartBeats, settings.streamAvailInterval, TimeUnit.MILLISECONDS)

        val streamAvail = StreamAvail(message)

        LOGGER.info { "Type message - StreamAvail: $streamAvail" }
        message.readerIndex(4)
        val streamIdAvail = StreamIdEncode(StreamId(message))
        val nextSeq = streamAvail.nextSeq.toInt()

        if (streamAvail.streamId.streamType == StreamType.REF.value || streamAvail.streamId.streamType == StreamType.GT.value) {
            if (connectRead.compareAndSet(OpenType.CLOSE, OpenType.SENT) || connectRead.get() == OpenType.SENT) {
                streamIdRead = streamIdAvail
                serverSeqNum.getAndSet(nextSeq)
                channel.send(
                    Open(
                        streamIdRead.streamId,
                        nextSeq,
                        nextSeq + seqNum
                    ).open(),
                    messageMetadata(MessageType.OPEN), IChannel.SendMode.MANGLE
                )
            }
        } else if (streamAvail.streamId.streamType == StreamType.TG.value) {
            if (connectWrite.compareAndSet(OpenType.CLOSE, OpenType.SENT) || connectWrite.get() == OpenType.SENT) {
                streamIdWrite = streamIdAvail
                clientSeqNum.getAndSet(nextSeq)
                channel.send(
                    Open(
                        streamIdWrite.streamId,
                        nextSeq,
                        nextSeq + seqNum
                    ).open(),
                    messageMetadata(MessageType.OPEN), IChannel.SendMode.MANGLE
                )
            }
        }

        return messageMetadata(MessageType.STREAM_AVAIL)
    }

    private fun checkOpenResponse(message: ByteBuf): Map<String, String> {

        val openResponse = OpenResponse(message)
        LOGGER.info { "Type message - OpenResponse: $openResponse" }

        when (Status.getStatus(openResponse.status)) {
            Status.OK -> {
                LOGGER.info("Open successful.")

                connectRead.compareAndSet(OpenType.SENT, OpenType.OPEN)
                connectWrite.compareAndSet(OpenType.SENT, OpenType.OPEN)

                if (connectRead.get() == OpenType.OPEN && connectWrite.get() == OpenType.OPEN){
                    if (state.compareAndSet(State.LOGGED_IN, State.OPEN_IN))
                        LOGGER.info { "Setting state -> ${state.get()}" }
                }
            }

            Status.NO_STREAM_PERMISSION -> LOGGER.warn { "No stream permission." }
            else -> error("This is not an OpenResponse status.")
        }

        return messageMetadata(MessageType.OPEN_RESPONSE)
    }

    private fun checkSeqMsg(message: ByteBuf, header: MsgHeader): Map<String, String> {
        val seqMsg = SeqMsg(message, header.length)
        LOGGER.info { "Type message - SeqMsg: $seqMsg" }
        serverSeqNum.incrementAndGet()
        if(serverSeqNum.get() == seqNum){
            connectRead.compareAndSet(OpenType.OPEN, OpenType.CLOSE)
        }
        return messageMetadata(MessageType.SEQMSG)
    }

    private fun checkCloseResponse(message: ByteBuf): Map<String, String> {
        val closeResponse = CloseResponse(message)

        LOGGER.info { "Type message - CloseResponse: $closeResponse" }

        when (Status.getStatus(closeResponse.status)) {
            Status.OK -> {
                LOGGER.info("Close successful.")
                if (closeResponse.streamId == streamIdRead.streamId)
                    connectRead.getAndSet(OpenType.CLOSE)
                if (closeResponse.streamId == streamIdWrite.streamId)
                    connectWrite.getAndSet(OpenType.CLOSE)

                if (connectRead.get()==OpenType.CLOSE && connectWrite.get()==OpenType.CLOSE) {
                    if (state.compareAndSet(State.OPEN_OUT, State.LOGGED_OUT)) {
                        LOGGER.info { "Setting a new state -> ${state.get()}." }
                        executor.shutdown()
                        try {
                            if (!executor.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                                executor.shutdownNow()
                            }
                        } catch (e: InterruptedException) {
                            executor.shutdownNow()
                        }
                    } else LOGGER.info { "Failed to set a new state ${State.LOGGED_OUT}." }
                }
            }
            Status.STREAM_NOT_OPEN -> LOGGER.warn { "Stream not close." }
            else -> error("This is not an CloseResponse status.")
        }

        return messageMetadata(MessageType.CLOSE_RESPONSE)
    }

    override fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Map<String, String> {
        clientSeqNum.incrementAndGet()
        SeqMsgToSend(message, clientSeqNum.get(), streamIdWrite.streamId, metadata).seqMsg()
        if(clientSeqNum.get() == seqNum){
            connectWrite.compareAndSet(OpenType.OPEN, OpenType.CLOSE)
        }
        return metadata
    }

    override fun onClose() {
        if (state.compareAndSet(State.OPEN_IN, State.SESSION_CLOSE)) {
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            LOGGER.info { "Handler is disconnected." }
            connectRead.getAndSet(OpenType.CLOSE)
            connectWrite.getAndSet(OpenType.CLOSE)
        } else LOGGER.info { "Failed to set a new state ${State.SESSION_CLOSE}." }
    }

    override fun close() {
        if (state.compareAndSet(State.OPEN_IN, State.OPEN_OUT)){
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            if (connectRead.get() == OpenType.OPEN) {
                channel.send(
                    Close(streamIdRead.streamIdBuf).close(),
                    messageMetadata(MessageType.CLOSE),
                    IChannel.SendMode.MANGLE
                )
            }
            if (connectWrite.get() == OpenType.OPEN) {
                channel.send(
                    Close(streamIdWrite.streamIdBuf).close(),
                    messageMetadata(MessageType.CLOSE),
                    IChannel.SendMode.MANGLE
                )
            }
            LOGGER.info { "Message has been sent to server - Close." }
        } else LOGGER.info { "Failed to set a new state ${State.OPEN_OUT}." }
    }

    private fun startSendHeartBeats() {
        channel.send(Heartbeat().heartbeat, messageMetadata(MessageType.HEARTBEAT), IChannel.SendMode.MANGLE)
        LOGGER.info { "Message has been sent to server - HeartBeats." }
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
            connectRead.getAndSet(OpenType.CLOSE)
            connectWrite.getAndSet(OpenType.CLOSE)
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
