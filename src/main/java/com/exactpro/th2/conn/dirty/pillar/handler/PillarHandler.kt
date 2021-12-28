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
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class PillarHandler(private val channel: IChannel,
                    private val settings: PillarHandlerSettings
): IProtocolHandler {

    private var state = AtomicReference(State.SESSION_CLOSE)
    private val executor = Executors.newSingleThreadScheduledExecutor()
    private var clientFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private var serverFuture: Future<*>? = CompletableFuture.completedFuture(null)
    private lateinit var streamId: StreamIdEncode

    init{
        require(settings.heartbeatInterval > 0) { "Heartbeat sending interval must be greater than zero." }
        require(settings.streamAvailInterval > 0) { "StreamAvail sending interval must be greater than zero." }
    }

    override fun onOpen() {
        if (state.compareAndSet(State.SESSION_CLOSE, State.SESSION_CREATED)) {
            LOGGER.info { "Setting a new state -> ${state.get()}." }

            channel.send(Login(settings).login(), messageMetadata(MessageType.LOGIN), IChannel.SendMode.MANGLE)

            startSendHeartBeats()
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
        val messageType = buffer.readShort().toInt()
        val messageLength = buffer.readShort().toInt()
        buffer.resetReaderIndex()

        if (!MessageType.contains(messageType)){
            buffer.resetReaderIndex()
            LOGGER.error { "Message type is not supported: $messageType." }
            return null
        }

        if(bufferLength < messageLength) {
            LOGGER.warn { "Buffer length is less than the declared one: $bufferLength -> $messageLength." }
            return null
        }

        if(bufferLength > messageLength) {
            LOGGER.info { "Buffer length is longer than the declared one: $bufferLength -> $messageLength." }
            return buffer.copy(0, messageLength)
        }

        return buffer
    }

    override fun onIncoming(message: ByteBuf): Map<String, String> {
        val msgHeader = MsgHeader(message)

        when (val msgType = msgHeader.type) {
            MessageType.LOGIN_RESPONSE.type -> {
                LOGGER.info { "Type message - LoginResponse." }
                val loginResponse = LoginResponse(message)

                when (val status = Status.getStatus(loginResponse.status)) {
                    Status.OK -> {
                        LOGGER.info("Login successful. Start sending heartbeats.")
                        if (state.compareAndSet(
                                State.SESSION_CREATED,
                                State.LOGGED_IN
                            )
                        ) LOGGER.info { "Setting a new state -> ${state.get()}." }
                        else LOGGER.info { "Failed to set a new state. ${State.LOGGED_IN} -> ${state.get()}." }
                    }
                    Status.NOT_LOGGED_IN -> {
                        if (state.compareAndSet(State.SESSION_CREATED, State.SESSION_CLOSE)) stopSendHeartBeats()
                        else LOGGER.info { "Failed to set a new state. ${State.SESSION_CLOSE} -> ${state.get()}." }

                        LOGGER.info("Received `not logged in` status. Fall in to error state.")
                        sendClose()
                    }
                    else -> error("Received $status status.")
                }
                return messageMetadata(MessageType.LOGIN_RESPONSE)
            }

            MessageType.STREAM_AVAIL.type -> {
                LOGGER.info { "Type message - StreamAvail." }
                val streamAvail = StreamAvail(message)
                streamId = StreamIdEncode(StreamId(message))
                val open = Open(
                    streamId.streamId,
                    streamAvail.nextSeq
                )

                channel.send(
                    open.open(),
                    messageMetadata(MessageType.OPEN), IChannel.SendMode.MANGLE
                )

                serverFuture =
                    executor.schedule(this::startSendHeartBeats, settings.streamAvailInterval, TimeUnit.MILLISECONDS)

                return messageMetadata(MessageType.STREAM_AVAIL)
            }

            MessageType.OPEN_RESPONSE.type -> {
                LOGGER.info { "Type message - OpenResponse." }
                val openResponse = OpenResponse(message)

                when (Status.getStatus(openResponse.status)) {
                    Status.OK -> LOGGER.info("Open successful.")
                    Status.NO_STREAM_PERMISSION -> LOGGER.warn { "No stream permission." }
                    else -> error("This is not an OpenResponse status.")
                }
                return messageMetadata(MessageType.OPEN_RESPONSE)
            }

            MessageType.SEQMSG.type -> {
                LOGGER.info { "Type message - SeqMsg." }
                channel.send(
                    SeqMsgToSend(SeqMsg(message)).seqMsg(),
                    messageMetadata(MessageType.SEQMSG),
                    IChannel.SendMode.MANGLE
                )
                return messageMetadata(MessageType.SEQMSG)
            }

            MessageType.CLOSE_RESPONSE.type -> {
                LOGGER.info { "Type message - CloseResponse." }
                val closeResponse = CloseResponse(message)
                when (Status.getStatus(closeResponse.status)) {
                    Status.OK -> LOGGER.info("Open successful.")
                    Status.STREAM_NOT_OPEN -> LOGGER.warn { "Stream not open." }
                    else -> error("This is not an CloseResponse status.")
                }
                return messageMetadata(MessageType.CLOSE_RESPONSE)
            }

            else -> error("Message type is not supported: $msgType.")
        }
    }

    override fun onOutgoing(message: ByteBuf, metadata: Map<String, String>): Map<String, String> {
        val buffer: ByteBuf = Unpooled.buffer(message.readableBytes() + 4)
        buffer.writeShort(metadata[TYPE_FIELD_NAME]!!.toInt())
        buffer.writeShort(message.readableBytes() + 4)
        buffer.writeBytes(message)
        return metadata
    }

    override fun onClose() {
        if (state.compareAndSet(state.get(), State.SESSION_CLOSE)) {
            state.getAndSet(State.SESSION_CLOSE)
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            LOGGER.info { "Handler is disconnected." }
        } else LOGGER.info { "Failed to set a new state." }
    }

    override fun close() {
        executor.shutdown()
        if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) executor.shutdownNow()
    }

    private fun sendClose() {
        if (state.compareAndSet(state.get(), State.LOGGED_OUT)) {
            state.getAndSet(State.LOGGED_OUT)
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            channel.send(
                Close(streamId.streamIdBuf).close(),
                messageMetadata(MessageType.CLOSE),
                IChannel.SendMode.MANGLE
            )
        } else LOGGER.info { "Failed to set a new state." }
    }

    private fun startSendHeartBeats() {
        channel.send(Heartbeat().heartbeat, messageMetadata(MessageType.HEARTBEAT), IChannel.SendMode.MANGLE)
        clientFuture = executor.schedule(this::receivedHeartBeats, settings.heartbeatInterval, TimeUnit.MILLISECONDS)
    }

    private fun stopSendHeartBeats() {
        clientFuture?.cancel(false)
    }

    private fun receivedHeartBeats() {
        if (state.compareAndSet(state.get(), State.NOT_HEARTBEAT)) {
            LOGGER.error { "Server stopped sending heartbeat." }
            state.getAndSet(State.NOT_HEARTBEAT)
            LOGGER.info { "Setting a new state -> ${state.get()}." }
            sendClose()
        } else LOGGER.info { "Failed to set a new state." }
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
