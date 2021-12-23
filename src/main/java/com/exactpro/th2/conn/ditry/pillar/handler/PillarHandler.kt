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

import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler
import com.exactpro.th2.conn.dirty.tcp.core.api.impl.Channel
import com.exactpro.th2.conn.ditry.pillar.handler.util.*
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

class PillarHandler(private val channel: Channel): IProtocolHandler {
    private val logger = KotlinLogging.logger {}

    private var state = State.SESSION_CLOSE

    val settings = PillarHandlerSettings()

    private val executor = Executors.newSingleThreadScheduledExecutor()

    private var clientFuture: Future<*>? = null

    private lateinit var sentHeartbeatCommand: Runnable

    private var serverFuture: Future<*>? = null

    private lateinit var receivedHeartbeatCommand: Runnable

    private lateinit var streamId: StreamIdEncode

    fun initCommand(){
        if(settings.heartbeatInterval <= 0L) {
            throw Exception("Heartbeat sending interval must be greater than zero")
        }

        sentHeartbeatCommand = Runnable {
            startSendHeartBeats()
        }

        if(settings.streamAvailInterval <= 0L) {
            throw Exception("StreamAvail sending interval must be greater than zero")
        }

        receivedHeartbeatCommand = Runnable {
            receivedHeartBeats()
        }
    }

    override fun onOpen() {
        when (state){
            State.SESSION_CLOSE -> {
                state = State.SESSION_CREATED
                logger.info { "Setting a new state -> $state" }

                channel.open()

                val login = Login(settings)
                channel.send(login.login(), messageMetadata(MessageType.LOGIN), IChannel.SendMode.MANGLE)

                initCommand()
                startSendHeartBeats()
                logger.info { "Connected handler" }
            }
            else -> logger.info { "Handler is already connected" }
        }
    }

    override fun onReceive(buffer: ByteBuf): ByteBuf? {

        val bufferLength = buffer.readableBytes()
        if (bufferLength == 0) {
            logger.warn { "Cannot parse empty buffer" }
            return null
        }

        buffer.markReaderIndex()
        val messageType = buffer.readShort().toInt()
        val messageLength = buffer.readShort().toInt()
        buffer.resetReaderIndex()

        if (!MessageType.contains(messageType)){
            buffer.resetReaderIndex()
            throw Exception ("Message type is not supported: $messageType")
        }

        if(bufferLength < messageLength) {
            logger.warn { "Buffer length is less than the declared one: $bufferLength -> $messageLength" }
            return null
        }

        if(bufferLength > messageLength) {
            logger.warn { "Buffer length is longer than the declared one: $bufferLength -> $messageLength" }
            val buf: ByteBuf = Unpooled.buffer(messageLength)
            buffer.resetReaderIndex()
            buf.writeBytes(buffer, 0, messageLength)
            buffer.readerIndex(messageLength)
            return buf
        }

        return buffer
    }

    override fun onIncoming(message: ByteBuf): Map<String, String> {
        val msgHeader = MsgHeader(message)

        when (val msgType = msgHeader.type) {
            MessageType.LOGIN_RESPONSE.type -> {
                logger.info { "Type message - LoginResponse" }
                val mapLoginResponse = LoginResponse(message).loginResponse()

                when (val status = Status.getStatus(mapLoginResponse[STATUS_FIELD_NAME]!!.toShort())) {
                    Status.OK -> {
                        logger.info("Login successful. Start sending heartbeats.")
                        state = State.LOGGED_IN
                        logger.info { "Setting a new state -> $state" }
                    }
                    Status.NOT_LOGGED_IN -> {
                        if(state == State.SESSION_CREATED) {
                            stopSendHeartBeats()
                            state = State.SESSION_CLOSE
                        }
                        logger.info("Received `not logged in` status. Fall in to error state.")
                        close()
                    }
                    else -> {
                        throw Exception("Received $status status.")
                    }
                }
                return messageMetadata(MessageType.LOGIN_RESPONSE)
            }

            MessageType.STREAM_AVAIL.type -> {
                logger.info { "Type message - StreamAvail" }
                val mapStreamAvail = StreamAvail(message).streamAvail()
                streamId = StreamIdEncode(StreamId(message))
                val open = Open(
                    streamId.streamId(),
                    mapStreamAvail[NEXT_SEQ_FIELD_NAME]!!.toInt(),
                    mapStreamAvail[STREAM_TYPE_FIELD_NAME]!!.toInt()
                )

                channel.send(
                    open.open(),
                    messageMetadata(MessageType.OPEN), IChannel.SendMode.MANGLE
                )

                serverFuture =
                    executor.schedule(receivedHeartbeatCommand, settings.streamAvailInterval, TimeUnit.MILLISECONDS)

                return messageMetadata(MessageType.STREAM_AVAIL)
            }

            MessageType.OPEN_RESPONSE.type -> {
                logger.info { "Type message - OpenResponse" }
                val mapOpenResponse = OpenResponse(message).openResponse()

                when (Status.getStatus(mapOpenResponse[STATUS_FIELD_NAME]!!.toShort())) {
                    Status.OK -> {
                        logger.info("Open successful.")
                    }
                    Status.NO_STREAM_PERMISSION -> {
                        logger.warn { "No stream permission" }
                    }
                    else -> {
                        throw Exception("This is not an OpenResponse status")
                    }
                }
                return messageMetadata(MessageType.OPEN_RESPONSE)
            }

            MessageType.CLOSE_RESPONSE.type -> {
                logger.info { "Type message - CloseResponse" }
                val mapCloseResponse = CloseResponse(message).closeResponse()
                when (Status.getStatus(mapCloseResponse[STATUS_FIELD_NAME]!!.toShort())) {
                    Status.OK -> {
                        logger.info("Open successful.")
                    }
                    Status.STREAM_NOT_OPEN -> {
                        logger.warn { "Stream not open" }
                    }
                    else -> {
                        throw Exception("This is not an CloseResponse status")
                    }
                }
                return messageMetadata(MessageType.CLOSE_RESPONSE)
            }

            MessageType.SEQMSG.type -> {
                logger.info { "Type message - SeqMsg" }
                return messageMetadata(MessageType.SEQMSG)
            }

            else -> {
                throw Exception("Message type is not supported: $msgType")
            }
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
        when (state != State.SESSION_CLOSE) {
            true -> {
                state = State.SESSION_CLOSE
                logger.info { "Setting a new state -> $state" }

                channel.close()
                logger.info { "Disconnected handler" }
            }
            else -> logger.info { "Handler is already disconnected" }
        }
    }

    override fun close() {
        executor.shutdownNow()
        state = State.LOGGED_OUT
        logger.info { "Setting a new state -> $state" }
        val close = Close(streamId.streamId())
        channel.send(close.close(), messageMetadata(MessageType.CLOSE), IChannel.SendMode.MANGLE)
    }

    private fun startSendHeartBeats(){
        val heartbeat = Heartbeat()
        channel.send(heartbeat.heartbeat(), messageMetadata(MessageType.HEARTBEAT), IChannel.SendMode.MANGLE)
        clientFuture = executor.schedule(sentHeartbeatCommand, settings.heartbeatInterval, TimeUnit.MILLISECONDS)
    }

    private fun stopSendHeartBeats(){
        if (clientFuture != null) {
            clientFuture!!.cancel(false)
        }
    }

    private fun receivedHeartBeats(){
        logger.error { "Server stopped sending heartbeat" }
        state = State.NOT_HEARTBEAT
        logger.info { "Setting a new state -> $state" }
        close()
    }

    private fun messageMetadata(messageType: MessageType): Map<String, String>{
        val metadata = mutableMapOf<String, String>()
        metadata[TYPE_FIELD_NAME] = messageType.type.toString()
        metadata[LENGTH_FIELD_NAME] = messageType.length.toString()
        return metadata
    }
}
