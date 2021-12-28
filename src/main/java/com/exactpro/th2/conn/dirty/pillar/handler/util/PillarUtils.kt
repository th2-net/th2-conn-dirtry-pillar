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

package com.exactpro.th2.conn.dirty.pillar.handler.util

import kotlin.experimental.and
import kotlin.experimental.or

const val TYPE_FIELD_NAME = "type"
const val LENGTH_FIELD_NAME = "length"
const val MODE_LOSSY = 0

enum class State(val value: Int){
    SESSION_CREATED(0),
    SESSION_CLOSE(1),
    LOGGED_IN(2),
    LOGGED_OUT(3),
    NOT_HEARTBEAT(4);
}

enum class Status(val value: Short) {
    OK(0),
    NOT_LOGGED_IN(18),
    INVALID_LOGIN_DETAILS(24),
    ALREADY_LOGGED_IN(27),
    HEARTBEAT_TIMEOUT(28),
    LOGIN_TIMEOUT(29),
    INVALID_MESSAGE(33),
    NO_STREAM_PERMISSION(54),
    STREAM_NOT_OPEN(85);

    companion object {
        fun getStatus(value: Short): Status {
            return values().find { it.value == value }!!
        }
    }
}

enum class Access(val value: Short) {
    READ(1),
    WRITE(2),
    THROTTLE_REJECT(4);

    companion object {
        fun getPermission(permission: Short): Short {
            var result: Short = 0
            values().forEach { access ->
                result = result or (access.value and permission)
            }
            return result
        }
    }
}

enum class StreamType (val value: Byte){
    TG(15),
    GT(13),
    REF(33),
    XDP(27);
    companion object {
        fun getStream(value: Byte): StreamType? {
            return values().find { it.value == value }
        }
    }
}

enum class MessageType(val type: Int, val length: Int) {
    LOGIN(513, 76),
    LOGIN_RESPONSE(514, 21),
    STREAM_AVAIL(515, 21),
    HEARTBEAT(516, 4),
    OPEN(517, 30),
    OPEN_RESPONSE(518, 14),
    CLOSE(519, 12),
    CLOSE_RESPONSE(520, 13),
    SEQMSG(2309, 32);

    companion object {
        fun getEnum(type: Int): MessageType? {
            return values().find { it.type == type }
        }

        fun contains(type: Int): Boolean {
            return values().find { it.type == type } != null
        }
    }
}