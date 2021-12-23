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

import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandlerSettings

class PillarHandlerSettings: IProtocolHandlerSettings {
    var username: String = "username"

    var password: String = "password"

    var mic: String = "mic"

    var version: String = "1.1"

    var heartbeatInterval: Long = 1000L

    var streamAvailInterval: Long = 5000L
}