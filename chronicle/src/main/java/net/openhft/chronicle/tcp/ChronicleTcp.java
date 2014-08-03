/*
 * Copyright 2014 Higher Frequency Trading
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.tcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

public class ChronicleTcp {
    public static final Logger LOG = LoggerFactory.getLogger(ChronicleTcp.class);

    public static final class Command {
        public static final long ACTION_SUBSCRIBE = 1;
        public static final long ACTION_QUERY     = 2;

        private ByteBuffer buffer;
        private long action;
        private long data;

        public Command() {
            this.buffer = ByteBuffer.allocate(16).order(ByteOrder.nativeOrder());
            this.action = 0;
            this.data = 0;
        }

        public long action() {
            return this.action;
        }

        public long data() {
            return this.data;
        }

        public Command read(final SocketChannel channel) throws IOException {
            this.buffer.clear();

            LOG.info("ChronicleTcp.Command.Read");

            ChronicleTcpUtil.readFullyOrEOF(channel, this.buffer);


            this.action = buffer.getLong();
            this.data   = buffer.getLong();

            LOG.info("action " + action);
            LOG.info("data " + data);

            return this;
        }

        public boolean isSubscribe() {
            return this.action == ACTION_SUBSCRIBE;
        }

        public boolean isQuery() {
            return this.action == ACTION_QUERY;
        }

        public static ByteBuffer newCommand(long action, long data) {
            return ByteBuffer.allocate(16).order(ByteOrder.nativeOrder()).putLong(action).putLong(data);
        }
    }
}
