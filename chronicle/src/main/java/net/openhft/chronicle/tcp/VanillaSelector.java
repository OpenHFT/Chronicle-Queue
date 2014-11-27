/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

import net.openhft.chronicle.tools.ChronicleTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class VanillaSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(VanillaSelector.class);

    private VanillaSelectionKeySet selectionKeySet;
    private Selector selector;

    public VanillaSelector() {
        this.selectionKeySet = new VanillaSelectionKeySet();
        this.selector = null;
    }

    public void open() throws IOException {
        this.selector = Selector.open();

        try {
            final Class<?> selectorImplClass =
                Class.forName("sun.nio.ch.SelectorImpl", false, ChronicleTools.getSystemClassLoader());

            if (selectorImplClass.isAssignableFrom(this.selector.getClass())) {
                final Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                selectedKeysField.setAccessible(true);

                final Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");
                publicSelectedKeysField.setAccessible(true);

                selectedKeysField.set(this.selector, this.selectionKeySet);
                publicSelectedKeysField.set(this.selector, this.selectionKeySet);
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void register(SocketChannel channel, int ops, VanillaSelectionKeyConsumer consumer) throws IOException {
        channel.register(this.selector, ops, consumer);
    }

    public void deregister(SocketChannel channel, int ops) throws IOException {
        SelectionKey selectionKey = channel.keyFor(this.selector);
        if (selectionKey != null) {
            selectionKey.interestOps(selectionKey.interestOps() & ~ops);
        }
    }

    private void select(int spinLoopCount, long timeout) throws IOException {
        for (int i = 0; i < spinLoopCount; i++) {
            if (selector.selectNow() != 0) {

                SelectionKey[] keys = selectionKeySet.flip();
                VanillaSelectionKeyConsumer consumer = null;

                if (keys.length != 0) {
                    for (int k = 0; ; k++) {
                        final SelectionKey key = keys[k];
                        if (key != null) {
                            consumer = (VanillaSelectionKeyConsumer)key.attachment();

                            if(consumer != null) {
                                consumer.onKey(key);
                            }
                        } else {
                            break;
                        }

                        /*
                        try {
                            processKey(approxTime, key);
                        } catch (BufferUnderflowException e) {
                            if (!isClosed)
                                LOG.error("", e);
                        }
                        */
                    }
                }

                return;
            }
        }

        selector.select(timeout);
    }
}
