/*
 * Copyright 2016-2022 chronicle.software
 *
 *       https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue.util;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

public final class PretouchUtil implements PretoucherFactory {
    public static final PretoucherFactory INSTANCE;

    static {
        PretoucherFactory instance;
        try {
            final Class<?> clazz = Class.forName("software.chronicle.enterprise.queue.pretoucher.EnterprisePretouchUtil");
            instance = (PretoucherFactory) ObjectUtils.newInstance(clazz);
            assert SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable();
        } catch (Exception e) {
            instance = new PretouchUtil();
            SingleChronicleQueueBuilder.onlyAvailableInEnterprise("Pretoucher");
        }
        INSTANCE = instance;
    }

    @Override
    public EventHandler createEventHandler(@NotNull final SingleChronicleQueue queue) {
        return () -> false;
    }

    @Override
    public Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue) {
        return new EmptyPretoucher();
    }

    private static class EmptyPretoucher implements Pretoucher {
        @Override
        public void execute() throws InvalidEventHandlerException {
        }

        @Override
        public void close() {
        }
    }
}