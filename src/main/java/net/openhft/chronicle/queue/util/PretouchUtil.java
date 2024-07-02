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
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.Pretoucher;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

public final class PretouchUtil {
    private static final PretoucherFactory INSTANCE;

    static {
        PretoucherFactory instance;
        try {
            final Class<?> clazz = Class.forName("software.chronicle.enterprise.queue.pretoucher.EnterprisePretouchUtil");
            instance = (PretoucherFactory) ObjectUtils.newInstance(clazz);
            assert SingleChronicleQueueBuilder.areEnterpriseFeaturesAvailable();
        } catch (Exception e) {
            instance = new PretouchFactoryEmpty();
            SingleChronicleQueueBuilder.onlyAvailableInEnterprise("Pretoucher");
        }
        INSTANCE = instance;
    }

    public static EventHandler createEventHandler(@NotNull final ChronicleQueue queue) {
        return INSTANCE.createEventHandler((SingleChronicleQueue) queue);
    }

    public static Pretoucher createPretoucher(@NotNull final ChronicleQueue queue) {
        return INSTANCE.createPretoucher((SingleChronicleQueue) queue);
    }

    private static class PretouchFactoryEmpty implements PretoucherFactory {

        @Override
        public EventHandler createEventHandler(@NotNull final SingleChronicleQueue queue) {
            return () -> false;
        }

        @Override
        public Pretoucher createPretoucher(@NotNull final SingleChronicleQueue queue) {
            return queue.createPretoucher();
        }
    }
}
