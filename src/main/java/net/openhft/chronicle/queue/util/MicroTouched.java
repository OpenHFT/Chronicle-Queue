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

/**
 * Interface representing small operations that can be used to reduce jitter in threads.
 * <p>Provides methods to perform tiny operations either on the current thread or in a background thread to improve performance consistency.
 */
public interface MicroTouched {

    /**
     * Performs a tiny operation to improve jitter in the current thread.
     * <p>This method should be called in contexts where reducing jitter or improving performance consistency is desired.
     *
     * @return {@code true} if the operation was successful, otherwise {@code false}
     */
    boolean microTouch();

    /**
     * Performs a small operation to improve jitter in a background thread.
     * <p>This method is designed to be executed in a background thread to smooth out performance fluctuations.
     */
    void bgMicroTouch();
}
