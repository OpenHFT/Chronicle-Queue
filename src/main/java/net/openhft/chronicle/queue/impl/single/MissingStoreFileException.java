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

package net.openhft.chronicle.queue.impl.single;

/**
 * This exception is thrown when a store file, which is expected to be present,
 * is missing. The missing file could be due to accidental deletion or
 * other external causes, potentially leading to issues in data consistency.
 *
 * <p>This class extends {@link IllegalStateException} and provides an
 * informative message to identify the missing store file.
 */
public class MissingStoreFileException extends IllegalStateException {
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a {@code MissingStoreFileException} with the specified detail message.
     *
     * @param s The detail message, indicating which file is missing or relevant information.
     */
    public MissingStoreFileException(String s) {
        super(s);
    }
}
