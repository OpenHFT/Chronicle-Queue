/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.queue;

import java.util.List;

public interface JDBCResult {
    void queryResult(List<String> columns, List<List<Object>> rows, String query, Object... args);

    void queryThrown(Throwable t, String query, Object... args);

    void updateResult(long count, String update, Object... args);

    void updateThrown(Throwable t, String update, Object... args);
}
