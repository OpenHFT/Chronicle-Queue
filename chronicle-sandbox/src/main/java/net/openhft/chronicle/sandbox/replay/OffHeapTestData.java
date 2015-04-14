/*
 * Copyright 2015 Higher Frequency Trading
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

package net.openhft.chronicle.sandbox.replay;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.MaxSize;

/**
 * Created by peter.lawrey on 20/01/15.
 */
public interface OffHeapTestData extends Byteable {
    /*
    CREATE TABLE testSet(
        id BIGINT NOT NULL AUTO_INCREMENT,
        name LONGTEXT NOT NULL,
        age BIGINT NOT NULL,
        importance DOUBLE NOT NULL,
        time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB//
    */
    void setName(@MaxSize(16) CharSequence name);

    String getName();

    void setAge(long age);

    long getAge();

    void setImportance(double importance);

    double getImportance();

    void setTimestamp(long timestamp);

    long getTimestamp();
}
