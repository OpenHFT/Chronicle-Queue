/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
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

import net.openhft.lang.io.serialization.BytesMarshallable;

public interface TestData extends BytesMarshallable {
    String getName();

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
    void setName(CharSequence name);

    long getAge();

    void setAge(long age);

    double getImportance();

    void setImportance(double importance);

    long getTimestamp();

    void setTimestamp(long timestamp);
}
