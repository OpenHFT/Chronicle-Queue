package net.openhft.chronicle.sandbox.replay;

import net.openhft.lang.io.serialization.BytesMarshallable;

/**
 * Created by peter.lawrey on 20/01/15.
 */
public interface TestData extends BytesMarshallable {
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

    String getName();

    void setAge(long age);

    long getAge();

    void setImportance(double importance);

    double getImportance();

    void setTimestamp(long timestamp);

    long getTimestamp();
}
