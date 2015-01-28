package net.openhft.chronicle.sandbox.replay;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.MaxSize;

/**
 * Created by peter on 20/01/15.
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
