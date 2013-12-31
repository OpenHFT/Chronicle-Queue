Vanilla Chronicle
============

Vanilla Chronicle is a designed for more features rather than just speed. It it is fast enough, it should be easier to use/manage.  It supports

 - rolling files on a daily, weekly or hourly basis.
 - concurrent writers on the same machine.
 - concurrent readers on the same machine or across multiple machines via TCP replication.
 - zero copy serialization and deserialization.
 - millions of writes/reads per second on commodity hardware. <br/>(~5 M messages / second for 96 byte messages on a i7-4500 laptop)
 - synchronous persistence as required. (commit to disk before continuing)
 - reader on source can wait for replication. i.e. source reader sees excerpts after replication acknowledge.
 - data files have more information for rebuilding indexes.
 - exact length of entries

File Format
------------

The directory structure is as follows.

<pre>
base-directory /
   {cycle-name} /       - The default format is yyyyMMdd
        index-{n}       - multiple index files from 0 .. {n}
        data-{tid}-{m}  - multiple data files for each thread id (matches the process id) from 0 .. {n}
</pre>

The index file format is an sequence of 8-byte values which consist of a 16-bit {tid} and the offset in bytes of the start of the record.

The data file format has a 4-byte length of record. The length is the inverted bits of the 4-byte value.
This is used to avoid seeing regular data as a length and detect corruption.  The length always starts of a 4-byte boundary.

TCP Replication
---------------

Each *source* can have any number of down stream *sinks*.  With TCP replication this works well up to 10 consumers, above this you may get scalability issues.

When a *sink* connects to a *source*, it sends the last entry it had and the source will send entries from there.

The source sends a message for;

 - each entry, existing or new.
 - when there is a new cycle.
 - when the source is in sync.
 - a heartbeat every 2.5 seconds.


Concurrent Producer
-------------------

Any number of threads can be writing to the Chronicle at the same time provided

 - you only append OR
 - you modify records using a lock or CAS operation

Example

    ExcerptAppender appender = chronicle.createAppender();
     
    // for each record
    appender.startExcept();    // can be called by any number of threads/processes at once
    appender.writeXxxx( ... ); // write binary
    appender.append( ... );    // write text
    appender.finish();

Concurrent Consumers
--------------------

Consumers can work on either a Topic basis (the default) or can be applied on a Queue basis (where only one consumer "gets" a message)

    ExcerptTailer tailer = chronicle.createTailer();
    int threadId = AffinitySupport.getThreadId();
    
    // in a busy loop, check there is an excerpt and this is the only consumer.
    if (tailer.hasNext() && tailer.compareAndSwapInt(0L, 0, threadId)) {
        tailer.position(4); // skip the lock.
        // binary format
        long v = tailer.readXxxx();
        String text = tailer.readEnum(String.class); // read a UTF-8 String using a string pool.
        // text format
        long x = tailer.parseLong();
        double y = tailer.parseDouble();
        tailer.finish();
    }


