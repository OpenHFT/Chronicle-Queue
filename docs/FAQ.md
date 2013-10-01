# Frequently Asked Questions about Chronicle

## What is Chronicle designed for?

Chronicle is design to be a record everything of interest logger and persisted IPC.
A key requirement of low latency systems is transparency and you should be able to record enough information
that a monitoring system can recreate the state of the system monitored.  This allows downstream systems to record any information
they need and perform queries without needed to touch the critical system as they can record anything they might need later.

##  How fast is fast?

Chronicle is design to persist messages and replay them in micro-second time.  Simple messages are as low as 0.1 micro-seconds.
Complex messages might take 10 micro-seconds to write and read.

## How does it scale?

It scales vertically.  Many distributed systems can scale by adding more boxes.  They are designed to handle between 100 and 1000 transactions per second per node.
Chronicle is design to handle more transaction per node, in the order of 100K to 1M transactions per second.  This means you need far less nodes, between 10 and 100 times less.

Vertical scalability is essential for low latency as having more nodes usually increases latency.

Having one node which can handle the load of data centre also save money and power consumption.

## What types of Excerpt are there?

Chronicle 2.x has three types of excerpt optimised for different purposes.

    Chronicle chronicle = new IndexedChronicle(basePath);
    ExcerptAppender appender = chronicle.createAppender(); // sequential writes.
    ExcerptTailer tailer = chronicle.createTailer();       // sequential reads.
    Excerpt excerpt = chronicle.createExcerpt();           // random access to existing excerpts.

The plain Except is slower so only use this is to need random access.

## How does writing work?

You start by making sure there is enough free spaces with

    appender.startExcerpt(capacity);

It doesn't matter to much if the capacity is more than you need as the entry is shrink wrapped at the end.
Making the capacity too large only matters at the end of a chunk as it can trigger a new chunk to be allocated when perhaps the end of the previous one would have been enough.
Making the capacity a few KB more than it needs to be will have little measurable difference.

Then you write the text or binary to the excerpt with the variety of RandomDataOutput (binary) or ByteStringAppender (text) methods.
These are all designed to operate without creating garbage.

    appender.writeXxxx(xxx); // write binary
    appender.append(value);  // write text

Note: Serializable objects are supported for compatibility and are ok in small doses.  Java Serialization creates a lot of garbage for writing and reading.

Say you want to write to an excerpt later and you don't want shrink wrapping

    // don't do this unless you want to pad the excerpts.
    appender.position(capacity); // tell the appender the whole capacity is needed.

At this point if the program crashes, the entry is lost. On restart, the entry will be ignored.

To finish writing (or reading) call finish();

    appender.finish(); // update the index.

When finish() returns, the data will be written to disk even if the program crashes. By crash I mean a JVM crash not just an Error or Exception thrown.

## How does reading work?

When you read an excerpt, it first checks that index entry is there (the last thing written)

    if(tailer.nextIndex()) {
       // read the new excerpt
       Type value = tailer.readXxxx(); // read a data type.
    }

Finally you must call finish() to perform bounds check.  Chronicle doesn't bounds check every access to reduce runtime overhead.

    tailer.finish();

## I want to store large messages, what is the limit.

The theoretic limit is about 1 GB as Chronicle 2.x still uses Java's memory mappings.
The practical limit without tuning the configuration is about 64 MB.
At this point you get significant inefficiencies unless you increase the data allocation chunk size.