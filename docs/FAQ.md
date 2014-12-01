# Frequently Asked Questions about Chronicle

## With a tight reader loop I see 100% utilization, will there be processing capability left for anything else ?

####  Question
I looked at the demo and demo2 samples and noticed that the reader thread usually goes in tight loops, continually trying to read the next excerpt. As a result I noticed that one processor (the one doing the reading presumably) has a close to 100% utilization. If I have N processes each with one such reader thread then I assume N processors will have 100% utilization. If N exceeds the number of processors then the CPU utilization will be 100% and there will be no more processing capability left for anything else. Do you see any flaw in my reasoning?

#### Answer
Your reasoning is fine. If you want to minimise latency you want to have a small number of dedicated cpus. Most applications only need a small number of these critical threads. In fact 6 is the most I have ever seen for a tuned system and you can easily buy a server with much more cores than this.
Part of the reason you don't need more cpus because by this point your bottleneck has moved somewhere else such as the memory bandwidth (in synthetic test) but more often its something on the network or an upstream/downstream services over which you have no control eg an exchange.

## What is Chronicle designed for?

Chronicle is design to be a record everything of interest logger and persisted IPC.
A key requirement of low latency systems is transparency and you should be able to record enough information
that a monitoring system can recreate the state of the system monitored.  This allows downstream systems to record any information
they need and perform queries without needed to touch the critical system as they can record anything they might need later.

Chronicle works best in SEDA style event driven systems, where latency is critical and you need a record of exactly what was performed when. (Without expensive network sniffing/recording systems)

## What was the library originally designed for?

The original design was for a low latency trading system which required persistence of everything in and out for a complete record of
what happened, when and for deterministic testing. The target round trip time for persisting the request, processing and persisting the response was a micro-second.

Key principles are; ultra-low GC (less than one object per event), lock-less, cache friendly data structures.

## What was not in the originally design?

The marshalling, de-marshalling and handling of thread safe off heap memory has been added more recently and moving into the Java-Lang module.

This library now supports low latency/GC-less writing and reading/parsing or text as well as binary.

##  How fast is fast?

Chronicle is design to persist messages and replay them in micro-second time.  Simple messages are as low as 0.1 micro-seconds.
Complex messages might take 10 micro-seconds to write and read.

Chronicle is designed to sustain millions of inserts and updates per second. For burst of up to 10% of your main memory, you can sustain rates of 1 - 3 GB/second written.
e.g. A laptop with 8 GB of memory might handle bursts of 800 MB at a rate of 1 GB per second.
A server with 64 GB of memory might handle a burst of 6.5 GB at a rate of 3 GB per second.

If your key system is not measuring latency in micro-seconds and throughput in thousands per second, it is not that fast.  It may well be fast enough however. ;)

## How does it scale?

It scales vertically.  Many distributed systems can scale by adding more boxes.  They are designed to handle between 100 and 1000 transactions per second per node.
Chronicle is design to handle more transaction per node, in the order of 100K to 1M transactions per second.  This means you need far less nodes, between 10 and 100 times less.

Vertical scalability is essential for low latency as having more nodes usually increases latency.

Having one node which can handle the load of data centre also save money and power consumption.

## What if I have a slow consumer?

Chronicle has an advantage over other queuing systems that the consumer can be any amount behind the producer (up to the free space on your disk)
Chronicle has been tested where the consumer was more than main memory behind the producer.  This reduced the maximum throughput by about half.
Most systems, in Java, where the queue exceed the main memory cause the machine to become unusable.

Note: the Consumer can stop, restart and continue with minimal impact to the producer, if the data is still in main memory.

Having a faster disk sub-system helps in extreme conditions like these.
Chronicle has been tested on a laptop with and HDD with a write speed of 12 MB/s and an over-clocked hex core i7 PCI-SSD card which sustained write speed of 900 MB/s.

## What types of Excerpt are there?

Chronicle has three types of excerpt optimised for different purposes.

    Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath);
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

## What do I do if I really don't know the size?

If you have no control of the size you have a concern that you don't control the system.
If this is a rare event, and you have to, you can create a large NativeBytes off heap buffer of any size (even > 2 GB)
You can serialize into this off heap memory and write the Bytes produced with the actual size.

Note: huge memory to memory copies do not have safe points, which means a 1 GB raw memory copy can prevent a GC from starting until it is finished.

## How does reading work?

When you read an excerpt, it first checks that index entry is there (the last thing written)

    if(tailer.nextIndex()) {
       // read the new excerpt
       Type value = tailer.readXxxx(); // read a data type.
    }

Finally you must call finish() to perform bounds check.  Chronicle doesn't bounds check every access to reduce runtime overhead.

    tailer.finish();

## How is disk space managed?
A key assumption is that disk space is cheap, or at least it should be.  Some organizations have amazing unrealistic (almost unprofessional) internal charging rates,
but you should be able to get 100 GB for about one hour of your time.  This assumes retail costs for disk compares with minimum wage.
The organizational cost of disk is often 10-100x the real cost, but so is your cost to the business.

In essence, disk should be cheap and you can record a week to a month of continuous data on one cheap drive.

Never the less, there is less maintenance overhead if the chronicle logs rotate themselves and there is work being done to implement this for Chronicle 2.1.
 Initially, chronicle files will be rotated when they reach a specific number of entries.

## I want to use Chronicle as an off heap cache.  What do I do?

Chronicle is designed for replay.  While it can, and has been used as an off heap persisted cache, it doesn't do this very easily.
An old library called HugeCollections will be resurrected to handle collections more cleanly.

# Thread safety

## Can I have multiple readers?

A given Chronicle can safely have many readers, both inside and outside of the process creating it.

To have multiple readers of a Chronicle, you should generally create a new Chronicle per reader pointing at the same underlying Journal. On each of these Chronicles, you will call createTailer and get a new tailer that can be used to read it. These Tailers should never be shared.
A less performant option to this is to share a single Chronicle and Tailer and lock access with synchronized or ReentrantLock. Only one Tailer should ever be active at the same time.

## Can I have multiple writers?

A given Chronicle should only have a single writer. It is not threadsafe for multiple threads to write to the same Chronicle.
Multiple writers in the same process will cause a performance degradation. If you still want to, you need to use some form of external locking.  Either synchronized or ReentrantLock may be suitable.

You cannot safely write to the same Chronicle from multiple processes.

# Replication

## Does Chronicle support replication?

Yes, you can wrap the source (single master) with ChronicleSource and the copies with IChronicleSink.
This supports TCP replication and means a copy is stored on each client. When file rolling is supported, this will make it easier to delete old files.

## Does Chronicle support UDP replication?

No, Chronicle is designed to be both reliable and deterministic.  UDP is not designed for this.  A hybrid UDP/TCP system is possible is the future.

## How do I know the consumer is up to date?

For the tailer, either replicated or not, you can assume you are up to date when nextIndex() returns false for the first time.

# Infrequently Asked Questions

## Can records be updated?

They can be updated at any time, but you lose any event driven notification to readers at this point.
It might be practical to have multiple chronicles, one which stores large updated records, and another for small notifications.

## I want to store large messages, what is the limit.

The theoretic limit is about 1 GB as Chronicle 2.x still uses Java's memory mappings.
The practical limit without tuning the configuration is about 64 MB.
At this point you get significant inefficiencies unless you increase the data allocation chunk size.

## I get an Exception writing or finish()ing an excerpt. What does this mean?

Most often this means you wrote more than the capcity allowed.  The quaility of the messages is improving?

## I get an Exception attempting to read an Excerpt. What does this mean?

Most likely your read code doesn't match your write code.  I suggest making your reader and writer separate, stand alone and well tested so you can pick up such errors.

## How does the byte order work with replication?

The byte order doesn't change in replication.  This means it will work best in a byte endian homogeneous systems. e.g. Windows/Linux x86/x64/ARM.
Chronicle may support changing the byte order in future.

## Does chronicle support other serialization libraries?

Chronicle supports ObjectInput, ObjectOutput, Appendable, OutputStream and InputStream APIs.  It also has a fast copy to/from a byte[].

Chronicle is designed to be faster with persistence than other serialization libraries are without persistence.
To date, I haven't found a faster library for serialization without a standardized format. e.g. Chronicle doesn't support JSON or XML yet.

Where XML or JSon is neede down stream, I suggest writing in binary format and have the reader incur the overhead of the conversion rather than slow the producer.

## Does Chronicle support a synchronous mode?

It does, clone() a ChronicleConfig you like, e.g. DEFAULT and set synchronousMode(true).
This will force() a persistence for every finish().  What this does is likely to be OS platform dependant.
