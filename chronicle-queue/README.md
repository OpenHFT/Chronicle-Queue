*We can help you get Chronicle up and running in your organisation, we suggest you invite us in for consultancy, charged on an ad-hoc basis, we can discuss the best options tailored to your individual requirements. - [Contact Us](sales@chronicle.software)*

*Or you may already be using Chronicle and just want some help - [find out more..](http://chronicle.software/support/)*

# Chronicle Queue

Inter Process Communication ( IPC ) with sub millisecond latency and able to store every message:
![Chronicle](http://chronicle.software/wp-content/uploads/2014/07/ChronicleQueue_200px.png)



Releases are available on maven central as

 ```xml
<dependency>
  <groupId>net.openhft</groupId>
  <artifactId>chronicle</artifactId>
  <version><!--replace with the latest version--></version>
</dependency>
```

Snapshots are available on [OSS sonatype](https://oss.sonatype.org/content/repositories/snapshots/net/openhft/chronicle)

Click here to get the [Latest Version Number](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.openhft%22%20AND%20a%3A%22chronicle%22)


### Contents
* [Overview](https://github.com/OpenHFT/Chronicle-Queue#overview)
* [Building Blocks](https://github.com/OpenHFT/Chronicle-Queue#building-blocks)
* [Chronicle Queue V3](https://github.com/OpenHFT/Chronicle-Queue#chronicle-queue-v3)
  * [Indexed Chronicle](https://github.com/OpenHFT/Chronicle-Queue#indexed-chronicle)
  * [Vanilla Chronicle](https://github.com/OpenHFT/Chronicle-Queue#vanilla-chronicle)
  * [Getting Started](https://github.com/OpenHFT/Chronicle-Queue#getting-started)
  * [Replication](https://github.com/OpenHFT/Chronicle-Queue#replication)
    * [Source](https://github.com/OpenHFT/Chronicle-Queue#source)
    * [Sink](https://github.com/OpenHFT/Chronicle-Queue#sink)
    * [Remote Tailer](https://github.com/OpenHFT/Chronicle-Queue#remote-tailer)
    * [Remote Appender](https://github.com/OpenHFT/Chronicle-Queue#remote-appender)
* [Advanced usage](https://github.com/OpenHFT/Chronicle-Queue#advanced-usage)
  * [Off-Heap Data Structures](https://github.com/OpenHFT/Chronicle-Queue#off-heap-data-structures)
    * [Write with Direct Instance](https://github.com/OpenHFT/Chronicle-Queue#write-with-direct-instance)
    * [Write with Direct Reference](https://github.com/OpenHFT/Chronicle-Queue#write-with-direct-reference)
    * [Read with Direct Reference](https://github.com/OpenHFT/Chronicle-Queue#read-with-direct-reference)
    * [Ordering fields of DataValueClasses](https://github.com/OpenHFT/Chronicle-Queue#ordering-fields-of-DataValueClasses)
  * [Reading the Chronicle after a shutdown](https://github.com/OpenHFT/Chronicle-Queue#reading-after-a-shutdown)
  * [Non blocking Remote Client](https://github.com/OpenHFT/Chronicle-Queue#non-blocking-remote-client)
  * [Data Filtering](https://github.com/OpenHFT/Chronicle-Queue#data-filtering)
* [Support](https://github.com/OpenHFT/Chronicle-Queue#support)
* [JavaDoc](http://openhft.github.io/Chronicle-Queue/apidocs/)

## Overview
Chronicle is a Java project focused on building a persisted low latency messaging framework for high performance and critical applications.

![](http://chronicle.software/wp-content/uploads/2014/07/Chronicle-diagram_005.jpg)

At first glance Chronicle Queue can be seen as **yet another queue implementation** but it has major design choices that should be emphasised.

Using non-heap storage options(RandomAccessFile) Chronicle provides a processing environment where applications do not suffer from GarbageCollection. While implementing high performance and memory-intensive applications ( you heard the fancy term "bigdata"?) in Java; one of the biggest problems is GarbageCollection. GarbageCollection (GC) may slow down your critical operations non-deterministically at any time. In order to avoid non-determinism and escape from GC delays off-heap memory solutions are ideal. The main idea is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management interface over off-heap memory so you can build your own solutions over it.
Chronicle uses RandomAccessFiles while managing memory and this choice brings lots of possibilities. Random access files permit non-sequential, or random, access to a file's contents. To access a file randomly, you open the file, seek a particular location, and read from or write to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can access any random index "directly" using pointers. File portions can be used as ByteBuffers if the portion is mapped into memory.

This memory mapped file is also used for exceptionally fast interprocess communication (IPC) without affecting your system performance. There is no Garbage Collection (GC) as everything is done off heap.

![](http://chronicle.software/wp-content/uploads/2014/07/Screen-Shot-2014-09-30-at-11.24.53.png)

## Building Blocks
Chronicle is the main interface for management and can be seen as the Collection class of Chronicle environment. You will reserve a portion of memory and then put/fetch/update records using Chronicle interface.

Chronicle has three main concepts:
  * Tailer (sequential reads)
  * Excerpt (random reads)
  * Appender (sequential writes).

An Excerpt is the main data container in a Chronicle, each Chronicle is composed of Excerpts. Putting data to a chronicle means starting a new Excerpt, writing data into it and finishing Excerpt at the end.
A Tailer is an Excerpt optimized for sequential reads.
An Appender is something like Iterator in Chronicle environment. You add data appending the current chronicle.

## Chronicle Queue V3

Current version of Chronicle-Queue (V3) contains IndexedChronicle and VanillaChronicle implementations.

### IndexedChronicle
IndexedChronicle is a single writer multiple reader Chronicle.

For each record, IndexedChronicle holds the memory-offset in another index cache for random access. This means IndexedChronicle "knows" where the 3rd object resides in memory this is why it named as "Indexed". But this index is just sequential index, first object has index 0, second object has index 1... Indices are not strictly sequential so if there is not enough space in the current chunk, a new chunk is created and the left over space is a padding record with its own index which the Tailer skips.

```
base-directory /
    name.index
    name.data
```

### VanillaChronicle
Vanilla Chronicle is a designed for more features rather than just speed and it supports:
 - rolling files on a daily, weekly or hourly basis.
 - concurrent writers on the same machine.
 - concurrent readers on the same machine or across multiple machines via TCP replication.
 - zero copy serialization and deserialization.
 - millions of writes/reads per second on commodity hardware. <br/>(~5 M messages / second for 96 byte messages on a i7-4500 laptop)
 - synchronous persistence as required. (commit to disk before continuing)
 - exact length of entries

The directory structure is as follows.

```
base-directory /
   {cycle-name} /       - The default format is yyyyMMdd
        index-{n}       - multiple index files from 0 .. {n}
        data-{tid}-{m}  - multiple data files for each thread id (matches the process id) from 0 .. {n}
```

The index file format is an sequence of 8-byte values which consist of a 16-bit {tid} and the offset in bytes of the start of the record.
The data file format has a 4-byte length of record. The length is the inverted bits of the 4-byte value.
This is used to avoid seeing regular data as a length and detect corruption.  The length always starts of a 4-byte boundary.

## Getting Started

### Chronicle Construction
Creating an instance of Chronicle is a little more complex than just calling a constructor.
To create an instance you have to use the ChronicleQueueBuilder.

```java
String basePath = System.getProperty("java.io.tmpdir") + "/getting-started"
Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath).build();
```

In this example we have created an IndexedChronicle which creates two RandomAccessFile one for indexes and one for data having names relatively:

${java.io.tmpdir}/getting-started.index
${java.io.tmpdir}/getting-started.data

### Writing
```java
// Obtain an ExcerptAppender
ExcerptAppender appender = chronicle.createAppender();

// Configure the appender to write up to 100 bytes
appender.startExcerpt(100);

// Copy the content of the Object as binary
appender.writeObject("TestMessage");

// Commit
appender.finish();
```

### Reading
```java
// Obtain an ExcerptTailer
ExcerptTailer reader = chronicle.createTailer();

// While until there is a new Excerpt to read
while(!reader.nextIndex());

// Read the objecy
Object ret = reader.readObject();

// Make the reader ready for next read
reader.finish();
```

### Cleanup

Chronicle-Queue stores its data off heap and it is recommended that you call close() once you have finished working with Excerpts and Chronicle-Queue.

```java
appender.close();
reader.close();
chronicle.close();
```

## Replication

Chronicle-Queue supports TCP replication with optional filtering so only the required record or even fields are transmitted. This improves performances and reduce bandwith requirements.

![](http://chronicle.software/wp-content/uploads/2014/07/Screen-Shot-2015-01-16-at-15.06.49.png)

### Source

A Chronicle-Queue Source is the master source of data
```java
String basePath = System.getProperty("java.io.tmpdir") + "/getting-started-source"

// Create a new Chronicle-Queue source
Chronicle source = ChronicleQueueBuilder
    .indexed(basePath + "/new")
    .source()
    .bindAddress("localhost", 1234)
    .build();

// Wrap an existing Chronicle-Queue
Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath + "/wrap")
Chronicle source = ChronicleQueueBuilder
    .source(chronicle)
    .bindAddress("localhost", 1234)
    .build();
```

### Sink

A Chronicle-Queue sink is a Chronicle-Queue client that stores a copy of data locally (replica).

```java
String basePath = System.getProperty("java.io.tmpdir") + "/getting-started-sink"

// Create a new Chronicle-Queue sink
Chronicle sink = ChronicleQueueBuilder
    .indexed(basePath + "/statefull")
    .sink()
    .connectAddress("localhost", 1234)
    .build();

// Wrap an existing Chronicle-Queue
Chronicle chronicle = ChronicleQueueBuilder.indexed(basePath + "/statefull")
Chronicle sink = ChronicleQueueBuilder
    .sink(chronicle)
    .connectAddress("localhost", 1234)
    .build();
```

### Remote Tailer

A Remote Tailer is a stateless Sink (it operates in memory)

```java
Chronicle chronicle = ChronicleQueueBuilder
    .remoteTailer()
    .connectAddress("localhost", 1234)
    .build();
```

### Remote Appender

A Remote Appender is a Chronicle-Queue implementation which supports append excerpt to a Chronicle-Source.
It is not a full implementation of a Chronicle-Queue as you can only create a single ExcerptAppender.

```java
Chronicle chronicle = ChronicleQueueBuilder
    .remoteAppender()
    .connectAddress("localhost", 1234)
    .build();

ExcerptAppender appender.createAppender();
appender.startExcerpt();
appender.writeLong(100)
appender.finish()
```

The appender can be configured in a fire and forget way (default) or require an ack from the Chronicle Source

```java
Chronicle chronicle = ChronicleQueueBuilder
    .remoteAppender()
    .connectAddress("localhost", 1234)
    .appendRequireAck(true)
    .build();
```

## Advanced usage

### Off-Heap Data Structures

An Exceprt provide all the low-level primitive to read/store data to Chronicle-Queue but it is often convenient and faster to think about interfaces/beans and rely on OpenHFT's code generation.

As example, we want to store some events to Chronicle-Queue so we can write an interface like that:

```java
public static interface Event extends Byteable {
    boolean compareAndSwapOwner(int expected, int value);
    int getOwner();
    void setOwner(int meta);

    void setId(long id);
    long getId();

    void setType(long id);
    long getType();

    void setTimestamp(long timestamp);
    long getTimestamp();
}
```
Now we have the option to automatically generate a concrete class with:
  * DataValueClasses.newDirectInstance(Event.class) which creates a concrete implementation of the given interface baked by an off-heap buffer
  * DataValueClasses.newDirectReference(Event.class) which reates a concrete implementation of the given interface which need to be supplied with a buffer to write to

#### Write with Direct Instance

When we write to an object created with newDirectInstance we write to an off-heap buffer owned by the generated class itself so we do not write directly to an Excerpt, once done we can write to the Excerpt as usual:

```java
final int items = 100;
final String path = System.getProperty("java.io.tmpdir") + "/direct-instance";
final Event event = DataValueClasses.newDirectInstance(Event.class);

try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
    ExcerptAppender appender = chronicle.createAppender();
    for(int i=0; i<items; i++) {
        event.bytes(appender, 0);
        event.setOwner(0);
        event.setType(i / 10);
        event.setTimestamp(System.currentTimeMillis());
        event.setId(i);

        appender.startExcerpt(event.maxSize());
        appender.write(event);
        appender.finish();
    }

    appender.close();
}
```

#### Write with Direct Reference

An object created with newDirectReference does not hold any buffer so we need to provide one which can be an Excerp. By doing so we directly write to the Excerpt's buffer.

```java
final int items = 100;
final String path = System.getProperty("java.io.tmpdir") + "/direct-instance";
final Event event = DataValueClasses.newDirectReference(Event.class);

try (Chronicle chronicle = ChronicleQueueBuilder.vanilla(path).build()) {
    ExcerptAppender appender = chronicle.createAppender();
    for(int i=0; i<items; i++) {
        appender.startExcerpt(event.maxSize());

        event.bytes(appender, 0);
        event.setOwner(0);
        event.setType(i / 10);
        event.setTimestamp(System.currentTimeMillis());
        event.setId(i);

        appender.position(event.maxSize());
        appender.finish();
    }

    appender.close();
}
```

#### Read with Direct Reference

Reading data from an Excerp via a class generated via newDirectReference is very efficient as
  * it does not involve any copy because the de-serialization is performed lazily and only on the needed fileds
  * you do not have to deal with data offsets as it the code to do so is generated for you by newDirectReference

```java
try (ExcerptTailer tailer = chronicle.createTailer()) {
     final Event event = DataValueClasses.newDirectReference(Event.class);
     while(tailer.nextIndex()) {
         event.bytes(tailer, 0);
         // Do something with event
         tailer.finish();
     }
 } catch(Exception e) {
     e.printStackTrace();
 }
 ```


#### Ordering fields of DataValueClasses

![](http://chronicle.software/wp-content/uploads/2014/09/Chronicle-Queue-Group_01.jpg)

By default when classes generated via DataValueClasses are serialized, their fields will be ordered by field size ( smallest first ), however sometimes, especially when you add new fields, you may not want new small fields to be serialize towards the top, You may wish to preserve the existing order of your fields, you can do this by using the @Group annotation to each method. The serialization order of the fields are determined by adding @Group to the set() method, If you wish you can have a number of different methods with the same value in @Group(), Methods with the same value continue to be ordered by size ( smallest first).

See test below
``` java

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.IByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.Group;
import net.openhft.lang.model.constraints.MaxSize;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Rob Austin
 */
public class GroupTest {

    @Test
    public void test() {
        IByteBufferBytes byteBufferBytes = ByteBufferBytes.wrap(ByteBuffer.allocate(1024));

        {
            BaseInterface baseInterface = DataValueClasses.newDirectInstance(BaseInterface.class);

            ((Byteable) baseInterface).bytes(byteBufferBytes, 0);

            baseInterface.setInt(1);
            baseInterface.setStr("Hello World");

            Assert.assertEquals(1, baseInterface.getInt());
            Assert.assertEquals("Hello World", baseInterface.getStr());

        }
        {
            ExtInterface extInterface = DataValueClasses.newDirectReference(ExtInterface.class);
            byteBufferBytes.clear();
            ((Byteable) extInterface).bytes(byteBufferBytes, 0);
            extInterface.setInt2(43);

            Assert.assertEquals(1, extInterface.getInt());
            Assert.assertEquals(43, extInterface.getInt2());
            Assert.assertEquals("Hello World", extInterface.getStr());
            extInterface.setInt(2);

            Assert.assertEquals(2, extInterface.getInt());
        }
    }


    @Test
    public void test2() {
        IByteBufferBytes byteBufferBytes = ByteBufferBytes.wrap(ByteBuffer.allocate(1024));

        {
            ExtInterface extInterface = DataValueClasses.newDirectReference(ExtInterface.class);
            byteBufferBytes.clear();
            ((Byteable) extInterface).bytes(byteBufferBytes, 0);

            extInterface.setInt(1);
            extInterface.setInt2(2);
            extInterface.setStr("Hello World");

            Assert.assertEquals(1, extInterface.getInt());
            Assert.assertEquals(1, extInterface.getInt());
            Assert.assertEquals("Hello World", extInterface.getStr());
        }

        {
            BaseInterface baseInterface = DataValueClasses.newDirectReference(BaseInterface.class);
            byteBufferBytes.clear();
            ((Byteable) baseInterface).bytes(byteBufferBytes, 0);

            Assert.assertEquals(1, baseInterface.getInt());
            Assert.assertEquals("Hello World", baseInterface.getStr());
        }
    }

    public interface BaseInterface {
        String getStr();
        void setStr(@MaxSize(15) String str);
        int getInt();
        void setInt(int i);
    }

    public interface ExtInterface extends BaseInterface {
        int getInt2();

        @Group(1)
        void setInt2(int i);
    }
}
```

### Reading the Chronicle after a shutdown

Let's say my Chronicle Reader Thread dies. When the reader thread is up again, how do we ensure that the reader will read from the point where he left off?

here is a number of solutions. You can:
  * write the results of the message to an output chronicle with with meta data like timings and the source index. If you reread the output to reconstruct your state you can also determine which entry was processed ie. You want to replay any entries read but for which there was no output.
  * you can record the index of the last entry processed in a ChronicleMap.
  * you can reread all the entries and check via some other means whether it is a duplicate or not.
  * you can mark entries on the input, either for load balancing or timestamp in when the entry was read. The last entry read can be found by finding the last entry without a time stamp. You can use the binary search facility to do this efficiently.


Example with mark entries as processed and recovery in case of failure:
```java
protected static class Reader implements Runnable  {
    private final Chronicle chronicle;
    private final int id;
    private final int type;

    public Reader(final Chronicle chronicle, int id, int type) {
        this.chronicle = chronicle;
        this.id = id;
        this.type = type;
    }

    @Override
    public void run() {
        try (ExcerptTailer tailer = chronicle.createTailer().toStart()) {
            final Event event = DataValueClasses.newDirectReference(Event.class);
            while(tailer.nextIndex()) {
                event.bytes(tailer, 0);
                if(event.getType() == this.type) {
                    // Check if the Reader was interrputed before completion
                    if(event.getOwner() == this.id * 100) {
                        // Do something with the Event
                    }
                    // Try to acquire the Excerpt and mark the event as being processed by this Reader
                    else if (event.compareAndSwapOwner(0, this.id * 100)) {
                        // Do something with the Event

                        // Mark the event as processed by this Reader
                        event.compareAndSwapOwner(this.id * 100, this.id);
                    }
                }

                tailer.finish();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Non blocking Remote Client

On a remote client (Synk or Tailer) nextIndex() waits untill some data is received from the Source before return true or false (in case the client receives an heart-Beat, nextIndex returns false) but sometimes you do not want this behavior, i.e. you want to monitor moultiple chronicles so you can set the number of times the client checks for data before giving up:

```java
Chronicle[] chronicles = new Chronicle[] {
    ChronicleQueueBuilder.remoteTailer()
        .connectAddress("localhost", 1234)
        .readSpinCount(5)
        .build(),
    ChronicleQueueBuilder.remoteTailer()
        .connectAddress("localhost", 1235)
        .readSpinCount(5)
        .build()
 };

 for(Chronicle chronicle : chronicles) {
     if(chronicle.nextIndex()) {
         // do something
     }
 }
```
If readSpinCount is set to a value greater than zero, the socket channel is configured to be non-blocking and the read operations spins readSpinCount times before giving up if no data is received.

### Data filtering

By default a remote client receives every bit stored on the source but that is something you may not want as a client may be interested in some specific data or even fields

```java
final Chronicle highLowSink = sink(sinkHighLowBasePath)
    .withMapping(new new MappingFunction() {
        @Override
        public void apply(Bytes from, Bytes to) {
            //date
            to.writeLong(from.readLong());

            //open which we not send out
            from.readDouble();

            // high
            to.writeDouble(from.readDouble());

            //low
            to.writeDouble(from.readDouble());
        })
    .connectAddress("localhost", port)
    .build();
```



##  How the Indexer works and how it stores its data

In chronicle the indexer is use to index your excerpts, it creates indexes and these indexes are 
stored in a number of excerpts, in the chronicle header then index2index tells chronicle where (
 in other words the address of where ) the root first level index is stored.

The indexing works like a tree, but only 2 levels deep, the root of the tree is at index2index 
( this first level index is 1MB in size and there is only one of them, 
it only holds the addresses of the second level indexes, 
there will be many second level indexes ( created on demand ), 
each is about 1MB in size  (this second level index only stores the position of every 64th excerpt), 
so from every 64th excerpt a linear scan occurs.

The indexes are only built when the indexer is run, this could be on a background thread. Each 
index is created into chronicle as an excerpt

So say that you had 64 excerpt, ran the indexer and then added another 1000 excerpts, if you 
wanted excerpt at index 192, the linear scan would only have to look at another 128 entries from 
index 64 ( as this is the last known index ), but ideally you would have the background thread 
continuously index the excerpts as they are added.  More not thing to mention that that we are 
looking to support a TextWire. 
In this case, the structure is the same however the size of the index: and infex2index:
 is larger to accommodate the text format. 
 
 
[Full example](https://github.com/lburgazzoli/Chronicle-Queue/blob/master/chronicle/src/test/java/net/openhft/chronicle/tcp/WithMappedTest.java)


##  Support
* [Chronicle FAQ](https://github.com/OpenHFT/Chronicle-Queue/blob/master/docs/FAQ.md)
* [Chronicle support on StackOverflow](http://stackoverflow.com/tags/chronicle/info)
* [Chronicle support on Google Groups](https://groups.google.com/forum/?hl=en-GB#!forum/java-chronicle)
* [Development Tasks - JIRA] (https://higherfrequencytrading.atlassian.net/browse/CHRON)



# Tuning Chronicle Queue


The main thing to look for in terms of Chronicle Queue is the update rate (MB/s), the dirty buffer size (MB) and write latency to disk (queue length in ms)
You can see all these in the utility ‘atop' 
The update rate is the rate you write data, provided this is lower than the write bandwidth of your disk subsystem over say 5 seconds, you should avoid most serious pauses in the system.  The dirty buffer (which is also visible in /proc/meminfo) tells you how much data is yet to be written to disk and you want this to be well below 10% of main memory.  This is a default hard limit at which point the application stops rather than writing more data.  You can tune this, but in general you want to avoid needing to tune this.

YourKit is something you should use in testing your application to ensure that the application runs cleanly.  It should be done regularly like housework and it help keep down technical debt.  If you do this regularly and you suddenly have a problem it should be really obvious in this tool what is wrong.  If you don't do it regularly you get "noise" building up and it can be fairly hard to diagnose a problem quickly.  If this does happen, the best solution is an extensive tuning session to fix the software by reducing the level of noise.

The CPU cache statistics are interesting but a bit too low level.  Chronicle Queue reads/write sequentially by design and has a near optimal cpu cache profile. e.g. it can produce half the cpu cache to cache traffic of an in cache ring buffer.   If you have a problem with it, it is usually down to something the hardware or OS is doing rather than something you will see in the CPU.  i.e. when you have a problem, its not very subtle, your dirty cache is full and you are seeing multi-milli-seconds delays.

Monitoring the latency distribution is valuable, however coordinated omission is harder in Chronicle Queue due to the fact we make very little use of flow control.  The TCP replication has flow control as it uses TCP, but that is about it.  If you have a slow, or even off line consumer, the producer has no idea and thus doesn't slow down (in fact flow control is not supported)  All messages are asynchronous.  If you construct a synchronous request/reply protocol on top of Queue, then you can get coordinated omission, but this should be apparent to you.,

Repeatable processes are really essential in diagnosing subtle bugs.

Chronicle tends to use lots of busy waiting tasks.  This means you need to dedicate CPUs to those threads and the number of these can add up.  If you can fit all your critical threads onto your system (with a few to spare) with hyper threading, this may help.  However, it is more important to have the right number of threads running all the time than disabling HT. (in general I leave it on)

Swappiness - I would avoid the system needing to swap.  Chronicle Queue encourages you to put the bulk of your data into the queue and thus results in paging.  The OS tends to page more aggressively than it swaps. If you tune the paging (mostly by making sure you have the right hardware) you should be fine.

For IRQ balancing you want to turn it off our move as many interrupts away from critical threads as possible.  The only exception might be the thread which is handling your network adapter.  I haven't experimented enough to say whether it is a good thing to have it on the same thread as the one processing its data or not. (though it sounds like it might be)

I suggest using the affinity library to bind to isolated CPUs.  You can do this with taskset, but java doesn't make this easy.  You want to avoid binding more than one thread to a CPU.  This is usually worse than doing nothing.

I would set the -Xmx32G and reduce it if you feel this is too much.  I would set the Young generation size to be large e.g. -Xmn24G and play with the survivor ration with -XX:SurvivorRatio=8 depending on how full the survivor space gets.  Ideally your Eden never fills during the day and you can set the ration to 100.  You can set the -Xms to be the same as the -Xmn but this means you really will use this much memory rather than the JVM work it out.

Ideally you shouldn't be GCing except very rarely.  If you do this how you tune the GC and even which GC you use doesn't matter so much.  You might find the G1 collector is worth trying.  It depends on what your profile looks like.  If you only GC once a day, the (default) parallel collector will be fastest.

- Peter


# Questions and Answers

#### Question
I got this login info. I don’t know what it means!
 18:59:45.860 INFO  net.openhft.chronicle.map.VanillaChronicleMap [1668] - Thread took 100ms to release the lock, (Was there a GC?)
#### Answer
The locking mechanism detects when you take a long time to get a lock. This means either;
-  you had a GC which paused the jvm. If you add to the command line -verbose:gc you will see if this is the case
- you are writing at a high rate which has resulted in some blocking paging/IO operation. You can monitor this in Unix with atop. Look for heavy write activity. You can tune the kernel to reduce the impact.
- you are performing a long operation while holding a lock (Unlikely in your case)

#### Question
can I configure my queue to always write to disk if desired?
#### Answer
Yes

#### Question
Are the objects stored in the queue always written to disk or is there some kind of configuration that keeps objects in memory first and when required stores to disk?
#### Answer
By default, Queue writes synchronously to the OS and lets the OS write the data when it can. You can change this so that it will not return from finish() unless the OS says it has written the data to disk.

#### Question
Is the memory used by the JVM not managed by itself but by the OS (through the use of memory mapped file)?
#### Answer
The memory is managed by the OS so there is nothing for the JVM to do as such.  This memory doesn't use any heap (except a few objects to manage the native memory)

#### Question
Does it mean if the JVM crash, what was not yet written to disk is not lost? And will be written to disk eventually?
#### Answer
After finish() returns, and the JVM crashes, the data will still be written to disk. If the JVM crashes in the middle of writing a message, the message is truncated. If the OS crashes before data has been written, it can be lost. If this is a concern you can use replication to mitigate this.

#### Question
If I start the application again, will it be able to resume reading and writing to the same queue without loss of message?
#### Answer
If the OS hasn't died, you will be able to continue reading and writing from where you were up date.

#### Question
To be able to continue reading after a crash, do I must know the index where I was?
#### Answer
To continue after a restart you need to know the index to read from. This can mean storing the index in another file like Chronicle Map or marking the entries as you process them e.g. with a time stamp or a flag.

#### Question
Data stored in the queue is not removed after it has been read? Should I take care of it if I want to remove things? Is there some configurable way of doing that automatically like keeping N messages or keeping for a given time? OR Should it be done outside of the JVM?
#### Answer
The Queue is a persistent Queue where old entries are never removed. The assumption is that they will be archived or deleted as whole files when you no longer need them. It has to be done outside the JVM at the moment. The assumption is that disk space is cheap. If you are not writing at a high rate you might find you use only a few GB per day and you can delete the files after they rotate at the end of the day. You can also do hourly rotation if you need (or faster)

---
