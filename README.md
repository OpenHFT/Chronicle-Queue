*We can help you get Chronicle up and running in your organisation, we suggest you invite us in for consultancy, charged on an ad-hoc basis, we can discuss the best options tailored to your individual requirements. - [Contact Us](sales@higherfrequencytrading.com)*

*Or you may already be using Chronicle and just want some help - [find out more..](http://openhft.net/support/)*

# Chronicle Queue

Inter Process Communication ( IPC ) with sub millisecond latency and able to store every message:
![Chronicle](http://openhft.net/wp-content/uploads/2014/07/ChronicleQueue_200px.png)



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
    * [Stateful Client](https://github.com/OpenHFT/Chronicle-Queue#stateful-client)
    * [Stateless Client](https://github.com/OpenHFT/Chronicle-Queue#stateless-client)
    * [Appender Client](https://github.com/OpenHFT/Chronicle-Queue#appender-client)
* [Support](https://github.com/OpenHFT/Chronicle-Queue#support)
* [JavaDoc](http://openhft.github.io/Chronicle-Queue/apidocs/)

## Overview
Chronicle is a Java project focused on building a persisted low latency messaging framework for high performance and critical applications. 

![](http://openhft.net/wp-content/uploads/2014/07/Chronicle-diagram_005.jpg)

In first glance it can be seen as **yet another queue implementation** but it has major design choices that should be emphasized. 

Using non-heap storage options(RandomAccessFile) Chronicle provides a processing environment where applications does not suffer from GarbageCollection. While implementing high performance and memory-intensive applications ( you heard the fancy term "bigdata"?) in Java; one of the biggest problem is GarbageCollection. GarbageCollection (GC) may slow down your critical operations non-deterministically at any time.. In order to avoid non-determinism and escape from GC delays off-heap memory solutions are addressed. The main idea is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management interface over off-heap memory so you can build your own solutions over it.

Chronicle uses RandomAccessFiles while managing memory and this choice brings lots of possibility. Random access files permit non-sequential, or random, access to a file's contents. To access a file randomly, you open the file, seek a particular location, and read from or write to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can access any random index "directly" using pointers. File portions can be used as ByteBuffers if the portion is mapped into memory. 

This memory mapped file is also used for exceptionally fast interprocess communication (IPC) without affecting your system performance. There is no Garbage Collection (GC) as everything is done off heap. 

![](http://openhft.net/wp-content/uploads/2014/07/Screen-Shot-2014-09-30-at-11.24.53.png)

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

Current version of Chronicle (V3) contains IndexedChronicle and VanillaChronicle implementations. 

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
String basePath = Syste.getProperty("java.io.tmpdir") + "/getting-started"
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

![](http://openhft.net/wp-content/uploads/2014/07/Screen-Shot-2015-01-16-at-15.06.49.png)

### Stateful Client
### Stateless Client
### Appender Client


##  Support
* [Chronicle Wiki](https://github.com/OpenHFT/Chronicle-Queue/wiki)
* [Chronicle support on StackOverflow](http://stackoverflow.com/tags/chronicle/info)
* [Chronicle support on Google Groups](https://groups.google.com/forum/?hl=en-GB#!forum/java-chronicle)
* [Development Tasks - JIRA] (https://higherfrequencytrading.atlassian.net/browse/CHRON)
