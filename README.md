*We can help you get Chronicle up and running in your organisation, we suggest you invite us in for consultancy, charged on an ad-hoc basis, we can discuss the best options tailored to your individual requirements. - [Contact Us](sales@higherfrequencytrading.com)*

*Or you may already be using Chronicle and just want some help - [find out more..](http://openhft.net/support/)*

# Chronicle Queue

Inter Process Communication ( IPC ) with sub millisecond latency and able to store every message:
![Chronicle](http://openhft.net/wp-content/uploads/2014/07/ChronicleQueue_200px.png)



It is available on maven central as

 ```xml
<dependency>
  <groupId>net.openhft</groupId>
  <artifactId>chronicle</artifactId>
  <version><!--replace with the latest version--></version>
</dependency>
```
Click here to get the [Latest Version Number](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.openhft%22%20AND%20a%3A%22chronicle%22) 


#### Contents
* [Overview](https://github.com/OpenHFT/Chronicle-Queue#overview)
* [Building Blocks](https://github.com/OpenHFT/Chronicle-Queue#building-blocks)
* [Implementations](https://github.com/OpenHFT/Chronicle-Queue#implementations)
  * [Indexed Chronicle](https://github.com/OpenHFT/Chronicle-Queue#indexed-chronicle)
  * [Vanilla Chronicle](https://github.com/OpenHFT/Chronicle-Queue#vanilla-chronicle)
* [JavaDoc](http://openhft.github.io/Chronicle-Queue/apidocs/)

### Overview
Chronicle is a Java project focused on building a persisted low latency messaging framework for high performance and critical applications. 

![](http://openhft.net/wp-content/uploads/2014/07/Chronicle-diagram_005.jpg)

In first glance it can be seen as **yet another queue implementation** but it has major design choices that should be emphasized. 

Using non-heap storage options(RandomAccessFile) Chronicle provides a processing environment where applications does not suffer from GarbageCollection. While implementing high performance and memory-intensive applications ( you heard the fancy term "bigdata"?) in Java; one of the biggest problem is GarbageCollection. GarbageCollection (GC) may slow down your critical operations non-deterministically at any time.. In order to avoid non-determinism and escape from GC delays off-heap memory solutions are addressed. The main idea is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management interface over off-heap memory so you can build your own solutions over it.

Chronicle uses RandomAccessFiles while managing memory and this choice brings lots of possibility. Random access files permit non-sequential, or random, access to a file's contents. To access a file randomly, you open the file, seek a particular location, and read from or write to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can access any random index "directly" using pointers. File portions can be used as ByteBuffers if the portion is mapped into memory. 

### Building Blocks
Chronicle is the main interface for management and can be seen as the Collection class of Chronicle environment. You will reserve a portion of memory and then put/fetch/update records using Chronicle interface. 

Chronicle has three main concepts:
  * Tailer (sequential reads)
  * Excerpt (random reads)
  * Appender (sequential writes).

An Excerpt is the main data container in a Chronicle, each Chronicle is composed of Excerpts. Putting data to a chronicle means starting a new Excerpt, writing data into it and finishing Excerpt at the end. 
A Tailer is an Excerpt optimized for sequential reads.
An Appender is something like Iterator in Chronicle environment. You add data appending the current chronicle. 

### Implementations
Current version of Chronicle contains IndexedChronicle and VanillaChronicle implementations. 

## IndexedChronicle 
IndexedChronicle is a single writer multiple reader Chronicle. 

For each record, IndexedChronicle holds the memory-offset in another index cache for random access. This means IndexedChronicle "knows" where the 3rd object resides in memory this is why it named as "Indexed". But this index is just sequential index, first object has index 0, second object has index 1... Indices are not strictly sequential so if there is not enough space in the current chunk, a new chunk is created and the left over space is a padding record with its own index which the Tailer skips.

## VanillaChronicle 
VanillaChronicle is a multiple writer multiple reader Chronicle



###  Support

See frequently asked questions on [Stackoverflow](http://stackoverflow.com/tags/chronicle/info), or you can post your question on the forum at [Java Chronicle support group](https://groups.google.com/forum/?hl=en-GB#!forum/java-chronicle)



If you want to access objects with other logical keys ( i.e via some value of object ) you have to manage your own mapping from logical key to index. 

---

[Chronicle Wiki](https://github.com/OpenHFT/Chronicle-Queue/wiki)

[Chronicle support on Stackoverflow](http://stackoverflow.com/tags/chronicle/info)

[Forum for those learning about OpenHFT](https://groups.google.com/forum/?hl=en-GB#!forum/open-hft)

[Development Tasks - JIRA] (https://higherfrequencytrading.atlassian.net/browse/CHRON)
