## How Chronicle Works
Chronicle is a Java project focused on building a persisted low latency messaging framework for high performance and critical applications. 

## What is Different?
In first glance it can be seen as **yet another queue implementation** but it has major design choices that should be emphasized. 

Using non-heap storage options(RandomAccessFile) Chronicle provides a processing environment where applications does not suffer from GarbageCollection. While implementing high performance and memory-intensive applications ( you heard the fancy term "bigdata"?) in Java; one of the biggest problem is GarbageCollection. GarbageCollection (GC) may slow down your critical operations non-deterministically at any time.. In order to avoid non-determinism and escape from GC delays off-heap memory solutions are addressed. The main idea is to manage your memory manually so does not suffer from GC. Chronicle behaves like a management interface over off-heap memory so you can build your own solutions over it.

Chronicle uses RandomAccessFiles while managing memory and this choice brings lots of possibility. Random access files permit non-sequential, or random, access to a file's contents. To access a file randomly, you open the file, seek a particular location, and read from or write to that file. RandomAccessFiles can be seen as "large" C-type byte arrays that you can access any random index "directly" using pointers. File portions can be used as ByteBuffers if the portion is mapped into memory. 

( TODO : What is the effect of page faults when we have a huge Chronicle and not enough RAM ? ) 

## Building Blocks
Chronicle has three main concepts; Chronicle, Excerpt and Appender. 
( TODO: Tailer ?) 

Chronicle is the main interface for management and can be seen as the Collection class of Chronicle environment. You will reserve a portion of memory and then put/fetch/update records using Chronicle interface. Current version of Chronicle contains IndexedChronicle implementation. IndexedChronicle is a single writer multiple(?) reader Chronicle that you can put huge numbers of objects having different sizes. For each record, IndexedChronicle holds the memory-offset in another index cache for random access. This means IndexedChronicle "knows" where the 3rd object resides in memory this is why it named as "Indexed". But this index is just sequential index, first object has index 0, second object has index 1... If you want to access objects with other logical keys ( i.e via some value of object ) you have to manage your own mapping from logical key to index. 

( TODO: In examples TLongLongMap is used for key mapping. What if we use a performant b+tree implementation like [BTreeMap](https://github.com/jankotek/MapDB/blob/master/src/main/java/org/mapdb/BTreeMap.java) ) 

Excerpt is the main data container in a Chronicle, each Chronicle is composed of Excerpts. Putting data to a chronicle means starting a new Excerpt, writing data into it and finishing Excerpt at the end. 

Appender is something like Iterator in Chronicle environment. You add data appending the current chronicle. 

## How it Really Works
Lets see Chronicle in action with an example. In this example we simply will: Create a Chronicle, Put a record to chronicle and Read the record from chronicle.


	package net.openhft.chronicle.examples;
	import java.io.IOException;
	import net.openhft.chronicle.ChronicleConfig;
    import net.openhft.chronicle.Excerpt;
    import net.openhft.chronicle.ExcerptAppender;
    import net.openhft.chronicle.IndexedChronicle;
    import net.openhft.chronicle.tools.ChronicleTools;

    public class ExampleCacheMain {
        public static void main(String... ignored) throws IOException {
            
            String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";
            ChronicleTools.deleteOnExit(basePath);
            
            ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
            IndexedChronicle chronicle = new IndexedChronicle(basePath, config);
    
            ExcerptAppender appender = chronicle.createAppender();
            Excerpt reader = chronicle.createExcerpt();
    
            appender.startExcerpt(100);
            appender.writeObject("TestMessage");
            appender.position(100);
            appender.finish();
            
            reader.index(0);
            Object ret = reader.readObject();
            reader.finish();
    
            System.out.println(ret);
            
        }
    
    }


Create a chronicle giving Java_temp_directory/SimpleChronicle as the base folder. 
	String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";
	ChronicleTools.deleteOnExit(basePath);
            
	ChronicleConfig config = ChronicleConfig.DEFAULT.clone();
	IndexedChronicle chronicle = new IndexedChronicle(basePath, config);

IndexedChronicle creates two RandomAccessFile one for indexes and one for data having names relatively: 

	Java_temp_directory/SimpleChronicle.index
    Java_temp_directory/SimpleChronicle.data

Create appender and reader

	ExcerptAppender appender = chronicle.createAppender();
	Excerpt reader = chronicle.createExcerpt();

NativeExcerptAppender.startExcerpt method does some checks and calculates startAddr and limitAddr(startAddr+100) for this excerpt

	appender.startExcerpt(100);

writeObject method copies the contents of the object int excerpt

	appender.writeObject("TestMessage");

we set appender position to startAddr+100 whatever the objectsize is. 

	appender.position(100);

in finish method object offset is written to index cache. This method acts like a commit, without writing this offset to cache you put data to datacache but not persist it. 

	appender.finish();

In order to read data from data cache, you first need to get physical start address of the data from index cache. Reader.index(0) method does the calculation for you. You read the data and finish reading operation.

	reader.index(0);
	Object ret = reader.readObject();
    reader.finish();

End of simple put/get example. 


