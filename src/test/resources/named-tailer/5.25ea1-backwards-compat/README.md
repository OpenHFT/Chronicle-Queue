Queue files that were generated with v 5.25ea1 of Chronicle Queue. 3 named tailers were setup and advanced to the third index. This data is used to verify the backwards compatibility of named tailer versioning introduced after this release.

The code used to generate the queue data is:

```java
public class QueueFileCreator {
    public static void main(String[] args) {
        try (SingleChronicleQueue queue = SingleChronicleQueueBuilder.builder().path("target/backwards").build();
             ExcerptAppender appender = queue.createAppender();
             ExcerptTailer tailerOne = queue.createTailer("replicated:tailerOne");
             ExcerptTailer tailerTwo = queue.createTailer("replicated:tailerTwo");
             ExcerptTailer tailerThree = queue.createTailer("replicated:tailerThree")) {

            appender.writeText("1");
            appender.writeText("2");
            appender.writeText("3");
            long index = appender.lastIndexAppended();

            tailerOne.moveToIndex(index);
            tailerTwo.moveToIndex(index);
            tailerThree.moveToIndex(index);
        }
    }
}
```
