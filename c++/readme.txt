== About the C++ API

The C++ API is only available for Chronicle-Queue, it's there to facilitate C++ processes that
wish to write into the off heap memory that is being controlled by the java chronicle queue. This assumes that there is only a single appender, It allows a C++ process to batch up numerous writes ( where you handle your own serialisation ) to off heap memory, and it's only when that memory block is full that the java process will then index the queue and provide the next block. The latency for the C++ write is entirely down to how quickly your C++ process can write to this memory block. The size of this memory block is configurable but it can be a number of MB’s large.
