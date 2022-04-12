/*
 * Copyright 2015 Higher Frequency Trading
 *
 * http://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.queue;

public class MappedMemoryTest {

    private static final long SHIFT = 26L;
    private static long BLOCK_SIZE = 1L << SHIFT;

    /*
    @Test
    public void withMappedNativeBytesTest() throws IOException, InterruptedException {

        for (int t = 0; t < 10; t++) {
            File tempFile = File.createTempFile("chronicle", "q");
            try {

                final MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 8);
                final MappedNativeBytes bytes = new MappedNativeBytes(mappedFile, true);
                bytes.writeLong(1, 1);
                long startTime = System.nanoTime();
                for (long i = 0; i < BLOCK_SIZE; i += 8) {
                    bytes.writeLong(i);
                }

               // System.out.println("With MappedNativeBytes,\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of longs written=" + BLOCK_SIZE / 8));
                mappedFile.close();
            } finally {
                tempFile.delete();
            }
            Jvm.pause(200);
        }
    }

    @Test
    public void withRawNativeBytesTess() throws IOException, InterruptedException {

        for (int t = 0; t < 10; t++) {
            File tempFile = File.createTempFile("chronicle", "q");
            try {

                MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 8);
                Bytes<?> bytes1 = mappedFile.acquire(1).bytes();

                long startTime = System.nanoTime();
                for (long i = 0; i < BLOCK_SIZE; i += 8L) {
                    bytes1.writeLong(i);
                }

               // System.out.println("With NativeBytes,\t\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of longs written=" + BLOCK_SIZE / 8));
                mappedFile.close();
            } finally {
                tempFile.delete();
            }
            Jvm.pause(200);
        }
    }

    @Ignore
    @Test
    public void testShowComparablePerformanceOfBytes() throws IOException {

        for (int x = 0; x < 5; x++) {
           // System.out.println("\n\niteration " + x);
            File tempFile = File.createTempFile("chronicle", "q");
            try {

                final MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 8);
                final MappedNativeBytes bytes = new MappedNativeBytes(mappedFile, true);
                bytes.writeLong(1, 1);
                long startTime = System.nanoTime();
                for (long i = 0; i < BLOCK_SIZE; i++) {
                    bytes.writeByte('X');
                }

               // System.out.println("With MappedNativeBytes,\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of bytes written= 1L << " + SHIFT + " = " + BLOCK_SIZE));
            } finally {
                tempFile.delete();
            }

            File tempFile2 = File.createTempFile("chronicle", "q");
            try {

                MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 8);
                Bytes<?> bytes1 = mappedFile.acquire(1).bytes();

                long startTime = System.nanoTime();
                for (long i = 0; i < BLOCK_SIZE; i++) {
                    bytes1.writeByte('X');
                }

               // System.out.println("With NativeBytes,\t\t time=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + ("ms, number of bytes written= 1L << " + SHIFT + " = " + BLOCK_SIZE));
            } finally {
                tempFile2.delete();
            }
           // System.out.println("");
        }
    }

    @Test
    public void mappedMemoryTest() throws IOException {

        File tempFile = File.createTempFile("chronicle", "q");
        try {

            final MappedFile mappedFile = new MappedFile(tempFile.getName(), BLOCK_SIZE, 8);
            final MappedNativeBytes bytes = new MappedNativeBytes(mappedFile, true);
            bytes.writeUTF("hello this is some very long text");

            bytes.clear();

            bytes.position(100);
            bytes.writeUTF("hello this is some more long text...................");

            bytes.position(100);
           // System.out.println("result=" + bytes.readUTF());
        } finally {
            tempFile.delete();
        }
    }

    /**
     * ensure a IllegalStateException is throw if the block size is not a power of 2
     *
     * @throws IOException
     *//*
    @Test(expected = IllegalStateException.class)
    public void checkBlockSizeIsPowerOfTwoTest() throws IOException {
        File tempFile = File.createTempFile("chronicle", "q");
        MappedFile mappedFile = new MappedFile(tempFile.getName(), 10, 0);
        new ChronicleUnsafe(mappedFile);
    }

    */
}

