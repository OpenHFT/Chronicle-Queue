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

package simple;

public class WriterMain {
    static final int CHUNK_BITS = 26;
/*
    public static void main(String[] args) throws IOException {
        int dataSize = 256 << 20;
        int length = 50;
        byte[] bytes = new byte[length];
        for (int t = 0; t < 5; t++) {
            long start = System.nanoTime();
            File tmpFile = new File("deleteme" + Time.uniqueId() + ".q"); //File.createTempFile("deleteme", "q");
            tmpFile.deleteOnExit();
            MappedFile file = new MappedFile(tmpFile.getName(), 1 << CHUNK_BITS, 1 << CHUNK_BITS);
            FileWriter fw = new FileWriter(file);
            int count = 0;
            for (int i = 0; i < dataSize - 128; i += length + 4) {
                fw.writeData(bytes);
                count++;
            }
            file.close();
            tmpFile.delete();
            long time = System.nanoTime() - start;
           // System.out.printf("%,d in %.3f secs, throughput %.6f M/s x %,d%n", count, time / 1e9, count * 1e3 / time, length);
        }
    }*/
/*
    static class FileWriter {
        static final int NOT_COMPLETE = 0x80000000;
        final MappedFile file;
        final LongValue readOffset = DataValueClasses.newDirectReference(LongValue.class);
        long writeOffset;
        private int index = -1;
        private MappedMemory mm;
        private Bytes<?> bytes;

        FileWriter(MappedFile file) throws IOException {
            this.file = file;
            mm = file.acquire(0);
            bytes = mm.bytes();
            ((Byteable) readOffset).bytes(bytes, 0L);
            readOffset.compareAndSwapValue(0, 64);
            writeOffset = readOffset.getVolatileValue();
        }

        public void writeData(byte[] byteArray) throws IOException {
            int index2 = (int) (writeOffset >> CHUNK_BITS);
            if (index2 != index) {
                mm = file.acquire(index2);
                bytes = mm.bytes();
                index = index2;
            }
            int offset2 = (int) (writeOffset & ((1 << CHUNK_BITS) - 1));

            // reserve space
            int length = byteArray.length + 4;
            while (!bytes.compareAndSwapInt(offset2, 0, NOT_COMPLETE | length)) {
                offset2 += bytes.readVolatileInt(offset2);
            }
            bytes.write(offset2 + 4, byteArray);
            bytes.writeOrderedInt(offset2, length);
            writeOffset += length;
        }
    }*/
}
