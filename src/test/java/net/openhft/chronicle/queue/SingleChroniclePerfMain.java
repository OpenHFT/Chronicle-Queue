package net.openhft.chronicle.queue;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.values.Array;
import net.openhft.chronicle.values.MaxUtf8Length;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.DocumentContext;
import org.junit.Test;

import java.io.IOException;

import static net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder.single;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleChroniclePerfMain {
    static final int count = 1_000_000;
    static final int size = 4 << 10;
    // blackholes to avoid code elimination.
    static int s32;
    static long s64;
    static float f32;
    static double f64;
    static String s;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
    }

    public static void main(String[] args) throws IOException {
        for (int t = 0; t < 2; t++) {
            doPerfTest(
                    bytes -> writeMany(bytes, size),
                    bytes -> readMany(bytes, size),
                    t == 0 ? 100_000 : count, t > 0);
        }
    }

    static void doPerfTest(TestWriter<Bytes> writer, TestReader<Bytes> reader, int count, boolean print) throws IOException {
        Histogram writeHdr = new Histogram(30, 7);
        Histogram readHdr = new Histogram(30, 7);
        String file = OS.TARGET + "/deleteme-" + System.nanoTime();
        try (ChronicleQueue chronicle = single(file).blockSize(64 << 20).build()) {
            ExcerptAppender appender = chronicle.createAppender();
            UncheckedBytes bytes = new UncheckedBytes(NoBytesStore.NO_BYTES);
            for (int i = 0; i < count; i++) {
                long start = System.nanoTime();
                try (DocumentContext dc = appender.writingDocument()) {
                    Bytes<?> bytes0 = dc.wire().bytes();
                    bytes0.ensureCapacity(size);
                    bytes.setBytes(bytes0);
                    bytes.readPosition(bytes.writePosition());
                    writer.writeTo(bytes);
                    bytes0.writePosition(bytes.writePosition());
                }
                long time = System.nanoTime() - start;
                writeHdr.sample(time);
            }

            ExcerptTailer tailer = chronicle.createTailer();
            for (int i = 0; i < count; i++) {
                long start2 = System.nanoTime();
                try (DocumentContext dc = tailer.readingDocument()) {
                    assertTrue(dc.isPresent());
                    Bytes<?> bytes0 = dc.wire().bytes();
                    bytes.setBytes(bytes0);
                    reader.readFrom(bytes);
                }
                long time2 = System.nanoTime() - start2;
                readHdr.sample(time2);
            }
        }
        if (print) {
            System.out.println("Write latencies " + writeHdr.toMicrosFormat());
            System.out.println("Read latencies " + readHdr.toMicrosFormat());
        }
        IOTools.deleteDirWithFiles(file, 3);
    }

    static void writeMany(Bytes bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            bytes.writeInt(i);// 4 bytes
            bytes.writeFloat(i);// 4 bytes
            bytes.writeLong(i);// 8 bytes
            bytes.writeDouble(i);// 8 bytes
            bytes.writeUtf8("Hello!!"); // 8 bytes
        }
    }

    static void readMany(Bytes bytes, int size) {
        for (int i = 0; i < size; i += 32) {
            s32 = bytes.readInt();// 4 bytes
            f32 = bytes.readFloat();// 4 bytes
            s64 = bytes.readLong();// 8 bytes
            f64 = bytes.readDouble();// 8 bytes
            s = bytes.readUtf8(); // 8 bytes
            assertEquals("Hello!!", s);
        }
    }

    @Test
    public void testFacarde() {
        IFacade f = Values.newNativeReference(IFacade.class);
        Byteable byteable = (Byteable) f;
        long capacity = byteable.maxSize();
        byteable.bytesStore(NativeBytesStore.nativeStore(capacity), 0, capacity);
        System.out.println(f);
    }

    interface TestWriter<T> {
        void writeTo(T t);
    }

    interface TestReader<T> {
        void readFrom(T t);
    }

    //IFacade (at the bottom) is the façade we need tested

    interface IFacadeBase {
        short getValue0();

        void setValue0(short value);

        byte getValue1();

        void setValue1(byte value);

        byte getValue2();

        void setValue2(byte value);

        int getValue3();

        void setValue3(int value);

        short getValue4();

        void setValue4(short value);

        boolean getValue5();

        void setValue5(boolean value);

        boolean getValue6();

        void setValue6(boolean value);

        short getValue7();

        void setValue7(short value);

        short getValue8();

        void setValue8(short value);

        long getValue9();

        void setValue9(long value);

        long getValue10();

        void setValue10(long value);

        long getValue11();

        void setValue11(long value);

        long getValue12();

        void setValue12(long value);

        long getValue13();

        void setValue13(long value);

        long getValue14();

        void setValue14(long value);

        long getValue15();

        void setValue15(long value);

        short getValue16();

        void setValue16(short value);

        short getValue17();

        void setValue17(short value);

        short getValue18();

        void setValue18(short value);
    }

    interface IFacadeSon extends IFacadeBase {
        long getValue19();

        void setValue19(long value);

        int getValue20();

        void setValue20(int value);

        int getValue21();

        void setValue21(int value);

        double getValue22();

        void setValue22(double value);

        String getValue23();

        void setValue23(@MaxUtf8Length(10) String value);

        int getValue24();

        void setValue24(int value);

        double getValue25();

        void setValue25(double value);

        byte getValue26();

        void setValue26(byte value);

        double getValue27();

        void setValue27(double value);

        double getValue28();

        void setValue28(double value);

        double getValue29();

        void setValue29(double value);

        double getValue30();

        void setValue30(double value);

        double getValue31();

        void setValue31(double value);

        double getValue32();

        void setValue32(double value);
    }

    interface IFacadeDaughter extends IFacadeBase {
        long getValue33();

        void setValue33(long value);

        String getValue34();

        void setValue34(@MaxUtf8Length(11) String value);

        int getValue35();

        void setValue35(int value);

        String getValue36();

        void setValue36(@MaxUtf8Length(11) String value);

        int getValue37();

        void setValue37(int value);

        long getValue38();

        void setValue38(long value);

        short getValue39();

        void setValue39(short value);

        long getValue40();

        void setValue40(long value);

        String getValue41();

        void setValue41(@MaxUtf8Length(43) String value);

        long getValue42();

        void setValue42(long value);

        long getValue43();

        void setValue43(long value);

        long getValue44();

        void setValue44(long value);

        long getValue45();

        void setValue45(long value);

        long getValue46();

        void setValue46(long value);

        byte getValue47();

        void setValue47(byte value);

        byte getValue48();

        void setValue48(byte value);

        double getValue49();

        void setValue49(double value);

        double getValue50();

        void setValue50(double value);

        double getValue51();

        void setValue51(double value);

        byte getValue52();

        void setValue52(byte value);

        byte getValue53();

        void setValue53(byte value);

        byte getValue54();

        void setValue54(byte value);

        byte getValue55();

        void setValue55(byte value);

        byte getValue56();

        void setValue56(byte value);

        long getValue57();

        void setValue57(long value);

        byte getValue58();

        void setValue58(byte value);

        double getValue59();

        void setValue59(double value);

        double getValue60();

        void setValue60(double value);

        double getValue61();

        void setValue61(double value);

        double getValue62();

        void setValue62(double value);

        double getValue63();

        void setValue63(double value);

        long getValue64();

        void setValue64(long value);

        long getValue65();

        void setValue65(long value);

        double getValue66();

        void setValue66(double value);

        double getValue67();

        void setValue67(double value);

        short getValue68();

        void setValue68(short value);

        String getValue69();

        void setValue69(@MaxUtf8Length(101) String value);

        String getValue70();

        void setValue70(@MaxUtf8Length(17) String value);

        boolean getValue71();

        void setValue71(boolean value);

        boolean getValue72();

        void setValue72(boolean value);

        String getValue73();

        void setValue73(@MaxUtf8Length(11) String value);

        String getValue74();

        void setValue74(@MaxUtf8Length(9) String value);

        byte getValue75();

        void setValue75(byte value);

        int getValue76();

        void setValue76(int value);

        int getValue77();

        void setValue77(int value);

        String getValue78();

        void setValue78(@MaxUtf8Length(3) String value);

        int getValue79();

        void setValue79(int value);

        double getValue80();

        void setValue80(double value);

        byte getValue81();

        void setValue81(byte value);

        byte getValue82();

        void setValue82(byte value);

        byte getValue83();

        void setValue83(byte value);

        byte getValue84();

        void setValue84(byte value);

        byte getValue85();

        void setValue85(byte value);

        byte getValue86();

        void setValue86(byte value);

        byte getValue87();

        void setValue87(byte value);

        byte getValue88();

        void setValue88(byte value);

        int getValue89();

        void setValue89(int value);

        int getValue90();

        void setValue90(int value);

        int getValue91();

        void setValue91(int value);

        int getValue92();

        void setValue92(int value);

        int getValue93();

        void setValue93(int value);

        int getValue94();

        void setValue94(int value);

        int getValue95();

        void setValue95(int value);

        int getValue96();

        void setValue96(int value);

        int getValue97();

        void setValue97(int value);

        int getValue98();

        void setValue98(int value);

        int getValue99();

        void setValue99(int value);

        int getValue100();

        void setValue100(int value);

        int getValue101();

        void setValue101(int value);

        int getValue102();

        void setValue102(int value);

        int getValue103();

        void setValue103(int value);

        int getValue104();

        void setValue104(int value);

        double getValue105();

        void setValue105(double value);

        double getValue106();

        void setValue106(double value);

        long getValue107();

        void setValue107(long value);

        long getValue108();

        void setValue108(long value);

        long getValue109();

        void setValue109(long value);

        byte getValue110();

        void setValue110(byte value);

        byte getValue111();

        void setValue111(byte value);

        byte getValue112();

        void setValue112(byte value);

        byte getValue113();

        void setValue113(byte value);

        byte getValue114();

        void setValue114(byte value);

        byte getValue115();

        void setValue115(byte value);

        byte getValue116();

        void setValue116(byte value);

        byte getValue117();

        void setValue117(byte value);

        byte getValue118();

        void setValue118(byte value);

        byte getValue119();

        void setValue119(byte value);

        byte getValue120();

        void setValue120(byte value);

        byte getValue121();

        void setValue121(byte value);

        byte getValue122();

        void setValue122(byte value);

        byte getValue123();

        void setValue123(byte value);

        byte getValue124();

        void setValue124(byte value);

        byte getValue125();

        void setValue125(byte value);

        int getValue126();

        void setValue126(int value);

        int getValue127();

        void setValue127(int value);

        int getValue128();

        void setValue128(int value);

        int getValue129();

        void setValue129(int value);

        int getValue130();

        void setValue130(int value);

        int getValue131();

        void setValue131(int value);

        int getValue132();

        void setValue132(int value);

        int getValue133();

        void setValue133(int value);

        int getValue134();

        void setValue134(int value);

        int getValue135();

        void setValue135(int value);

        int getValue136();

        void setValue136(int value);

        int getValue137();

        void setValue137(int value);

        int getValue138();

        void setValue138(int value);

        int getValue139();

        void setValue139(int value);

        int getValue140();

        void setValue140(int value);

        int getValue141();

        void setValue141(int value);

        double getValue142();

        void setValue142(double value);

        double getValue143();

        void setValue143(double value);

        byte getValue144();

        void setValue144(byte value);

        String getValue145();

        void setValue145(@MaxUtf8Length(3) String value);

        short getValue146();

        void setValue146(short value);

        byte getValue147();

        void setValue147(byte value);

        byte getValue148();

        void setValue148(byte value);

        int getValue149();

        void setValue149(int value);

        short getValue150();

        void setValue150(short value);

        boolean getValue151();

        void setValue151(boolean value);

        boolean getValue152();

        void setValue152(boolean value);

        short getValue153();

        void setValue153(short value);

        short getValue154();

        void setValue154(short value);

        long getValue155();

        void setValue155(long value);

        long getValue156();

        void setValue156(long value);

        long getValue157();

        void setValue157(long value);

        long getValue158();

        void setValue158(long value);

        long getValue159();

        void setValue159(long value);

        long getValue160();

        void setValue160(long value);

        long getValue161();

        void setValue161(long value);

        short getValue162();

        void setValue162(short value);

        short getValue163();

        void setValue163(short value);

        short getValue164();

        void setValue164(short value);

    }

    interface IFacade extends IFacadeBase {
        @Array(length = 3)
        void setDaughterAt(int idx, IFacadeDaughter daughter);

        IFacadeDaughter getDaughterAt(int idx);

        @Array(length = 3)
        void setSonAt(int idx, IFacadeSon son);

        IFacadeSon getSonAt(int idx);
    }
}

