package net.openhft.chronicle.queue.channel;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;

import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.channel.EventPoller;
import net.openhft.chronicle.wire.channel.ChronicleContext;

import java.util.function.Consumer;

import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Predicate;

import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.bytes.StreamingDataInput;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.channel.InternalChronicleChannel;
import org.mockito.MockedStatic;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;

import static org.mockito.Mockito.doNothing;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.doThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mockStatic;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SubscribeHandlerTest {

    private final ChronicleChannel chronicleChannelMock = mock(ChronicleChannel.class);

    private final ChronicleContext chronicleContextMock = mock(ChronicleContext.class);

    private final ChronicleQueue chronicleQueueMock = mock(ChronicleQueue.class);

    private final Consumer consumerMock = mock(Consumer.class);

    private final DocumentContext documentContextMock = mock(DocumentContext.class);

    private final ExcerptTailer excerptTailerMock = mock(ExcerptTailer.class);

    private final InternalChronicleChannel iccMock = mock(InternalChronicleChannel.class);

    private final Pauser pauserMock = mock(Pauser.class);

    private final Predicate predicateMock = mock(Predicate.class);


    private final ExcerptTailer tailerMock = mock(ExcerptTailer.class);


    //Sapient generated method id: ${c5486346-60b0-3623-a477-689db1e0d5d7}, hash: 0D2C967D8B1153928479403FAFCC8ECB
    @Test()
    void queueTailerWhenDefaultBranchThrowsIllegalArgumentException() {
        /* Branches:
         * (branch expression (line -1)) : true  #  inside $$$reportNull$$$0 method
         */
        //Arrange Statement(s)
        Pauser pauser = null;
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Argument for @NotNull parameter 'pauser' of net/openhft/chronicle/queue/channel/SubscribeHandler.queueTailer must not be null");
        //Act Statement(s)
        final IllegalArgumentException result = assertThrows(IllegalArgumentException.class, () -> {
            SubscribeHandler.queueTailer(pauser, chronicleChannelMock, chronicleQueueMock, predicateMock, consumerMock);
        });
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, is(notNullValue()));
            assertThat(result.getMessage(), equalTo(illegalArgumentException.getMessage()));
        });
    }

    //Sapient generated method id: ${37934c8d-19b2-3b9d-9f68-b07e8aedbca5}, hash: 0579A8794C3FAECD51B6D3BE6EB5408D
    @Test()
    void queueTailerWhenDefaultBranch2ThrowsIllegalArgumentException() {
        /* Branches:
         * (branch expression (line -1)) : true  #  inside $$$reportNull$$$0 method
         */
        //Arrange Statement(s)
        ChronicleChannel chronicleChannel = null;
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Argument for @NotNull parameter 'channel' of net/openhft/chronicle/queue/channel/SubscribeHandler.queueTailer must not be null");
        //Act Statement(s)
        final IllegalArgumentException result = assertThrows(IllegalArgumentException.class, () -> {
            SubscribeHandler.queueTailer(pauserMock, chronicleChannel, chronicleQueueMock, predicateMock, consumerMock);
        });
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, is(notNullValue()));
            assertThat(result.getMessage(), equalTo(illegalArgumentException.getMessage()));
        });
    }

    //Sapient generated method id: ${699688e5-934b-33fa-849a-0b66116d94a6}, hash: DE53E4A3D7C15D8FBBC979565805FECF
    @Test()
    void queueTailerWhenDefaultBranch3ThrowsIllegalArgumentException() {
        /* Branches:
         * (branch expression (line -1)) : true  #  inside $$$reportNull$$$0 method
         */
        //Arrange Statement(s)
        ChronicleQueue chronicleQueue = null;
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Argument for @NotNull parameter 'subscribeQueue' of net/openhft/chronicle/queue/channel/SubscribeHandler.queueTailer must not be null");
        //Act Statement(s)
        final IllegalArgumentException result = assertThrows(IllegalArgumentException.class, () -> {
            SubscribeHandler.queueTailer(pauserMock, chronicleChannelMock, chronicleQueue, predicateMock, consumerMock);
        });
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, is(notNullValue()));
            assertThat(result.getMessage(), equalTo(illegalArgumentException.getMessage()));
        });
    }

    //Sapient generated method id: ${b52a97cb-bc0b-3665-afe2-0b6d7e1c145a}, hash: D5FA2A371E10D61535A2CFF90D52C68F
    @Test()
    void queueTailerWhenDefaultBranch4ThrowsIllegalArgumentException() {
        /* Branches:
         * (branch expression (line -1)) : true  #  inside $$$reportNull$$$0 method
         */
        //Arrange Statement(s)
        Consumer<ExcerptTailer> consumer = null;
        IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Argument for @NotNull parameter 'subscriptionIndexController' of net/openhft/chronicle/queue/channel/SubscribeHandler.queueTailer must not be null");
        //Act Statement(s)
        final IllegalArgumentException result = assertThrows(IllegalArgumentException.class, () -> {
            SubscribeHandler.queueTailer(pauserMock, chronicleChannelMock, chronicleQueueMock, predicateMock, consumer);
        });
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, is(notNullValue()));
            assertThat(result.getMessage(), equalTo(illegalArgumentException.getMessage()));
        });
    }

    //Sapient generated method id: ${5b859eaa-dc9e-391e-b985-4fa9286b7ec5}, hash: CE13DF174340A9BE6E9D42C0A4E3220A
    @Test()
    void copyOneMessageWhenDefaultBranch() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : true
         * (branch expression (line 96)) : false
         */
        //Arrange Statement(s)
        doReturn(documentContextMock).when(tailerMock).readingDocument();
        doReturn(false).when(documentContextMock).isPresent();
        doNothing().when(documentContextMock).close();
        //Act Statement(s)
        boolean result = SubscribeHandler.copyOneMessage(chronicleChannelMock, tailerMock, predicateMock);
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, equalTo(Boolean.FALSE));
            verify(tailerMock).readingDocument();
            verify(documentContextMock).isPresent();
            verify(documentContextMock).close();
        });
    }

    //Sapient generated method id: ${c73567e2-8ebe-3d4c-83bd-9ea761b7f7d1}, hash: 9D0E5FC0AC5C3DBD1BAF1D4A514E1937
    @Test()
    void copyOneMessageWhenDcIsMetaDataAndDefaultBranch() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : false
         * (dc.isMetaData()) : true
         * (branch expression (line 96)) : false
         */
        //Arrange Statement(s)
        doReturn(documentContextMock).when(tailerMock).readingDocument();
        doReturn(true).when(documentContextMock).isPresent();
        doReturn(true).when(documentContextMock).isMetaData();
        doNothing().when(documentContextMock).close();
        //Act Statement(s)
        boolean result = SubscribeHandler.copyOneMessage(chronicleChannelMock, tailerMock, predicateMock);
        //Assert statement(s)
        assertAll("result", () -> {
            assertThat(result, equalTo(Boolean.TRUE));
            verify(tailerMock).readingDocument();
            verify(documentContextMock).isPresent();
            verify(documentContextMock).isMetaData();
            verify(documentContextMock).close();
        });
    }

    //Sapient generated method id: ${57adfd7a-69ba-3dc2-a1b2-a5a3cddaa7ca}, hash: 74A3801EFCE3D55B6A6CC547E323B2BD
    @Test()
    void subscribeTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        String result = target.subscribe();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    //Sapient generated method id: ${33172789-0f24-347a-9f2b-b66d691af2c5}, hash: 3FA335BD7C6BD5FC203AC4863DB7F361
    @Test()
    void subscribe1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.subscribe("subscribe1");
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${65afb4f3-4b94-34a1-9621-6c7f9580bb34}, hash: A1E573272572E75BE7822FC3FCF3C496
    @Test()
    void syncModeTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SyncMode result = target.syncMode();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    //Sapient generated method id: ${cca940fa-019f-3145-835a-8b85259845ed}, hash: 984730B27A190F9DF38C5BD67694F938
    @Test()
    void syncMode1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.syncMode(SyncMode.NONE);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${9d9f579f-32e3-3150-94ca-d16da44c4882}, hash: 2E4D08E9EA26528684064DC4A7CA82E7
    @Test()
    void filterTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        Predicate<Wire> result = target.filter();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    //Sapient generated method id: ${74c35f30-6267-38c5-91c2-62a2069cdd65}, hash: 1A68F1A472F2644035447B3508935141
    @Test()
    void filter1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.filter(predicateMock);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${5ee56fb1-1efb-35fb-b1bb-d35d0790b8a8}, hash: B554C3FCBBBAC7C3A38C5F17D2DDF27E
    @Test()
    void runWhenDefaultBranch() throws Throwable, Exception {
        /* Branches:
         * (icc.supportsEventPoller()) : true
         * (branch expression (line 144)) : false
         */
        //Arrange Statement(s)
        try (MockedStatic<PipeHandler> pipeHandler = mockStatic(PipeHandler.class)) {
            doReturn(true).when(iccMock).supportsEventPoller();
            doReturn(chronicleChannelMock).when(iccMock).eventPoller((EventPoller) any());
            pipeHandler.when(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock);
            doReturn(excerptTailerMock).when(chronicleQueueMock).createTailer();
            doNothing().when(chronicleQueueMock).close();
            SubscribeHandler target = new SubscribeHandler();
            //Act Statement(s)
            target.run(chronicleContextMock, iccMock);
            //Assert statement(s)
            assertAll("result", () -> {
                verify(iccMock).supportsEventPoller();
                verify(iccMock).eventPoller((EventPoller) any());
                pipeHandler.verify(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0), atLeast(1));
                verify(chronicleQueueMock).createTailer();
                verify(chronicleQueueMock).close();
            });
        }
    }

    //Sapient generated method id: ${d9093b93-bb96-3e61-96a1-18faaa8245fe}, hash: 9E73BB5FED3C6C9AFD44747DE7626140
    @Test()
    void runWhenDefaultBranchAndDefaultBranch() throws Throwable, Exception {
        /* Branches:
         * (icc.supportsEventPoller()) : true
         * (branch expression (line 139)) : true
         * (branch expression (line 144)) : false
         *
         * TODO: Help needed! Please adjust the input/test parameter values manually to satisfy the requirements of the given test scenario.
         *  The test code, including the assertion statements, has been successfully generated.
         */
        //Arrange Statement(s)
        try (MockedStatic<PipeHandler> pipeHandler = mockStatic(PipeHandler.class)) {
            doReturn(true).when(iccMock).supportsEventPoller();
            doReturn(chronicleChannelMock).when(iccMock).eventPoller((EventPoller) any());
            pipeHandler.when(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock);
            doReturn(excerptTailerMock).when(chronicleQueueMock).createTailer();
            doNothing().when(chronicleQueueMock).close();
            SubscribeHandler target = new SubscribeHandler();
            //Act Statement(s)
            target.run(chronicleContextMock, iccMock);
            //Assert statement(s)
            assertAll("result", () -> {
                verify(iccMock).supportsEventPoller();
                verify(iccMock).eventPoller((EventPoller) any());
                pipeHandler.verify(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0), atLeast(1));
                verify(chronicleQueueMock).createTailer();
                verify(chronicleQueueMock).close();
            });
        }
    }

    //Sapient generated method id: ${b08fa3d1-fd26-32b5-b51b-e2a50bdfd787}, hash: 64A2F556BAE71ADFD269BDE15827E712
    @Test()
    void runWhenDefaultBranchThrowsThrowable() {
        //TODO: Please change the modifier of the below class from private to public to isolate the test case scenario.
        //Act Statement(s)
        //Assert statement(s)
        //doReturn(affinityLockMock).when(contextMock).affinityLock();
        //doNothing().when(affinityLockMock).close();
        //doReturn(false).when(iccMock).supportsEventPoller();
        //doNothing().when(chronicleQueueMock).close();
        //pipeHandler.when(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock).thenReturn(chronicleQueueMock2);
        //subscribeHandler.when(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any())).thenAnswer((Answer<Void>) invocation -> null);
        //SubscribeHandler target = new SubscribeHandler();
        //final Throwable result = assertThrows(Throwable.class, () -> {    target.run(contextMock, iccMock);});
        //assertAll("result", () -> {    assertThat(result, is(notNullValue()));    verify(contextMock).affinityLock();    verify(affinityLockMock).close();    verify(iccMock).supportsEventPoller();    pipeHandler.verify(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0), atLeast(2));    verify(chronicleQueueMock).close();    subscribeHandler.verify(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any()));});
    }

    //Sapient generated method id: ${b4d632fd-3234-3d8c-aff3-34a15f698799}, hash: F2C30732945AC3E3BCFED4DC20C724BD
    @Test()
    void runWhenIccNotSupportsEventPollerAndDefaultBranchAndDefaultBranch() throws Throwable, Exception {
        //TODO: Please change the modifier of the below class from private to public to isolate the test case scenario.
        //Act Statement(s)
        //Assert statement(s)
        //doReturn(affinityLockMock).when(contextMock).affinityLock();
        //doNothing().when(affinityLockMock).close();
        //doReturn(false).when(iccMock).supportsEventPoller();
        //doNothing().when(chronicleQueueMock).close();
        //pipeHandler.when(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock).thenReturn(chronicleQueueMock2);
        //subscribeHandler.when(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any())).thenAnswer((Answer<Void>) invocation -> null);
        //SubscribeHandler target = new SubscribeHandler();
        //target.run(contextMock, iccMock);
        //assertAll("result", () -> {    verify(contextMock).affinityLock();    verify(affinityLockMock).close();    verify(iccMock).supportsEventPoller();    pipeHandler.verify(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0), atLeast(2));    verify(chronicleQueueMock).close();    subscribeHandler.verify(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any()));});
    }

    //Sapient generated method id: ${f2cf8476-da18-3413-bf90-f19ffddfcc46}, hash: DCA992B27632138AC3138E9CE3A0B79B
    @Test()
    void runWhenDefaultBranchAndDefaultBranchAndDefaultBranch() throws Throwable, Exception {
        //TODO: Please change the modifier of the below class from private to public to isolate the test case scenario.
        //Act Statement(s)
        //Assert statement(s)
        //doReturn(affinityLockMock).when(contextMock).affinityLock();
        //doNothing().when(affinityLockMock).close();
        //doReturn(false).when(iccMock).supportsEventPoller();
        //doNothing().when(chronicleQueueMock).close();
        //pipeHandler.when(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock).thenReturn(chronicleQueueMock2);
        //subscribeHandler.when(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any())).thenAnswer((Answer<Void>) invocation -> null);
        //SubscribeHandler target = new SubscribeHandler();
        //target.run(contextMock, iccMock);
        //assertAll("result", () -> {    verify(contextMock).affinityLock();    verify(affinityLockMock).close();    verify(iccMock).supportsEventPoller();    pipeHandler.verify(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0), atLeast(2));    verify(chronicleQueueMock).close();    subscribeHandler.verify(() -> SubscribeHandler.queueTailer((Predicate) any(), eq(iccMock), eq(chronicleQueueMock2), eq((Predicate) null), (SubscribeHandler.NoOp) any()));});
    }

    //Sapient generated method id: ${4f159a32-779d-341d-bc2a-35cd95be2b5a}, hash: BF7F16F9EE72F6CAFD734BC88895E021
    @Test()
    void closeWhenRunEndsTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        boolean result = target.closeWhenRunEnds();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(Boolean.TRUE)));
    }

    //Sapient generated method id: ${729cdc87-0956-3895-bd0a-826db7b5b8f4}, hash: A608E1F70E458A974636A8D7E38A825C
    @Test()
    void asInternalChannelTest() {
        //Arrange Statement(s)
        ChronicleChannelCfg chronicleChannelCfgMock = mock(ChronicleChannelCfg.class);
        try (MockedStatic<PipeHandler> pipeHandler = mockStatic(PipeHandler.class)) {
            pipeHandler.when(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock);
            SubscribeHandler target = new SubscribeHandler();
            //Act Statement(s)
            ChronicleChannel result = target.asInternalChannel(chronicleContextMock, chronicleChannelCfgMock);
            //Assert statement(s)
            //TODO: Please implement equals method in SubscribeQueueChannel for verification of the entire object or you need to adjust respective assertion statements
            assertAll("result", () -> {
                assertThat(result, is(notNullValue()));
                pipeHandler.verify(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0), atLeast(1));
            });
        }
    }

    //Sapient generated method id: ${b1843744-2d18-32d8-8bb1-952ac01b568f}, hash: 583BD9C428750261890D218BA8DB7E7E
    @Test()
    void subscribeSourceIdTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.subscribeSourceId(0);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${1ad1ceaf-b614-3638-bdee-232fdce18611}, hash: 6F3FEA21446F712B1678E13F94BA172E
    @Test()
    void subscriptionIndexControllerTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.subscriptionIndexController(consumerMock);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }
}
