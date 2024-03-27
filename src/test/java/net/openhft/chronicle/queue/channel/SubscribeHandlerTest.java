package net.openhft.chronicle.queue.channel;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.channel.EventPoller;

import java.util.function.Predicate;

import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import net.openhft.chronicle.wire.channel.InternalChronicleChannel;
import org.mockito.MockedStatic;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.util.function.Consumer;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.bytes.SyncMode;
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

    private final Predicate predicateMock = mock(Predicate.class);

    private final ExcerptTailer tailerMock = mock(ExcerptTailer.class);

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

    @Test()
    void copyOneMessageWhenDcNotIsPresentAndDefaultBranch() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : true
         * (branch expression (line 96)) : false
         *
         * TODO: Help needed! Please adjust the input/test parameter values manually to satisfy the requirements of the given test scenario.
         *  The test code, including the assertion statements, has been successfully generated.
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

    @Test()
    void copyOneMessageWhenDcIsMetaData() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : false
         * (branch expression (line 96)) : true
         * (dc.isMetaData()) : true
         *
         * TODO: Help needed! Please adjust the input/test parameter values manually to satisfy the requirements of the given test scenario.
         *  The test code, including the assertion statements, has been successfully generated.
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

    @Test()
    void copyOneMessageWhenDcNotIsMetaDataAndFilterIsNotNullAndFilterTestWire1AndDataBufferedLessThan3210AndDefaultBranch() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : false
         * (branch expression (line 96)) : true
         * (dc.isMetaData()) : false
         * (filter != null) : true
         * (!filter.test(wire1)) : false
         * (dataBuffered < 32 << 10) : true
         * (branch expression (line 95)) : false
         *
         * TODO: Help needed! Please adjust the input/test parameter values manually to satisfy the requirements of the given test scenario.
         *  The test code, including the assertion statements, has been successfully generated.
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

    @Test()
    void copyOneMessageWhenDcNotIsMetaDataAndFilterIsNotNullAndFilterTestWire1AndDataBufferedNotLessThan3210AndDefaultBranch() throws Throwable {
        /* Branches:
         * (!dc.isPresent()) : false
         * (branch expression (line 96)) : true
         * (dc.isMetaData()) : false
         * (filter != null) : true
         * (!filter.test(wire1)) : false
         * (dataBuffered < 32 << 10) : false
         * (branch expression (line 95)) : false
         *
         * TODO: Help needed! Please adjust the input/test parameter values manually to satisfy the requirements of the given test scenario.
         *  The test code, including the assertion statements, has been successfully generated.
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

    @Test()
    void subscribeTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        String result = target.subscribe();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    @Test()
    void subscribe1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.subscribe("subscribe1");
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    @Test()
    void syncModeTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SyncMode result = target.syncMode();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    @Test()
    void syncMode1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.syncMode(SyncMode.NONE);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    @Test()
    void filterTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        Predicate<Wire> result = target.filter();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    @Test()
    void filter1Test() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.filter(predicateMock);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

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

    @Test()
    void runWhenIccSupportsEventPollerAndDefaultBranchAndDefaultBranch2() throws Throwable, Exception {
        /* Branches:
         * (icc.supportsEventPoller()) : true
         * (branch expression (line 141)) : true
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

    @Test()
    void closeWhenRunEndsTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        boolean result = target.closeWhenRunEnds();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(Boolean.TRUE)));
    }

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

    @Test()
    void subscribeSourceIdTest() {
        //Arrange Statement(s)
        SubscribeHandler target = new SubscribeHandler();
        //Act Statement(s)
        SubscribeHandler result = target.subscribeSourceId(0);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

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
