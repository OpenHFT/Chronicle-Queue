package net.openhft.chronicle.queue.channel;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.affinity.AffinityLock;
import org.mockito.stubbing.Answer;
import net.openhft.chronicle.wire.channel.ChronicleChannel;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.channel.ChronicleChannelCfg;
import net.openhft.chronicle.wire.channel.ChronicleContext;
import org.mockito.MockedStatic;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.TimingPauser;

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
class PublishHandlerTest {

    private final AffinityLock affinityLockMock = mock(AffinityLock.class);

    private final ChronicleChannel chronicleChannelMock = mock(ChronicleChannel.class);

    private final ChronicleQueue chronicleQueueMock = mock(ChronicleQueue.class);

    private final ChronicleContext contextMock = mock(ChronicleContext.class);

    @Test()
    void publishTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        String result = target.publish();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    @Test()
    void publish1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.publish("publish1");
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    @Test()
    void syncModeTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        SyncMode result = target.syncMode();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    @Test()
    void syncMode1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.syncMode(SyncMode.NONE);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    @Test()
    void publishSourceIdTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        int result = target.publishSourceId();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(0)));
    }

    @Test()
    void publishSourceId1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.publishSourceId(0);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    @Test()
    void runWhenDefaultBranch() throws Throwable {
        /* Branches:
         * (branch expression (line 99)) : false
         */
        //Arrange Statement(s)
        try (MockedStatic<PublishHandler> publishHandler = mockStatic(PublishHandler.class);
             MockedStatic<PipeHandler> pipeHandler = mockStatic(PipeHandler.class)) {
            doReturn(affinityLockMock).when(contextMock).affinityLock();
            doNothing().when(affinityLockMock).close();
            pipeHandler.when(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock);
            publishHandler.when(() -> PublishHandler.copyFromChannelToQueue(eq(chronicleChannelMock), (TimingPauser) any(), eq(chronicleQueueMock), eq((SyncMode) null))).thenAnswer((Answer<Void>) invocation -> null);
            PublishHandler target = new PublishHandler();
            //Act Statement(s)
            target.run(contextMock, chronicleChannelMock);
            //Assert statement(s)
            assertAll("result", () -> {
                verify(contextMock).affinityLock();
                verify(affinityLockMock).close();
                pipeHandler.verify(() -> PipeHandler.newQueue(contextMock, (String) null, (SyncMode) null, 0), atLeast(1));
                publishHandler.verify(() -> PublishHandler.copyFromChannelToQueue(eq(chronicleChannelMock), (TimingPauser) any(), eq(chronicleQueueMock), eq((SyncMode) null)));
            });
        }
    }

    @Test()
    void asInternalChannelTest() {
        //Arrange Statement(s)
        ChronicleContext chronicleContextMock = mock(ChronicleContext.class);
        ChronicleChannelCfg chronicleChannelCfgMock = mock(ChronicleChannelCfg.class);
        try (MockedStatic<PipeHandler> pipeHandler = mockStatic(PipeHandler.class)) {
            pipeHandler.when(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0)).thenReturn(chronicleQueueMock);
            PublishHandler target = new PublishHandler();
            //Act Statement(s)
            ChronicleChannel result = target.asInternalChannel(chronicleContextMock, chronicleChannelCfgMock);
            //Assert statement(s)
            //TODO: Please implement equals method in PublishQueueChannel for verification of the entire object or you need to adjust respective assertion statements
            assertAll("result", () -> {
                assertThat(result, is(notNullValue()));
                pipeHandler.verify(() -> PipeHandler.newQueue(chronicleContextMock, (String) null, (SyncMode) null, 0), atLeast(1));
            });
        }
    }
}
