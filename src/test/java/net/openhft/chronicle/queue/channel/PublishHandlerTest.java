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

    //Sapient generated method id: ${590cd13f-424c-34cb-95b0-2654fa9768cf}, hash: C01C07207BFB5E217EC02BA11521A6F2
    @Test()
    void publishTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        String result = target.publish();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    //Sapient generated method id: ${125fc076-78de-38a4-953e-6a261fd08c0c}, hash: F4B9EC14656F94B400B40E5EA82CDE4F
    @Test()
    void publish1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.publish("publish1");
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${65afb4f3-4b94-34a1-9621-6c7f9580bb34}, hash: 618C98420B01AFC7E671285A6B786961
    @Test()
    void syncModeTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        SyncMode result = target.syncMode();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, is(nullValue())));
    }

    //Sapient generated method id: ${cca940fa-019f-3145-835a-8b85259845ed}, hash: E8F6167CAE2D8B90632A43FF16D7AFA4
    @Test()
    void syncMode1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.syncMode(SyncMode.NONE);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${cb27ab1e-0b67-347f-8320-3e7d949d1c3d}, hash: B229D5D2B895EEA7A583F1FCCC81310C
    @Test()
    void publishSourceIdTest() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        int result = target.publishSourceId();
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(0)));
    }

    //Sapient generated method id: ${d34fb813-ac8a-3459-bdbf-11648429438c}, hash: 402F2B629C0C17DA11CB20FB1E23285D
    @Test()
    void publishSourceId1Test() {
        //Arrange Statement(s)
        PublishHandler target = new PublishHandler();
        //Act Statement(s)
        PublishHandler result = target.publishSourceId(0);
        //Assert statement(s)
        assertAll("result", () -> assertThat(result, equalTo(target)));
    }

    //Sapient generated method id: ${5ee56fb1-1efb-35fb-b1bb-d35d0790b8a8}, hash: 46D1C15FEC40B9EAD904DC6B2CEEF42B
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

    //Sapient generated method id: ${729cdc87-0956-3895-bd0a-826db7b5b8f4}, hash: 4DA80895F1C55BFFF94A20D9E92801E6
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
