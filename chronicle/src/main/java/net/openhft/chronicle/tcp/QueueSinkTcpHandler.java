package net.openhft.chronicle.tcp;

import net.openhft.chronicle.MappingFunction;
import net.openhft.chronicle.network.SessionDetailsProvider;
import net.openhft.chronicle.network.TcpHandler;
import net.openhft.lang.io.Bytes;

/**
 * Created by lear on 16/07/2015.
 */
public class QueueSinkTcpHandler implements TcpHandler {

    private boolean subscriptionRequired;

    private long subscribedIndex;

    private MappingFunction mappingFunction;

    private AppenderAdapter adapter;

    @Override
    public void process(Bytes in, Bytes out, SessionDetailsProvider sessionDetailsProvider) {
        processIncoming(in);
        processOutgoing(out);
    }

    private void processIncoming(Bytes in) {
        if (in.remaining() >= ChronicleTcp.HEADER_SIZE) {
            int size = in.readInt();
            long receivedIndex = in.readLong();

            switch (size) {
                case ChronicleTcp.IN_SYNC_LEN:
                    // heartbeat
                    return;
                case ChronicleTcp.PADDED_LEN:
                    // write padded entry
                    adapter.writePaddedEntry();
                    processIncoming(in);
                    return;
                case ChronicleTcp.SYNC_IDX_LEN:
                    //Sync IDX message, re-try
                    processIncoming(in);
                    return;
            }


        }
    }

    private void nextExcerpt(Bytes in) {

    }

    private void processOutgoing(Bytes out) {
        if (subscriptionRequired) {
            out.writeLong(ChronicleTcp.ACTION_SUBSCRIBE);
            out.writeLong(subscribedIndex);

            if (mappingFunction != null) {
                // write with mapping and len
                out.writeLong(ChronicleTcp.ACTION_WITH_MAPPING);
                long pos = out.position();

                out.skip(4);
                long start = out.position();

                out.writeObject(mappingFunction);
                out.writeInt(pos, (int) (out.position() - start));
            }

            subscriptionRequired = false;
        }
    }

    @Override
    public void onEndOfConnection() {

    }

    public void subscribeTo(long index, MappingFunction mappingFunction) {
        this.subscribedIndex = index;
        this.subscriptionRequired = true;
        this.mappingFunction = mappingFunction;
    }

    public void setAdapter(AppenderAdapter adapter) {
        this.adapter = adapter;
    }
}
