#include "BatchAppenderNative.h"

#include <iostream>


JNIEXPORT jlong JNICALL Java_net_openhft_chronicle_queue_batch_BatchAppenderNative_writeMessages
        (JNIEnv *env, jobject obj, jlong rawAddress, jlong rawMaxBytes, jint rawMaxMessages) {

    // hello world in binary wire
    unsigned char bytes[] = {0x0c, 0x00, 0x00, 0x00, 0xeb, 0x68, 0x65, 0x6c,
                             0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

    jlong count = 1;
    jlong length = sizeof(bytes);
    memcpy((void *) rawAddress, (void *) bytes, sizeof(bytes));

    return count << 32 | length;
}