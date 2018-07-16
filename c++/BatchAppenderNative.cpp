#include "net_openhft_chronicle_queue_batch_BatchAppenderNative.h"

#include <iostream>


JNIEXPORT jlong JNICALL Java_net_openhft_chronicle_queue_batch_BatchAppenderNative_writeMessages
        (JNIEnv *env, jobject obj, jlong rawAddress, jlong rawMaxBytes, jint rawMaxMessages) {
    std::cout << "rawAddress=" << rawAddress;

    return 0;
}