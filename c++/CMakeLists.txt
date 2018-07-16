cmake_minimum_required(VERSION 3.10)
project(BatchAppenderNative)


# add extra include directories
include_directories(/System/Library/Frameworks/JavaVM.framework/Headers)


set(CMAKE_CXX_STANDARD 11)

add_library(BatchAppenderNative BatchAppenderNative.cpp net_openhft_chronicle_queue_batch_BatchAppenderNative.h)