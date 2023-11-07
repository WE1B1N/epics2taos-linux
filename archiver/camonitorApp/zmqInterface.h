#pragma once
#include <zmq.h>

#include "arraybuffer.h"
#include "tool_lib.h"

#define INPROC "inproc://s3archiver"

typedef struct{
    char            pvname[40];
    uint16_t        array_count;
    //void*		    pvname[40]; 
    //long            array_count;  //存储数组的个数
} ARCHIVE_MESSAGE;

typedef union {
    float float_hdfdata;
    double double_hdfdata;
    char char_hdfdata;
    int int_hdfdata;
    long long_hdfdata;
} Hdfdata;

void* zmqPublisherInitial();
int sendArchiverCmd(void* publisher, ARCHIVE_MESSAGE *message);



