#include "zmqInterface.h"
#include "hdf5Interface.h"

extern HASH_TABLE* pvtable;

void* zmqPublisherInitial()
{
    void *context = zmq_ctx_new();
    void *publisher = zmq_socket(context, ZMQ_PUSH);
    zmq_bind(publisher, INPROC);
    return publisher;
}

int sendArchiverCmd(void* publisher, ARCHIVE_MESSAGE *message)
{
    
    zmq_send(publisher, message, sizeof(ARCHIVE_MESSAGE), 0);
    //zmq_send(publisher, "zmq send test buffer.", 21, 0);
    //printf("send archive message, pvname:%s, array_count:%d\n", message.pvname, message.array_count);
    
}