#include "zmqInterface.h"

void* zmqPublisherInitial()
{
    void *context = zmq_ctx_new();
    void *publisher = zmq_socket(context, ZMQ_PUSH);
    zmq_bind(publisher, INPROC);
    return publisher;
}

int sendArchiverCmd(void* publisher, ARCHIVE_MESSAGE message)
{
    zmq_send(publisher, &message, sizeof(ARCHIVE_MESSAGE), 0);
}


void *zmqSubscriber_thread(void *arg) {
    void *context = zmq_ctx_new();
    void *subscriber = zmq_socket(context, ZMQ_PULL);
    // 连接到发布者
    zmq_connect(subscriber, "inproc://example");


    // 接收消息
    ARCHIVE_MESSAGE buffer;
    zmq_recv(subscriber, &buffer, sizeof( ARCHIVE_MESSAGE), 0);
     

}