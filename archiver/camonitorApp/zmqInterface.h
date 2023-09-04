#include <zmq.h>


#define INPROC "inproc://s3archiver"

typedef struct{
    void*		    pvname[40]; 
    long            array_count;  //存储数组的个数
} ARCHIVE_MESSAGE;

void* zmqPublisherInitial();
int sendArchiverCmd(void* publisher, ARCHIVE_MESSAGE message);


