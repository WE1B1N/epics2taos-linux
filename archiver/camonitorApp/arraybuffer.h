#pragma once
#include <taos.h> 
#include <stdio.h>
#include <epicsStdlib.h>
#include <string.h>
#include <epicsMutex.h> 
#include <epicsThread.h>
#include <cadef.h>
#include <epicsTime.h>
#include <epicsGetopt.h>
#include <tool_lib.h>
#include <syslog.h>
#include <cantProceed.h>
#include "zmqInterface.h"

#define HASH_TABLE_LENGTH 100000

typedef struct array_data
{
    long                type;                    // 数组单元的数据类型
    long                count;                   // 数组的长度
    epicsTimeStamp      timestamp;
    void*               *pdata;                  //数组指针
}ARRAY_DATA;

typedef struct data_element{
    ARRAY_DATA          data;     // 数组的指针
    struct data_element        *previous;
    struct data_element        *next;                   // 链表
}DATA_ELEMENT;

typedef struct hash_table_element{
    char		         pvname[40];                 // pv名
    uint16_t             currentsize;                //缓存的数组数量
    DATA_ELEMENT         *pdata_head;             // 数组缓存链表头
    DATA_ELEMENT         *pdata_tail;             // 数组缓存链表尾
    struct hash_table_element   *next;            //当发生哈希碰撞时，用链表保存相同哈希值的元素
}HASH_TABLE_ELEMENT;


typedef struct hash_tabe{
    long            maxsize;   /* type of pv */ 
    long            currentsize;                      //当前缓存总大小
    epicsMutexId bufferLock;                          //读写锁
    HASH_TABLE_ELEMENT  *hashtable[HASH_TABLE_LENGTH];         /* value of the pv */
    void* zmq_publisher;                //ZMQ句柄                **********增加*****************
}HASH_TABLE;


#define LINKLIST_OK 1
#define LINKLIST_ERROR 0
#define LINKLIST_HEAD 2
#define LINKLIST_TAIL 3
#define MAX_ARRAYBUFFER_LENGTH 10000000000

typedef int16_t linklist_error;

unsigned long hash(unsigned char *str);
linklist_error arraybuffer_initial(HASH_TABLE * pvtable);
ARRAY_DATA readarray(HASH_TABLE *pvtable,char * pvname);            //pbuff，指向数据指针的指针
linklist_error writearray(HASH_TABLE *pvtable, char * pvname, ARRAY_DATA data);

ARRAY_DATA readarray_ts(HASH_TABLE *pvtable,char * pvname);            //zero-copy read    用完之后，一定要释放pdata指针
linklist_error writearray_ts(HASH_TABLE *pvtable, char * pvname, ARRAY_DATA data);   //zero-copy write
linklist_error writearray_epics(HASH_TABLE *pvtable, struct event_handler_args eha);   