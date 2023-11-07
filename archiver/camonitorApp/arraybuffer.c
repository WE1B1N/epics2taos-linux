#include <stdio.h>
#include "arraybuffer.h"


void *publisher;

unsigned long hash(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash%10000;
}

//int main(int argc, char const *argv[])
//{
  //  unsigned long hashnum;
 //   hashnum=hash("dsvjk vkp29-2840jng");
   // printf("hash number is %lu\n",hashnum);
   // return 0;
//}

linklist_error arraybuffer_initial(HASH_TABLE * pvtable, void * zmqpublisher)
{
    pvtable->currentsize =0;
    size_t i;
    for (i = 0; i < HASH_TABLE_LENGTH; i++)
    {
        pvtable->hashtable[i] = NULL;
    }
    pvtable->bufferLock =epicsMutexMustCreate();
    pvtable->zmq_publisher = zmqpublisher;//initial zmq publisher socket
}

linklist_error writearray(HASH_TABLE *pvtable, char *pvname,ARRAY_DATA data)
{   
    
    unsigned long hashnum;
    hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *elist = pvtable->hashtable[hashnum];
    

    DATA_ELEMENT * newdataelement;
    if (elist != NULL)
    {
        //遍历链表HASH_TABLE_ELEMENT，寻找是否有pvname对应的hash table element，找到了将flag改为true
        HASH_TABLE_ELEMENT *plist = elist;
        bool flag_pvname_matched = false;
        while (plist != NULL)
        {
            if (strcmp(plist->pvname, pvname)==0)                   //找到对应的PV名，新建数据元素并加入链表
            {
                newdataelement = (DATA_ELEMENT*)callocMustSucceed(1, sizeof(DATA_ELEMENT), "malloc for data");  
                newdataelement->data = data;    
                newdataelement->next = NULL;   
                //处理链表新节点 
                if (plist->pdata_head == NULL ^ plist->pdata_head == NULL)
                {
                    printf("linklist error!\n");
                    return LINKLIST_ERROR;               
                }
                if (plist->pdata_tail ==NULL)
                {
                    newdataelement->previous = NULL; 
                    plist->pdata_tail = newdataelement;  
                    plist->pdata_head = newdataelement;        
                }  
                else
                {
                    plist->pdata_tail->next =  newdataelement;
                    newdataelement->previous = plist->pdata_tail;
                    plist->pdata_tail = newdataelement;  
                }
                flag_pvname_matched = true;
                plist->currentsize = plist->currentsize + 1;          //-----------记录数据数量-------------20230902
                //---------------在数组数量达到阈值时，给编码线程发消息----------------------------------
                //int size_thre = 2;

                /*
                if(plist->currentsize >= 2) {
                    //----------------------------------------------------------
                    
                    printf("pvname:%s, plistcurrentsize:%d, sflag:%d\n", pvname, plist->currentsize, elist->sflag);
                    if(elist->sflag) {
                        ARCHIVE_MESSAGE message;
                        strcpy(&(message.pvname), pvname);
                        message.array_count = plist->currentsize;
                        //message.array_count = size_thre;
                        //printf("pvname:%s, plistcurrentsize:%d\n", pvname, message.array_count);
                        //zmq_send(pvtable->zmq_publisher, "zmq send test buffer", 20, 0);
                        sendArchiverCmd(pvtable->zmq_publisher, &message);
                        elist->sflag = 0;
                        
                    }
                    
                    //----------------------------------------------------------
                }
                */
                
            }     
            plist = plist->next;
        } 
        //如果flag仍为false，说明遍历hash table element没有找到当前pvname对应的element，hash table还未存有相应的pv，新建该pv的hash table element
        if (!flag_pvname_matched)
        {
            plist->next = (HASH_TABLE_ELEMENT*)callocMustSucceed(1, sizeof(HASH_TABLE_ELEMENT), "malloc for HASH_TABLE_ELEMENT");   
            plist = plist->next;
            plist->next =NULL;
            strcpy(&(plist->pvname),pvname);
            newdataelement = (DATA_ELEMENT*)callocMustSucceed(1, sizeof(DATA_ELEMENT), "malloc for data");
            newdataelement->previous = NULL;
            newdataelement->next = NULL;
            newdataelement->data = data;     
            plist->currentsize = 1;                                  //----------初始记录数量-------------20230902            
            plist->pdata_head = newdataelement;  
            plist->pdata_tail = newdataelement;
        }    
        return LINKLIST_OK;    
    }
    else
    {
        elist = (HASH_TABLE_ELEMENT*)callocMustSucceed(1, sizeof(HASH_TABLE_ELEMENT), "malloc for HASH_TABLE_ELEMENT"); 
        elist->next =NULL;
        strcpy(&(elist->pvname),pvname);
        newdataelement = (DATA_ELEMENT*)callocMustSucceed(1, sizeof(DATA_ELEMENT), "malloc for data");   
        newdataelement->previous = NULL;
        newdataelement->next = NULL;
        newdataelement->data = data;   
               
        elist->pdata_head = newdataelement;  
        elist->pdata_tail = newdataelement;
        elist->currentsize =1;                             //初始化--否则会丢掉第一个元素
        elist->sflag = 1;
        pvtable->hashtable[hashnum] = elist;               //写入哈希表元素
             //---------------------------测试用-------------------------------
                 //   ARCHIVE_MESSAGE message;
                  //  strcpy(&(message.pvname), pvname);
                  //  message.array_count = elist->currentsize;
                    //zmq_send(pvtable->zmq_publisher, "zmq send test buffer", 20, 0);
                   // sendArchiverCmd(pvtable->zmq_publisher, &message);
            //----------------------------------------------------------
    }    
}

ARRAY_DATA readarray(HASH_TABLE *pvtable,char * pvname)
{
    ARRAY_DATA rst;
    unsigned long hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *elist = pvtable->hashtable[hashnum];
    DATA_ELEMENT *mem_tobe_free;

    if (elist == NULL)                   //哈希表中没有对应的pv
    {
        rst.count=0;
        rst.type=0;
        rst.pdata=NULL;
        return rst;
    }
    else
    {
        HASH_TABLE_ELEMENT *plist = elist;
        bool flag_pvname_matched = false;
        while (plist != NULL)
        {
            if (strcmp(plist->pvname, pvname)==0)                   //找到对应的PV名
            {
              if (plist->pdata_head == NULL ^ plist->pdata_head == NULL)
              {
                printf("linklist error!\n");
                rst.count=0;
                rst.type=0;
                rst.pdata=NULL;
                return rst;           
              }
              if (plist->pdata_head!=NULL)                          //有新数据
              {
                mem_tobe_free = plist->pdata_head;                  //需要释放DATA_ELEMENT结构体
                rst = plist->pdata_head->data;               
                plist->pdata_head =  plist->pdata_head->next;
                if (plist->pdata_head !=NULL)
                {
                    plist->pdata_head->previous = NULL;  
                }
                else
                {
                    plist->pdata_tail = NULL;
                }
                free(mem_tobe_free);
                plist->currentsize = plist->currentsize - 1;       //----------2023.9.6--------成功读取数据后将size-1
                return rst;   
              } 
              else
              {
                rst.count=0;
                rst.type=0;
                rst.pdata=NULL;
                return rst;
              }
            }     
            plist = plist->next;
        } 

        if (!flag_pvname_matched)               //哈希表第二层中没有对应的PV
        {
            rst.count=0;
            rst.pdata=NULL;
            rst.type=0;
            return rst;
        } 
    }

}


ARRAY_DATA readarray_ts(HASH_TABLE *pvtable,char * pvname)         //线程安全的读取
{
    ARRAY_DATA data;
    epicsMutexMustLock(pvtable->bufferLock);
    data= readarray(pvtable,pvname);
    epicsMutexUnlock(pvtable->bufferLock);
    //-----------------test--------------------------------------
    unsigned long hashnum;
    hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *elist = pvtable->hashtable[hashnum];
    //printf("RRRRRpvname:%s, plistcurrentsize:%d, sflag:%d\n", pvname, elist->currentsize, elist->sflag);
    //-----------------test--------------------------------------
    //printf("read pdata name:%s ptr:%p\n",pvname,  data.pdata);//
    return data;
}

linklist_error writearray_ts(HASH_TABLE *pvtable, char * pvname, ARRAY_DATA data)       //线程安全写入
{
    epicsMutexMustLock(pvtable->bufferLock);
    writearray(pvtable, pvname, data);
    
    //printf("write pdata name:%s ptr:%p\n",pvname, data.pdata);//
    
    unsigned long hashnum;
    hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *elist = pvtable->hashtable[hashnum];
    if(elist->currentsize >= 1000) {
        //----------------------------------------------------------     
        //printf("WWWWWpvname:%s, plistcurrentsize:%d, sflag:%d\n", pvname, elist->currentsize, elist->sflag);
        if(elist->sflag) {
            ARCHIVE_MESSAGE message;
            strcpy(&(message.pvname), pvname);
            message.array_count = elist->currentsize;     
            sendArchiverCmd(pvtable->zmq_publisher, &message);
            elist->sflag = 0;         
        }
    }
    
    epicsMutexUnlock(pvtable->bufferLock);
    
}


linklist_error writearray_epics(HASH_TABLE *pvtable, struct event_handler_args eha)
{
    ARRAY_DATA data;
    linklist_error rst;
    data.count = eha.count;
    data.type = eha.type;
    void * pvalue = dbr2parray(eha.dbr,eha.type);
    data.timestamp = dbr2ts(eha.dbr,eha.type);
    data.pdata = (void*) callocMustSucceed(eha.count, dbr_value_size[eha.type],"malloc epics arraybuffer");
    memcpy(data.pdata, pvalue, dbr_value_size[eha.type]*eha.count);
    rst=writearray_ts(pvtable,ca_name(eha.chid),data);
    return rst;
}

int get_sflag(HASH_TABLE *pvtable, char * pvname) {
    HASH_TABLE_ELEMENT* elist= getHashTableElement(pvtable, pvname);
    return elist->sflag;    
}

void set_sflag(HASH_TABLE *pvtable, char * pvname, int sflag) {
    HASH_TABLE_ELEMENT* elist= getHashTableElement(pvtable, pvname);
    elist->sflag = sflag;    
}

HASH_TABLE_ELEMENT* getHashTableElement(HASH_TABLE *pvtable, char * pvname)
{
    unsigned long hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *plist = pvtable->hashtable[hashnum];
    if(plist==NULL)
    {             //哈希表中无此单元       
        return NULL;
    }
    else{
        bool flag_pvname_matched = false;
        while (plist != NULL)
        {
            if (strcmp(plist->pvname, pvname)==0)                   //找到对应的PV名
            {
                return plist;   
            }      
            plist = plist->next;
        } 
        if (!flag_pvname_matched)               //哈希表第二层中没有对应的PV
        {
            return NULL;
        } 
    }
}

HASH_TABLE_ELEMENT* addHashTableElement(HASH_TABLE *pvtable, char * pvname)
{
    unsigned long hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *plist = pvtable->hashtable[hashnum];
    if (plist==NULL)
    {
        pvtable->hashtable[hashnum]= (HASH_TABLE_ELEMENT*)callocMustSucceed(1, sizeof(HASH_TABLE_ELEMENT), "malloc for HASH_TABLE_ELEMENT"); 
        return pvtable->hashtable[hashnum];
    }
    else{
        bool rflag=true;
        while (rflag)
        {
            if (strcmp(plist->pvname, pvname)==0)                   //找到对应的PV名,同名的PV已经存在
            {
                return NULL;                                       
            }
            else{   
                if (plist->next==NULL)
                {
                    plist->next=(HASH_TABLE_ELEMENT*)callocMustSucceed(1, sizeof(HASH_TABLE_ELEMENT), "malloc for HASH_TABLE_ELEMENT");
                    rflag=false;     
                }
                plist = plist->next;
            }
        } 
        return plist;
    }
}


linklist_error writearray_elist(HASH_TABLE_ELEMENT* plist, ARRAY_DATA data)
{
    DATA_ELEMENT* newdata=(DATA_ELEMENT*)callocMustSucceed(1, sizeof(DATA_ELEMENT), "malloc for DATA_ELEMENT");
    newdata->data=data;
    newdata->next=NULL;

    DATA_ELEMENT* pdata=plist->pdata_head;
    if (pdata == NULL){
        plist->pdata_head == newdata;
    }
    else{
        while (pdata !=NULL)
        {
            if (pdata->next ==NULL)
            {
                pdata->next = newdata;
            }
            else{
                pdata=pdata->next;
            }  
        }
        

    }

}