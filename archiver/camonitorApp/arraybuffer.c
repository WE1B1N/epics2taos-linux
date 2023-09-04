#include <stdio.h>
#include "arraybuffer.h"


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

linklist_error arraybuffer_initial(HASH_TABLE * pvtable)
{
    pvtable->currentsize =0;
    size_t i;
    for (i = 0; i < HASH_TABLE_LENGTH; i++)
    {
        pvtable->hashtable[i] = NULL;
    }
    pvtable->bufferLock =epicsMutexMustCreate();
}

linklist_error writearray(HASH_TABLE *pvtable, char *pvname,ARRAY_DATA data)
{
    unsigned long hashnum;
    hashnum = hash(pvname);
    HASH_TABLE_ELEMENT *elist = pvtable->hashtable[hashnum];
    

    DATA_ELEMENT * newdataelement;
    if (elist != NULL)
    {
        //遍历链表HASH_TABLE_ELEMENT
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
                plist->arraycount = plist->arraycount + 1;          //-----------记录数据数量-------------20230902
            }     
            plist = plist->next;
        } 
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
            plist->arraycount = 1;                                  //----------初始记录数量-------------20230902            
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
        pvtable->hashtable[hashnum] = elist;               //写入哈希表元素
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
    return data;
}

linklist_error writearray_ts(HASH_TABLE *pvtable, char * pvname, ARRAY_DATA data)       //线程安全写入
{
    epicsMutexMustLock(pvtable->bufferLock);
    writearray(pvtable, pvname, data);
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