/*caMonitor.c*/

/* This example accepts the name of a file containing a list of pvs to monitor.
 * It prints a message for all ca events: connection, access rights and monitor.
 */
#include <stdio.h>
#include <epicsStdlib.h>
#include <string.h>
#include "epicsVersion.h"

#include <cadef.h>
#include <epicsGetopt.h>
#include "archiver.h"
#include "uploader.h"
#include <epicsTime.h>
#include <epicsString.h>
#include "arraybuffer.h"
#include <pthread.h>


#define VALID_DOUBLE_DIGITS 18  /* Max usable precision for a double */
static unsigned long reqElems = 0;
static unsigned long eventMask = DBE_VALUE | DBE_ALARM;   /* Event mask used */
static int floatAsString = 0;                             /* Flag: fetch floats as string */
static int nConn = 0;                                     /* Number of connected PVs */

ARCHIVER* Archiver; 
HASH_TABLE* pvtable;

static void printChidInfo(chid chid, char *message)
{
    printf("\n%s\n",message);
    printf("pv: %s  type(%d) nelements(%ld) host(%s)",
        ca_name(chid),ca_field_type(chid),ca_element_count(chid),
        ca_host_name(chid));
    printf(" read(%d) write(%d) state(%d)\n",
        ca_read_access(chid),ca_write_access(chid),ca_state(chid));
}


// static void printChidInfo_taos(chid chid, char *message)
// {
//     //get time start
//     time_t rawtime;
//     struct tm *info;
//    	char time_cur[40];
//     TAOS_RES* result;
//    	time( &rawtime );
 	
//    	info = localtime( &rawtime );
 
//    	strftime(time_cur, 80, "%Y-%m-%d %H:%M:%S", info);
   	
//     char sql[256];
//     sprintf(sql, "insert into status.`%s` using status.pv_st tags(0) values (\'%s\', %d, %ld, \'%s\', %d, %d, %d );",ca_name(chid), time_cur, ca_field_type(chid), ca_element_count(chid), ca_host_name(chid), ca_read_access(chid),ca_write_access(chid),ca_state(chid));
//     printf("sql: %s \n ", sql);
//     /*-----------------------
//         将连接状态变化写入TDengine
//     result = taos_query(taos, str);
//     char* errstr = taos_errstr(result);
//    printf("query sql: %s \n query result: %s \n", str, errstr);
//     taos_free_result(result);    
//         数据库连接中断时自动重新连接（在独立的函数中实现。）
//     ------------------------------*/
//     // result = taos_query(Archiver->taos, sql);
//     // char* errstr = taos_errstr(result);
//     // printf("query sql: %s \n query result: %s \n", sql, errstr);
//     // taos_free_result(result);    
//     // free(sql);
// }

static void exceptionCallback(struct exception_handler_args args)
{
    chid        chid = args.chid;
    long        stat = args.stat; /* Channel access status code*/
    const char  *channel;
    static char *noname = "unknown";

    channel = (chid ? ca_name(chid) : noname);
    if(chid) printChidInfo(chid,"exceptionCallback");
    printf("exceptionCallback stat %s channel %s\n",
        ca_message(stat),channel);
}



static void accessRightsCallback(struct access_rights_handler_args args)
{
    chid        chid = args.chid;
    //debug accessrightscallback
    //printChidInfo(chid,"accessRightsCallback");
}



static void eventCallback(struct event_handler_args eha)
{
    
    pv* pv = eha.usr;
    //void *test = eha.dbr;
    //--------------------------------------
    //通过这个pv指针，可以用来存储特定pv通道的统计信息
    //--------------------------------------
    pv->status = eha.status; 
    //printf("pvname:%s, status:%d, count:%d\n", pv->name, eha.status, eha.count);
    if (eha.status == ECA_NORMAL)
    { 
        //pv->dbrType = eha.type;
            //pv->nElems = eha.count;
            //pv->value = (void *) eha.dbr;    /* casting away const */
            //print_time_val_sts(pv, reqElems);
            //printf("display in camonitor: %s\n",  val2str((void*)eha.dbr, eha.type,0));
            //fflush(stdout);
            //pv->value = NULL;archive_arraypv
        //archive_pv(eha); 
        pv->callbackCounts++; 
        if (eha.count > 1)
        {
            //对长度大于1的数组数据，写入hash table
            void * ptest;
            ptest=eha.dbr;

            writearray_epics(pvtable, eha);

            /*
            if(writearray_epics(pvtable, eha)) {
                printf("write array succeed!");
            } else {
                printf("write array error!");
            }
            */
            
            /*
            //pv->callbackCounts2++; 
            //void * pvalue = dbr2parray(eha.dbr,eha.type);
            size_t dbrsize = dbr_size_n(eha.type, eha.count);
            time_t t = time(NULL);
            
            char *pvname = pv->name;
            char *status = dbr2status(eha.dbr, eha.type);
            char *sev = dbr2sev(eha.dbr, eha.type);
            epicsTimeStamp ets = dbr2ts(eha.dbr, eha.type);
            uint64_t taosts = epicsTime2int(ets);
            time_t midnight_ts = get_midnight_ts(epicsTime2UnixTime(ets));
            //printf("midnight time: %ld\n", t1);
            //printf("pvname:%s\nstatus:%s\nseverity:%s\ntimestamps:%lu\n", pvname, status, severity, taosts);

            //s3_upload(Archiver->s3client, eha.dbr, pvname, dbrsize, taosts);
            //void *buff=malloc(dbrsize);
            //memcpy(buff,eha.dbr,dbrsize);
            s3_upload_asyn(Archiver->s3client, eha.dbr, pvname, dbrsize, taosts, midnight_ts);
            PvArray2TD(Archiver->taos, taosts, pvname, eha.type, eha.count, status, sev, midnight_ts);
            */         
        }  else {
            //pv->callbackCounts1++;
            archive_pv(eha);
        } 
    } 
}

/*-----------------------
     caMonitor模版的例子这里也是错的。
     PV上线时才能得到正确的类型信息，把订阅功能放在connectionCallback里可以连接启动时不在线的PV。
     当PV的类型改变后，比如认为修改了IOC后重启。。程序能以正确的类型进行订阅。
------------------------*/

static void connectionCallback(struct connection_handler_args args)
{
    pv *ppv = ( pv * ) ca_puser ( args.chid );
    if (args.op == CA_OP_CONN_UP ) {//连接恢复时
        nConn++;


        if (ppv->onceConnected && ppv->dbfType != ca_field_type(ppv->chid)) {
            /* Data type has changed. Rebuild connection with new type. */
            ca_clear_subscription(ppv->evid);
            ppv->evid = NULL;
        }

        if (!ppv->evid) {
            ppv->onceConnected = 1;
                                /* Set up pv structure */
                                /* Get natural type and array count */
            ppv->dbfType = ca_field_type(ppv->chid);
            ppv->dbrType = dbf_type_to_DBR_TIME(ppv->dbfType); /* Use native type */
            if (dbr_type_is_ENUM(ppv->dbrType))                /* Enums honour -n option */
            {
                if (enumAsNr) ppv->dbrType = DBR_TIME_INT;
                else          ppv->dbrType = DBR_TIME_STRING;
            }
            else if (floatAsString &&
                     (dbr_type_is_FLOAT(ppv->dbrType) || dbr_type_is_DOUBLE(ppv->dbrType)))
            {
                ppv->dbrType = DBR_TIME_STRING;
            }
                                /* Set request count */
            ppv->nElems   = ca_element_count(ppv->chid);
            ppv->reqElems = reqElems > ppv->nElems ? ppv->nElems : reqElems;

                                /* Issue CA request */
            /* install monitor once with first connect */
            ppv->status = ca_create_subscription(ppv->dbrType,
                                                ppv->reqElems,
                                                ppv->chid,
                                                eventMask,
                                                eventCallback,
                                                (void*)ppv,// callback argument
                                                &ppv->evid);

            
            //*******************
            //如果这些都成功了，也往数据库里写一条数据。
            //PV上线和下线都在数据库里进行记录
            //*******************
            //-----------------
            //注意！如果一段代码在超过一个地方调用，那么请单独封装成一个函数。否则在修改时，你就需要同时修改多个地方。
            //------------------
             
        }
        ppv->isConnected = 1;
        
        PVStatus2TD(Archiver->taos, ppv, 1);//在线写1      
    }
    else if ( args.op == CA_OP_CONN_DOWN ) {//连接断开时
        nConn--;
        ppv->status = ECA_DISCONN;
        ppv->isConnected = 0;
        print_time_val_sts(ppv, reqElems);
        PVStatus2TD(Archiver->taos, ppv, 0);//不在线写0
    }
}



int main(int argc,char **argv)
{
   
    char *filename;
    int         npv = 0;
    
    //pv          *pmynode[MAX_PV];
    pv** pmynode;
    pmynode  = (pv **) callocMustSucceed(MAX_PV, sizeof(pv*), "caMonitor");
	if(pmynode){
	    printf("pmynode sucess!");	
    }
    char        *pname[MAX_PV];
    int i;
    char        tempStr[MAX_PV_NAME_LEN];
    char        *pstr;
    FILE        *fp;
    printf("Start!\n");
    aws_initAPI();
    printf("aws sdk initilized!\n");
    Archiver = archive_initial();
    printf("archiver initilized!\n");
    syslog(LOG_USER|LOG_INFO,"archiver initilized!\n"); 
    pvtable = (HASH_TABLE *)callocMustSucceed(1, sizeof(HASH_TABLE), "pvtable");
    
    void *zmqcontext = zmq_ctx_new();
    printf("zmq context initilized!\n");
    void *zmqpublisher = zmq_socket(zmqcontext, ZMQ_PUSH);
    //void *zmqpublisher = zmq_socket(zmqcontext, ZMQ_ROUTER);
    zmq_bind(zmqpublisher, INPROC);
    arraybuffer_initial(pvtable, zmqpublisher);
    printf("hash table initilized!\n");

    //void* dbr = getdbr(Archiver->s3client, "test");
    
    if (argc != 2) {
        fprintf(stderr,"usage: caMonitor filename\n");
        exit(1);
    }
    filename = argv[1];
    fp = fopen(filename,"r");
    if (!fp) {
        perror("fopen failed");
        return(1);
    }
    while (npv < MAX_PV) {
        size_t len;
        pstr = fgets(tempStr, MAX_PV_NAME_LEN, fp);
        if (!pstr) break;
        len = strlen(pstr);
        if (len <= 1) continue;
        pstr[len - 1] = '\0'; /* Strip newline */
        pmynode[npv] = (pv*)callocMustSucceed(1, sizeof(pv), "caMonitor");
        pmynode[npv]->name=epicsStrDup(pstr);
        npv++;
    }
    fclose(fp);
    //test_s3();
    // printf("pmynode's address in main func = %p\n",  pmynode);
    // for (i=0; i<npv; i++) {
    //     //pmynode[i]->callbackCounts = 0;
    //     ///printf("ddddddddddd:%d\n",pmynode[i]->callbackCounts);
    //     //pmynode[i]->isConnected = 0;
    //     printf("pmynode[%d] address in main func = %p\n", i, pmynode[i]);      
    // }
    
    //-------------------------------hdf test begin-----------------------------
    /*
    hid_t file_id, dataset_id, dataspace_id;
    hsize_t dims[2] = {2, 2};
    int data[2][2] = {{11, 12}, {21, 22}};

    file_id  = H5Fcreate("hdftest.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    dataspace_id = H5Screate_simple(2, dims, NULL);
    dataset_id = H5Dcreate2(file_id, "/dataset", H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

    H5Dclose(dataset_id);
    H5Sclose(dataspace_id);
    H5Fclose(file_id); 
    */
    //-------------------------------hdf test end----------------------------- 

    Archiver->nodelist = pmynode;
    Archiver->nPv = npv;
    Archiver->zmqctx = zmqcontext;
    printf("Setup monitor!\n");
    syslog(LOG_USER|LOG_INFO,"Setup monitor!\n"); 
    start_archive_thread(Archiver);          //启动读取线程，将fifo中的数据读出来写入TDengine
    printf("archiver thread started!\n");
    syslog(LOG_USER|LOG_INFO,"archiver thread started!\n"); 
    //创建订阅线程
    pthread_t hdf_thread;
    pthread_create(&hdf_thread, NULL, HDF_SAVE_thread, NULL);
    //多消费线程测试
    /*
    pthread_t sub_thread[5];
    int thread_i;
    int thread_nums[5] = {1,2,3,4,5};
    for(thread_i  = 0; thread_i < 5; thread_i ++) {
        //pthread_create(&sub_thread[thread_i], NULL, HDF_SAVE_thread, zmqcontext);
        pthread_create(&sub_thread[thread_i], NULL, HDF_SAVE_thread1, &thread_nums[thread_i]);
    }
    */

    SEVCHK(ca_context_create(ca_disable_preemptive_callback),"ca_context_create");
    SEVCHK(ca_add_exception_event(exceptionCallback,NULL),
        "ca_add_exception_event");
    for (i=0; i<npv; i++) {  
        SEVCHK(ca_create_channel(pmynode[i]->name,connectionCallback,
                (void*)pmynode[i],20,&pmynode[i]->chid),
                "ca_create_channel");
        SEVCHK(ca_replace_access_rights_event(pmynode[i]->chid,
                accessRightsCallback),
                "ca_replace_access_rights_event");
    }
    start_archiver_monitor(Archiver);
    //return 0;
    //archiver_monitor_thread(Archiver);
    /*Should never return from following call*/
    SEVCHK(ca_pend_event(0.0),"ca_pend_event");
    aws_shutdownAPI();
    return 0;
}
