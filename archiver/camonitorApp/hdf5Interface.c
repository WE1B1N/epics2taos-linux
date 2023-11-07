#pragma once

#include "hdf5Interface.h"
#include "archiver.h"

#define ALLOCATE_MEMORY_FOR_PDATAS(type, arr, size1, size2) \
    do { \
        int i1; \
        for(i1 = 0; i1 < size1; i1++) { \
            (arr)[i1] = malloc(size2 * sizeof(type)); \
        } \
    } while (0)

extern HASH_TABLE* pvtable;
void hdfpack(ARCHIVE_MESSAGE buffer, char * filename){

    //hid_t file_id, dataspace_id, dataset_id, datatype_id;
    hid_t file_id, dataspace_id, datatype_id;
    //hid_t fapl_id;
    herr_t status;  herr_t h5Result;
    int dimx = buffer.array_count;//需要打包的数据数量
    int dimy;   

    char fpath[100];
    sprintf(fpath, "/tmp/ramdisk/%s", filename);
    //printf("fpath:%s\n", fpath);
    file_id = H5Fcreate(fpath, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
        
    int x;
    unsigned base_type;
    //printf("dimx:%d\n", dimx);

    ARRAY_DATA array1[dimx];//array1用来存放整个一连串的array data的拷贝
    void * array2[dimx];//array2用来存放一连串的array data中的pdata指针的拷贝，需要提前分配好空间，并在用完后释放
    epicsMutexMustLock(pvtable->bufferLock);//拷贝前上锁，拷贝完成后立即释放
    for(x = 0; x < dimx; x++) {
        ARRAY_DATA array_data = readarray_ts(pvtable, buffer.pvname);
        dimy = array_data.count;
        memcpy(&array1[x], &array_data, sizeof(ARRAY_DATA));
    
        if(x == 0) {
            base_type = array_data.type % (LAST_TYPE+1);
            //确定hdf中存储的数据类型，并且提前为array2分配对应类型大小的空间
            switch(base_type) {
                case DBR_STRING:
                    datatype_id = H5T_NATIVE_CHAR;//HDF DOESNT HAVE TYPE STRING
                    break;
                case DBR_FLOAT:
                    datatype_id = H5T_NATIVE_FLOAT;
                    ALLOCATE_MEMORY_FOR_PDATAS(float, array2, dimx, dimy);   
                    break;
                case DBR_DOUBLE:
                    datatype_id = H5T_NATIVE_DOUBLE;
                    ALLOCATE_MEMORY_FOR_PDATAS(double, array2, dimx, dimy);
                    break;
                case DBR_CHAR:
                    datatype_id = H5T_NATIVE_CHAR;
                    ALLOCATE_MEMORY_FOR_PDATAS(char, array2, dimx, dimy);
                    break;
                case DBR_INT:
                    datatype_id = H5T_NATIVE_INT;
                    ALLOCATE_MEMORY_FOR_PDATAS(int, array2, dimx, dimy);
                    break;
                case DBR_LONG:
                    datatype_id = H5T_NATIVE_LONG;
                    ALLOCATE_MEMORY_FOR_PDATAS(long, array2, dimx, dimy);
                    break;
                default:
                    datatype_id = H5T_NATIVE_DOUBLE;
                    ALLOCATE_MEMORY_FOR_PDATAS(double, array2, dimx, dimy);
            }  
        }

        switch(base_type) {
            case DBR_STRING:
                break;
            case DBR_FLOAT:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(float));
                break;
            case DBR_DOUBLE:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(double));
                break;
            case DBR_CHAR:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(char));
                break;
            case DBR_INT:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(int));
                break;
            case DBR_LONG:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(long));
                break;
            default:
                memcpy(array2[x], array_data.pdata, dimy * sizeof(double));
        } 
        //释放pdata
        free(array_data.pdata);
    }
    epicsMutexUnlock(pvtable->bufferLock);

    set_sflag(pvtable, buffer.pvname, 1);//拷贝完成后将flag设置为1，生产线程在数量达到阈值就可以再次通知消费线程；如果flag为0，说明拷贝未完成，则不发送读取请求
 

    
    hsize_t dims[1] = {dimy};//存储维度为1，大小为pdata内的数据数量
    dataspace_id = H5Screate_simple(1, dims, NULL);//创建相应的dataspace
    
    //dataset_id  = H5Dcreate(file_id, "dataset", datatype_id, dataspace_id,  H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    int x1; 
    for(x1 = 0; x1 < dimx; x1++) {
        char dname[50];
        unsigned long taosts = epicsTime2int(array1[x1].timestamp);
        
        sprintf(dname, "%lu", taosts);
        
        //以数据的ns时间戳为名字建立dataset
        hid_t dataset_id  = H5Dcreate(file_id, dname, datatype_id, dataspace_id,  H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        //status = H5Dwrite(dataset_id, datatype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, array1[x1].pdata);
        
        status = H5Dwrite(dataset_id, datatype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, array2[x1]);
        
        H5Dclose(dataset_id);
        free(array2[x1]);//释放array2
    }

    H5Sclose(dataspace_id);
    H5Fclose(file_id);
    
}