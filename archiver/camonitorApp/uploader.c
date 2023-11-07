#pragma once

#include "uploader.h"
#include "zmqInterface.h"
#include <cadef.h>
#include "archiver.h"

//接受一个void*类型的context，使用该context创建订阅socket
void *HDF_SAVE_thread(void *arg) {
    int thread_num = *((int*)arg);
    //void *context = zmq_ctx_new();
    void *subscriber = zmq_socket(Archiver->zmqctx, ZMQ_PULL);
    // 连接到发布者
    zmq_connect(subscriber, INPROC);

    //char buffer[256];
    //zmq_recv(subscriber, buffer, 256, 0);
    //printf("zmq recv test:%s\n", buffer);
    // 接收消息
    ARCHIVE_MESSAGE buffer;
    
    while (true)
    {
        /* code */
        zmq_recv(subscriber, &buffer, sizeof(ARCHIVE_MESSAGE), 0);
        //printf("Thread:%d, pvname: %s\n", thread_num, buffer.pvname);
        time_t t;
        struct tm *tm_info;

        time(&t);
        tm_info = localtime(&t);

        char time_str[20];
        strftime(time_str, 20, "%Y%m%d_%H%M%S", tm_info);

        char filename[100];
        sprintf(filename, "%s+%s.h5", buffer.pvname, time_str);

        hdfpack(buffer, filename);

        s3UploadHDFtest(Archiver->s3client, filename);

        //write index to taos...
        char time_str1[20];
        strftime(time_str1, 20 , "%Y-%m-%d %H:%M:%S", tm_info);
        Hdf2TD(Archiver->taos, time_str1, buffer.pvname, buffer.array_count);
    }
    
}

void *HDF_SAVE_thread1(void *arg) {
    //void *context = zmq_ctx_new();
    int thread_num = *((int*)arg);
    printf("Thread:%d\n", thread_num);
    hid_t file_id, dataset_id, dataspace_id;
    herr_t status;

    char filename[100];
    sprintf(filename, "thread-%d.h5",  thread_num);

    
    file_id = H5Fcreate(filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    hsize_t dims[2] = {2, 3};
    dataspace_id = H5Screate_simple(2, dims, NULL);
    dataset_id  = H5Dcreate2(file_id, "/dataset", H5T_STD_I32LE, dataspace_id,  H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    int data[2][3] = {{1,2,3},{4,5,6}};
    status = H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);

    H5Dclose(dataset_id);
    H5Sclose(dataspace_id);
    H5Fclose(file_id);
}
