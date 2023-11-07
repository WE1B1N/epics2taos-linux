#include "zmqInterface.h"
#include "hdf5Interface.h"

void *HDF_SAVE_thread(void *arg);
void *HDF_SAVE_thread1(void *arg);
void write_data(int thread_id);
//void *router_thread(void * arg);