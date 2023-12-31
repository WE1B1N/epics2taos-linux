#include "s3.h"
#include "loadConfig.h"
#include <unistd.h>


static const char ALLOCATION_TAG[] = "camonitor";
Aws::SDKOptions options;
Aws::String region;
std::mutex upload_mutex;
std::condition_variable upload_variable;
long time1;


// List all Amazon Simple Storage Service (Amazon S3) buckets under the account.
bool ListBuckets(const Aws::S3::S3Client& s3Client) {

    Aws::S3::Model::ListBucketsOutcome outcome = s3Client.ListBuckets();

    if (outcome.IsSuccess()) {
        std::cout << "All buckets under my account:" << std::endl;

        for (auto const& bucket : outcome.GetResult().GetBuckets())
        {
            std::cout << "  * " << bucket.GetName() << std::endl;
        }
        std::cout << std::endl;

        return true;
    }
    else {
        std::cout << "ListBuckets error:\n"<< outcome.GetError().GetMessage() << std::endl << std::endl;

        return false;
    }
}

// Create an Amazon Simple Storage Service (Amazon S3) bucket.
bool CreateBucket(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName) {

    std::cout << "Creating bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::CreateBucketRequest request;
    request.SetBucket(bucketName);

    //  If you don't specify an AWS Region, the bucket is created in the US East (N. Virginia) Region (us-east-1)
    // if (locConstraint != Aws::S3::Model::BucketLocationConstraint::us_east_1)
    // {
    //     Aws::S3::Model::CreateBucketConfiguration bucket_config;
    //     bucket_config.SetLocationConstraint(locConstraint);

    //     request.SetCreateBucketConfiguration(bucket_config);
    // }
    /*
    
    Aws::S3::Model::LifecycleRule rule;
    
    Aws::S3::Model::LifecycleRuleFilter lcrf;
    lcrf.SetPrefix("none");
    
    rule.SetID(bucketName);
    rule.SetFilter(lcrf);
    rule.SetStatus(Aws::S3::Model::ExpirationStatus::Enabled);

    Aws::S3::Model::LifecycleExpiration expiration;
    expiration.SetDays(30);
    rule.SetExpiration(expiration);

    Aws::S3::Model::BucketLifecycleConfiguration blccfg;
    blccfg.AddRules(rule);

    Aws::S3::Model::PutBucketLifecycleConfigurationRequest req;
    req.SetBucket(bucketName);
    req.SetLifecycleConfiguration(blccfg);    
    */
    Aws::S3::Model::CreateBucketOutcome outcome = s3Client.CreateBucket(request);

    if (outcome.IsSuccess()) {
        std::cout << "Bucket created." << std::endl << std::endl;
        //AddLifecycle(s3Client, bucketName);
        //auto outcome1 = s3Client.PutBucketLifecycleConfiguration(req);
        //if(outcome1.IsSuccess()) {
        return true;
        //} else {
            //return false;
        //}

        
    }
    else {
        std::cout << "CreateBucket error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;

        return false;
    }
}

// Delete an existing Amazon S3 bucket.
bool DeleteBucket(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName) {

    std::cout << "Deleting bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::DeleteBucketRequest request;
    request.SetBucket(bucketName);

    Aws::S3::Model::DeleteBucketOutcome outcome = s3Client.DeleteBucket(request);

    if (outcome.IsSuccess()) {
        std::cout << "Bucket deleted." << std::endl << std::endl;

        return true;
    }
    else {
        std::cout << "DeleteBucket error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;

        return false;
    }
}

// Put an Amazon S3 object to the bucket.
bool PutObjectFile(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName, const Aws::String& objectKey, const Aws::String& fileName) {

    std::cout << "Putting object: \"" << objectKey << "\" to bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectKey);
    std::shared_ptr<Aws::IOStream> bodyStream = Aws::MakeShared<Aws::FStream>(ALLOCATION_TAG, fileName.c_str(), std::ios_base::in | std::ios_base::binary);
    if (!bodyStream->good()) {
        std::cout << "Failed to open file: \"" << fileName << "\"." << std::endl << std::endl;
        return false;
    }
    request.SetBody(bodyStream);
    
    //A PUT operation turns into a multipart upload using the s3-crt client.
    //https://github.com/aws/aws-sdk-cpp/wiki/Improving-S3-Throughput-with-AWS-SDK-for-CPP-v1.9
    Aws::S3::Model::PutObjectOutcome outcome = s3Client.PutObject(request);
    //auto outcome = s3CrtClient.PutObject(request);
    if (outcome.IsSuccess()) {
        std::cout << "Object added." << std::endl << std::endl;

        return true;
    }
    else {
        std::cout << "PutObject error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;
        return false;
    }
}


bool PutObjectDbr(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName, const Aws::String& objectKey, void *file_image, size_t file_size){
    std::cout << "Putting object: \"" << objectKey << "\" to bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucketName).WithKey(objectKey);

    //std::stringstream data_stream;
    //data_stream.write(reinterpret_cast<char*>(dbr), dbrsize);
    
    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    
    data->write(static_cast<char*>(file_image), file_size);

    request.SetBody(data);

    auto outcome = s3Client.PutObject(request);
    if (outcome.IsSuccess()) {
        std::cout << "Object added." << std::endl << std::endl;
        return true;
    }
    else {
        std::cout << "PutObject error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;
        return false;
    }

}



bool PutObjectDbrAsync(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName, const Aws::String& objectKey, void *dbr, size_t dbrsize){
    clock_t start_time, end_time;
    double total_time;

   
    //std::cout << "Putting object: \"" << objectKey << "\" to bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(bucketName).WithKey(objectKey);

    //std::stringstream data_stream;
    //data_stream.write(reinterpret_cast<char*>(dbr), dbrsize);
    
    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    
    data->write(static_cast<char*>(dbr), dbrsize);
    

    request.SetBody(data);

    //free(dbr);
    // Create and configure the context for the asynchronous put object request.

    std::shared_ptr<Aws::Client::ArchiveContext> context =
            Aws::MakeShared<Aws::Client::ArchiveContext>("PutObjectAllocationTag");
    //context->SetBuffPointer(dbr);
    //context->SetUUID(objectKey);
    //start_time = clock(); // 记录开始时间

    s3Client.PutObjectAsync(request, PutObjectAsyncFinished, context);

    return true;

}

/*
// Get the Amazon S3 object from the bucket.
void* GetObjectDbr(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName, const Aws::String& objectKey) {

    std::cout << "Getting object: \"" << objectKey << "\" from bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::GetObjectRequest request;
    
    request.SetBucket(bucketName);
    request.SetKey(objectKey);

    Aws::S3::Model::GetObjectOutcome outcome = s3Client.GetObject(request);

    if (outcome.IsSuccess()) {
       //Uncomment this line if you wish to have the contents of the file displayed. Not recommended for large files
       // because it takes a while.
       // std::cout << "Object content: " << outcome.GetResult().GetBody().rdbuf() << std::endl << std::endl;
        auto& object = outcome.GetResultWithOwnership().GetBody();
        std::streambuf* buf = object.rdbuf();
        //Aws::OFStream local_file("local-file.txt", std::ios::out | std::ios::binary);
        //local_file << object.rdbuf();
        std::string filename = "local-file.txt"; 
        std::ifstream file(filename, std::ios::binary);
        if (file.is_open()) {
            char buffer[1024];
            while (file.read(buffer, sizeof(buffer)))
            {
                // 处理读取到的数据
            }
            if (file.gcount() > 0)
            {
                // 处理最后剩余的数据
            }
             std::cout << "buffer:"<< buffer << std::endl;
        
        } else {
            std::cout << "Unable to open file" << std::endl;
        }
        file.close();      
        int bufsize = 96;
        char cbuf[96] = {0};
        std::streamsize count  = buf->sgetn(cbuf, bufsize);
         
        auto& object_stream = outcome.GetResultWithOwnership().GetBody();
        auto object_size = object_stream.tellg();
        object_stream.seekg(0, std::ios::beg);
        void* object_data = new char[object_size];
        object_stream.read((char*)object_data, object_size);
        delete[] (char*)object_data;

        //std::streamsize size = 96;
        //sstream.read(result, size);
        return object_data;
        
        return 0;        
    }
    else {
        std::cout << "GetObject error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;
        return 0;
    }

    //auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", std::stringstream::in | std::stringstream::out | std::stringstream::binary);
    // auto& objectStream = outcome.GetResult().GetBody();
    // auto data = objectStream.rdbuf()
    // std::streambuf* buffer = objectStream.rdbuf();
    // size_t size = buffer->in_avail();
    // char * buffer_data = new char[size];
    // buffer->sgetn(buffer_data, size);

    // void* object_data = static_cast<void*>(buffer_data);
    // return object_data;
}
*/

// Delete the Amazon S3 object from the bucket.
bool DeleteObject(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName, const Aws::String& objectKey) {

    std::cout << "Deleting object: \"" << objectKey << "\" from bucket: \"" << bucketName << "\" ..." << std::endl;

    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucketName);
    request.SetKey(objectKey);

    Aws::S3::Model::DeleteObjectOutcome outcome = s3Client.DeleteObject(request);

    if (outcome.IsSuccess()) {
        std::cout << "Object deleted." << std::endl << std::endl;

        return true;
    }
    else {
        std::cout << "DeleteObject error:\n" << outcome.GetError().GetMessage() << std::endl << std::endl;

        return false;
    }
}

void aws_initAPI()
{
    Aws::InitAPI(options);
}

void aws_shutdownAPI() 
{  
    Aws::ShutdownAPI(options);
}

void * s3Client_init(){ 
    region = Aws::Region::US_EAST_1;
    const double throughput_target_gbps = 5;
    const uint64_t part_size = 8 * 1024 * 1024; // 8 MB.

    char  **fileData = NULL;
    int lines = 0;
    struct ConfigInfo* info = NULL;
    loadFile_configFile("./config.ini", &fileData, &lines);
    parseFile_configFile(fileData, lines, &info);
    char *endpoint = getInfo_configFile("s3_endpoint", info, lines);
    char *ak = getInfo_configFile("s3_accesskey", info, lines);
    char *sk = getInfo_configFile("s3_secretkey", info, lines);
    //std::cout << endpoint << ak << sk << std::endl;

    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = endpoint;
   
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.verifySSL = false;

    //-------------需要配置最大连接数和线程池大小。如果不用PooledThreadExecutor模式最大线程数会导致线程数目不断增加，最终超过系统限制而崩溃
    cfg.maxConnections = 100;
    //cfg.enableEndpointDiscovery = true;

    //-------------不加下面这一行会内存泄漏！！----------------------------
    Aws::Utils::Threading::OverflowPolicy policy =Aws::Utils::Threading::OverflowPolicy::REJECT_IMMEDIATELY;

    auto executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>("S3Adapter.S3Client", 50, policy);
    cfg.executor = executor;
    //----------------------------------------------------------

    Aws::Auth::AWSCredentials cred(ak,sk);

    //Aws::Vector<Aws::String> endpointList = {"172.16.0.171:9000", "172.16.0.172:9000", "172.16.0.173:9000"};
    
    Aws::S3::S3Client *s3Client = new Aws::S3::S3Client(cred, cfg, false, false);
    
    //Aws::S3::S3Client *s3Client = new Aws::S3::S3Client(cfg, Aws::MakeShared<Aws::S3::RoundRobinLBStrategy>("s3-lb-strategy", endpointList));
    void *s3Client_ptr = s3Client;
    return s3Client_ptr; 
   
}


void s3_upload(void *s3client, char * filename, void *file_image, size_t file_size) {


    //Aws::S3::Model::PutObjectRequest request;

    //Aws::S3::Model::BucketLocationConstraint locConstraint = Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(region);

    Aws::String bucket_name = "arraydata-bucket";

    //Aws::S3::Model::HeadBucketRequest hbr;
   // hbr.SetBucket(bucket_name);

    //如果bucket不存在，先创建bucket
    //if(!(*static_cast<Aws::S3::S3Client *>(s3Client)).HeadBucket(hbr).IsSuccess()){
    //    CreateBucket(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name, locConstraint);
    //}    

    Aws::String object_key;
    object_key = filename;
    //std::cout << object_key << std::endl << std::endl;
    //Aws::String bucket_name = "my-bucket";
    bool outcome = PutObjectDbr(*static_cast<Aws::S3::S3Client *>(s3client), bucket_name, object_key, file_image, file_size);
    //bool outcome = PutObjectFile(*static_cast<Aws::S3::S3Client *>(s3client), bucket_name, object_key, filename);
}

void s3UploadHDFtest(void *s3client, char * filename) {


    //Aws::S3::Model::PutObjectRequest request;

    //Aws::S3::Model::BucketLocationConstraint locConstraint = Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(region);

    Aws::String bucket_name = "arraydata-bucket";

    //Aws::S3::Model::HeadBucketRequest hbr;
   // hbr.SetBucket(bucket_name);

    //如果bucket不存在，先创建bucket
    //if(!(*static_cast<Aws::S3::S3Client *>(s3Client)).HeadBucket(hbr).IsSuccess()){
    //    CreateBucket(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name, locConstraint);
    //}    
    Aws::String fpath = "/tmp/ramdisk/";
    fpath += filename;

    Aws::String object_key;
    object_key = filename;
    //std::cout << object_key << std::endl << std::endl;
    //Aws::String bucket_name = "my-bucket";
    bool outcome = PutObjectFile(*static_cast<Aws::S3::S3Client *>(s3client), bucket_name, object_key, fpath);
    //bool outcome = PutObjectFile(*static_cast<Aws::S3::S3Client *>(s3client), bucket_name, object_key, filename);
    
    removeHDF(fpath.c_str());
}

void removeHDF(const char * filename) {
    if(std::remove(filename) != 0) {    
        perror("Error deleting file");

    } else {
        puts("File successfully deleted");
    }
}


void s3_upload_asyn(void *s3Client, void * dbr, char * pvname, size_t dbrsize, unsigned long time, unsigned long midnight_ts) {
    Aws::S3::Model::PutObjectRequest request;

   // Aws::S3::Model::BucketLocationConstraint locConstraint = Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(region);

    Aws::String bucket_name;
    bucket_name =  Aws::String(std::to_string(midnight_ts).c_str());
    
    /*
    long unixts = time / 1000000000;
    
    //全局变量time1在进程启动后第一次上传数据时赋值，time2在该值的基础上加86400，即一天的秒数
    //如果最新的数据时间不超过time2，即为在同一天内的数据，上传到同一个bucket内；否则上传到新bucket，并更新time1
    if(time1 == NULL) {
        time1 = unixts;
    }
    long time2 = time1 + 86400;
    if(unixts >= time2) {
        bucket_name = Aws::String(("ts" + std::to_string(unixts)).c_str());
        time1 = unixts;
    } else {
        bucket_name = Aws::String(("ts" + std::to_string(time1)).c_str());
    }
    */

    Aws::S3::Model::HeadBucketRequest hbr;
    hbr.SetBucket(bucket_name);

    //测试往桶ts1687833328添加生命周期
    //Aws::String str = "ts1687833328";
    //AddLifecycle(*static_cast<Aws::S3::S3Client *>(s3Client), str);

    //使用HeadBucket判断bucket是否存在，如果bucket不存在，先创建

    if(!(*static_cast<Aws::S3::S3Client *>(s3Client)).HeadBucket(hbr).IsSuccess()){    
        CreateBucket(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name);
        /*
        if(CreateBucket(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name)) {
        //如果成功创建新桶，往桶内添加生命周期规则，默认bucket lifecycle: 30d
        //AddLifecycle(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name);
        }  
        */
           
    }    

    Aws::String object_key;
    object_key = Aws::String((pvname + std::to_string(time)).c_str());

    //std::cout << object_key << std::endl << std::endl;
    //Aws::String bucket_name = "my-bucket";
    //std::unique_lock<std::mutex> lock(upload_mutex);
    bool outcome =PutObjectDbrAsync(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name, object_key, dbr, dbrsize);
    //std::cout << "main: Waiting for file upload attempt..." << std::endl << std::endl;
    ///upload_variable.wait(lock);
    //std::cout << std::endl << "main: File upload attempt completed." << std::endl;

   // bool outcome = PutObjectDbrAsync(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name, object_key, dbr, dbrsize);
}

/*
void *getdbr(void *s3Client, char *objectKey){
    //for test
    Aws::String object_key = "zheng1:compressExample1679650152938633705";
    Aws::String bucket_name = "pvarray-bucket";

    return GetObjectDbr(*static_cast<Aws::S3::S3Client *>(s3Client), bucket_name, object_key);
}
*/

void PutObjectAsyncFinished(const Aws::S3::S3Client *s3Client,
                            const Aws::S3::Model::PutObjectRequest &request,
                            const Aws::S3::Model::PutObjectOutcome &outcome,
                            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context) {
    /*
    if (outcome.IsSuccess()) {
        std::cout << "Success: PutObjectAsyncFinished: Finished uploading '"
                 << context->GetUUID() << "'.\n" << std::endl;
    }
    else {
        std::cerr << "Error: PutObjectAsyncFinished: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }
    */
    //----------------------将父类指针转换为子类的指针-----------------------------------------------
    Aws::Client::ArchiveContext* myContext = const_cast<Aws::Client::ArchiveContext*>(dynamic_cast<const Aws::Client::ArchiveContext*>(context.get()));
    if(myContext==nullptr){
        std::cout << "Error when casting pointer type!"<< std::endl;
        return;
    }
    //myContext->FreeBuff();

    // Unblock the thread that is waiting for this function to complete.
    //upload_variable.notify_one();
}

/*
void PutBucketLifecycleConfigurationFinished(const Aws::S3::S3Client *s3Client,
                            const Aws::S3::Model::PutBucketLifecycleConfigurationRequest &request,
                            const Aws::S3::Model::PutBucketLifecycleConfigurationOutcome &outcome,
                            const std::shared_ptr<const Aws::Client::AsyncCallerContext> &context) {
    
    if (outcome.IsSuccess()) {
       std::cout << "Success: PutBucketLifecycleConfigurationAsyncFinished: Finished uploading '"
                 << context->GetUUID() << "'.\n" << std::endl;
    }
    else {
        std::cerr << "Error: PutBucketLifecycleConfigurationAsyncFinished: " <<
                  outcome.GetError().GetMessage() << std::endl;
    }
    
    //----------------------将父类指针转换为子类的指针-----------------------------------------------
    Aws::Client::ArchiveContext* myContext = const_cast<Aws::Client::ArchiveContext*>(dynamic_cast<const Aws::Client::ArchiveContext*>(context.get()));
    if(myContext==nullptr){
        std::cout << "Error when casting pointer type!"<< std::endl;
        return;
    }
    //myContext->FreeBuff();

    // Unblock the thread that is waiting for this function to complete.
    //upload_variable.notify_one(); 
}

bool AddLifecycle(const Aws::S3::S3Client& s3Client, const Aws::String& bucketName) {
    //Aws::String ruleid = Aws::String("rule-") + bucketName;
    std::cout << "Putting lifecycle configuration to bucket \"" << bucketName << "\" " << std::endl;
    
    Aws::S3::Model::LifecycleRuleFilter lcrf;
    lcrf.SetPrefix("none");

    Aws::S3::Model::LifecycleRule rule;
    rule.SetID(bucketName);
    rule.SetFilter(lcrf);
    rule.SetStatus(Aws::S3::Model::ExpirationStatus::Enabled);

    Aws::S3::Model::LifecycleExpiration expiration;
    expiration.SetDays(30);
    rule.SetExpiration(expiration);

    Aws::S3::Model::BucketLifecycleConfiguration blccfg;
    blccfg.AddRules(rule);

    Aws::S3::Model::PutBucketLifecycleConfigurationRequest req; 
    req.SetBucket(bucketName);
    req.SetLifecycleConfiguration(blccfg);    

    //std::shared_ptr<Aws::Client::ArchiveContext> context =
    //    Aws::MakeShared<Aws::Client::ArchiveContext>("PutBucketLifecycleConfigurationAllocationTag");
    //std::shared_ptr<Aws::Client::AsyncCallerContext> context = Aws::MakeShared<Aws::Client::AsyncCallerContext>("PutBucketLifecycleConfigurationAllocationTag");
    //context->SetUUID(bucketName);
    //context->SetBuffPointer(dbr);
    Aws::S3::Model::PutBucketLifecycleConfigurationOutcome outcome = s3Client.PutBucketLifecycleConfiguration(req);//-----------------这里执行完报错
    //s3Client.PutBucketLifecycleConfigurationAsync(req, PutBucketLifecycleConfigurationFinished, context);
    //Aws::String error = outcome.GetError().GetMessage();

    if(outcome.IsSuccess())
    {
        std::cout << "Lifecycle configuration set successfully for bucket: " << bucketName << std::endl;
        return true;
    } else {
        std::cout<< "Failed to set lifecycle configuration for bucket: " << bucketName << std::endl;
        std::cout << "Error: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    } 
}
*/
/*
s3_upload_asyn函数内部在往桶内上传对象前先判断当日的桶是否存在，存在的则直接上传到当日桶内；
否则先创建当日的桶，并调用AddLifecycle方法为该桶添加生命周期
*/


