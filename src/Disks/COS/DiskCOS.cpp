#include "DiskCOS.h"

#include "Disks/DiskFactory.h"
#include "Poco/DateTime.h"

#include <random>
#include <utility>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>

#include <aws/core/http/HttpResponse.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <boost/algorithm/string.hpp>

using std::vector;
using Poco::DateTime;

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
    extern const int INCORRECT_DISK_INDEX;
    extern const int NOT_IMPLEMENTED;
}
namespace
{
    template <typename Result, typename Error>
    void throwIfError(Aws::Utils::Outcome<Result, Error> && response)
    {
        if (!response.IsSuccess())
        {
            const auto & err = response.GetError();
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
    }

    class ReadBufferFromCos final : public ReadBufferFromFileBase
    {
    public:
        ReadBufferFromCos(
            std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_, const String & path_, const size_t buf_size_)
            : client_ptr(std::move(client_ptr_)), bucket(bucket_), path(path_), buf_size(buf_size_)
        {
        }
        off_t seek(off_t offset_, int whence) override
        {
            if (whence == SEEK_CUR)
            {
                /// If position within current working buffer - shift pos.
                if (working_buffer.size() && size_t(getPosition() + offset_) < absolute_position)
                {
                    pos += offset_;
                    return getPosition();
                }
                else
                {
                    absolute_position += offset_;
                }
            }
            else if (whence == SEEK_SET)
            {
                /// If position within current working buffer - shift pos.
                if (working_buffer.size() && size_t(offset_) >= absolute_position - working_buffer.size()
                    && size_t(offset_) < absolute_position)
                {
                    pos = working_buffer.end() - (absolute_position - offset_);
                    return getPosition();
                }
                else
                {
                    absolute_position = offset_;
                }
            }
            else
                throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            current_buf = initialize();
            pos = working_buffer.end();

            return absolute_position;
        }
        std::string getFileName() const override
        {
            auto p = Poco::Path(path);
            return p.getBaseName();
        }
        off_t getPosition() override { return absolute_position - available(); }

    private:
        std::unique_ptr<ReadBufferFromS3> initialize()
        {
            size_t offset = absolute_position;
            auto buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, path, buf_size);
            buf->seek(offset, SEEK_SET);
            return buf;
            //return nullptr;
        }

        bool nextImpl() override
        {
            /// Find first available buffer that fits to given offset.
            if (!current_buf)
                current_buf = initialize();

            /// If current buffer has remaining data - use it.
            if (current_buf && current_buf->next())
            {
                working_buffer = current_buf->buffer();
                absolute_position += working_buffer.size();
                return true;
            }
            return false;
        }
        std::shared_ptr<Aws::S3::S3Client> client_ptr;
        std::unique_ptr<ReadBufferFromS3> current_buf;
        const String bucket;
        const String path;

        size_t buf_size;
        size_t absolute_position = 0;
        size_t current_buf_idx = 0;
    };

    class WriteBufferFromCos final : public WriteBufferFromFileBase
    {
    public:
        WriteBufferFromCos(
            std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
            const String & bucket_,
            const String & path_,
            bool is_multipart,
            size_t min_upload_part_size,
            size_t buf_size_)
            : WriteBufferFromFileBase(buf_size_, nullptr, 0)
            , impl(WriteBufferFromS3(client_ptr_, bucket_, path_, min_upload_part_size, is_multipart, buf_size_))
            , path(path_)
        {
        }

        ~WriteBufferFromCos() override
        {
            try
            {
                finalize();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        void finalize() override
        {
            if (finalized)
                return;
            next();
            impl.finalize();
            finalized = true;
        }
        void sync() override { }
        std::string getFileName() const override
        {
            auto p = Poco::Path(path);
            return p.getBaseName();
        }

    private:
        void nextImpl() override
        {
            /// Transfer current working buffer to WriteBufferFromS3.
            impl.swap(*this);

            /// Write actual data to S3.
            impl.next();

            /// Return back working buffer.
            impl.swap(*this);
        }
        WriteBufferFromS3 impl;
        bool finalized = false;
        String path;
    };
}

class DiskCOSDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskCOSDirectoryIterator(const vector<String> file_list_) : file_list(file_list_) { iter = file_list.begin(); }

    void next() override { ++iter; }

    bool isValid() const override { return iter < file_list.end(); }

    String path() const override
    {
        auto path = (*iter);
        LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "DiskCOSDirectoryIterator path get ====== : '{}'", path);
        return path;
    }

    String name() const override
    {
        auto path = (*iter);
        String name = "";

        if (path[path.size() - 1] == '/')
        {
            name = path.substr(0, path.size() - 1);
        }
        else
        {
            name = path;
        }
        auto pos = name.find_last_of("/");
        if (pos != String::npos)
        {
            name = name.substr(pos + 1, path.size());
        }
        LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "DiskCOSDirectoryIterator path: {} name : '{}'", path, name);
        return name;
    }

private:
    vector<String> file_list;
    vector<String>::iterator iter;
};


using DiskCOSPtr = std::shared_ptr<DiskCOS>;

class DiskCOSReservation final : public IReservation
{
public:
    DiskCOSReservation(const DiskCOSPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
        {
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        }
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override { size = new_size; }

    ~DiskCOSReservation() override { }

private:
    DiskCOSPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

/// Runs tasks asynchronously using global thread pool.
class AsyncExecutor : public Executor
{
public:
    explicit AsyncExecutor() = default;

    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();

        GlobalThreadPool::instance().scheduleOrThrowOnError([promise, task]() {
            try
            {
                task();
                promise->set_value();
            }
            catch (...)
            {
                tryLogCurrentException(&Poco::Logger::get("DiskCOS"), "Failed to run async task");

                try
                {
                    promise->set_exception(std::current_exception());
                }
                catch (...)
                {
                }
            }
        });

        return promise->get_future();
    }
};


DiskCOS::DiskCOS(
    String name_,
    std::shared_ptr<Aws::S3::S3Client> client_,
    String bucket_,
    String cos_root_path_,
    size_t min_upload_part_size_,
    size_t min_multi_part_upload_size_,
    size_t min_bytes_for_seek_)
    : IDisk(std::make_unique<AsyncExecutor>())
    , name(std::move(name_))
    , client(std::move(client_))
    , bucket(std::move(bucket_))
    , cos_root_path(std::move(cos_root_path_))
    , min_upload_part_size(min_upload_part_size_)
    , min_multi_part_upload_size(min_multi_part_upload_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
{
}

ReservationPtr DiskCOS::reserve(UInt64 bytes)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "reserve : '{}'", bytes);
    return std::make_unique<DiskCOSReservation>(std::static_pointer_cast<DiskCOS>(shared_from_this()), bytes);
}

String DiskCOS::makeCosKey(const String & path) const
{
    if ((path.size() + 1) > cos_root_path.size())
    {
        auto root_path_ = path.substr(0, cos_root_path.size());
        if (root_path_ == cos_root_path)
        {
            return path;
        }
    }
    std::stringstream fullkey;
    fullkey << cos_root_path << path;
    return fullkey.str();
}

bool DiskCOS::exists(const String & path) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "exists : '{}'", makeCosKey(path));
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(makeCosKey(path));
    auto response = client->HeadObject(request);
    if (!response.IsSuccess())
    {
        auto result = response.GetResult();
        const auto & err = response.GetError();
        if (err.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        {
            return false;
        }
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "exists HeadObject fail path: '{}' err: '{}'.", path, err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
    return true;
}

bool DiskCOS::isFile(const String & path) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "isFile : '{}'", makeCosKey(path));
    if (path[path.size() - 1] == '/')
    {
        return false;
    }
    return true;
}

bool DiskCOS::isDirectory(const String & path) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "isFile : '{}'", makeCosKey(path));
    if (path[path.size() - 1] != '/')
    {
        return false;
    }
    return true;
}

size_t DiskCOS::getFileSize(const String & path) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "getFileSize : '{}'", makeCosKey(path));
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(makeCosKey(path));
    auto response = client->HeadObject(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "getFileSize Head Object fail path: '{}' err: '{}'.", path, err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
    return response.GetResult().GetContentLength();
}

void DiskCOS::createDirectory(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "createDirectory : '{}'", makeCosKey(path));
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket);
    if (path[path.size() - 1] == '/')
    {
        request.SetKey(makeCosKey(path));
    }
    else
    {
        request.SetKey(makeCosKey(path + "/"));
    }
    auto response = client->PutObject(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "Head Object fail path: '{}' err: '{}'.", path, err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
}

void DiskCOS::createDirectories(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "createDirectories : '{}'", makeCosKey(path));
    createDirectory(path);
}

DiskDirectoryIteratorPtr DiskCOS::iterateDirectory(const String & path)
{
    auto absolute_path = makeCosKey(path);
    vector<String> file_list;
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "iterateDirectory ListObject for key: '{}' ", path);
    String marker = "";
    do
    {
        Aws::S3::Model::ListObjectsRequest request;
        request.SetBucket(bucket);
        request.SetPrefix(absolute_path);
        request.SetMaxKeys(1000);
        request.SetDelimiter("/");
        request.SetMarker(marker);
        auto response = client->ListObjects(request);
        if (!response.IsSuccess())
        {
            const auto & err = response.GetError();
            LOG_ERROR(
                &Poco::Logger::get("DiskCOS"), "iterateDirectory ListObject fail path: '{}' err: '{}'.", absolute_path, err.GetMessage());
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
        marker = response.GetResult().GetNextMarker();
        auto object_list = response.GetResult().GetContents();
        LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "ListObject GetContents for key: '{}' get marker '{}'", path, marker);
        for (auto iter = object_list.begin(); iter != object_list.end(); iter++)
        {
            auto obj_file = (*iter);
            if (obj_file.GetKey() == absolute_path)
            {
                continue;
            }
            file_list.push_back(obj_file.GetKey());
        }
        auto common_prefix_list = response.GetResult().GetCommonPrefixes();
        for (auto iter = common_prefix_list.begin(); iter != common_prefix_list.end(); iter++)
        {
            auto common_prefix = (*iter);
            LOG_DEBUG(
                &Poco::Logger::get("DiskCOS"), "ListObject common_prefix for key: '{}' get key '{}'", path, common_prefix.GetPrefix());
            file_list.push_back(common_prefix.GetPrefix());
        }
    } while (marker.size() > 0);
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "iterateDirectory ListObject for key: '{}' and size: '{}' ", path, file_list.size());
    return std::make_unique<DiskCOSDirectoryIterator>(file_list);
}

void DiskCOS::clearDirectory(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "clearDirectory path: '{}'.", path);
    if (path[path.size() - 1] == '/')
    {
        removeRecursive(path);
    }
    else
    {
        removeRecursive(path + "/");
    }
}

void DiskCOS::copyCosFile(const String & from_path, const String & to_path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "copyCosFile  from_path: '{}' to_path: '{}'", from_path, to_path);

    Aws::S3::Model::CopyObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(makeCosKey(to_path));
    request.SetCopySource(bucket + "/" + makeCosKey(from_path));
    auto response = client->CopyObject(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        LOG_ERROR(
            &Poco::Logger::get("DiskCOS"),
            "copyCosFile CopyObject fail from_path: '{}' to_path: '{}'. err: '{}'",
            from_path,
            to_path,
            err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
}

void DiskCOS::removeCosFile(const String & path)
{
    Aws::S3::Model::DeleteObjectRequest delrequest;
    delrequest.SetBucket(bucket);
    delrequest.SetKey(makeCosKey(path));
    auto delresponse = client->DeleteObject(delrequest);
    if (!delresponse.IsSuccess())
    {
        const auto & err = delresponse.GetError();
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "removeCosFile DeleteObject fail path: '{}' err: '{}'.", path, err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
}

void DiskCOS::moveFile(const String & from_path, const String & to_path)
{
    auto absolute_from_path = makeCosKey(from_path);

    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "moveFile from_path: '{}' to_path '{}'.", from_path, to_path);
    if (!exists(to_path))
    {
        createDirectory(to_path);
    }
    if (isFile(from_path) && exists(from_path))
    {
        copyCosFile(from_path, to_path);
        removeCosFile(from_path);
    }
    else
    {
        String marker = "";
        do
        {
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket(bucket);
            request.SetPrefix(absolute_from_path);
            request.SetMaxKeys(1000);
            request.SetDelimiter("/");
            request.SetMarker(marker);
            auto response = client->ListObjects(request);
            if (!response.IsSuccess())
            {
                const auto & err = response.GetError();
                if (err.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
                {
                    return;
                }
                LOG_ERROR(&Poco::Logger::get("DiskCOS"), "moveFile isDirectory ListObject fail path: '{}' err: '{}'.", from_path, err.GetMessage());
                throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
            }
            marker = response.GetResult().GetNextMarker();
            auto object_list = response.GetResult().GetContents();
            LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "moveFile ListObject for key: '{}' get file list size: '{}'.", from_path, object_list.size());
            for (auto iter = object_list.begin(); iter != object_list.end(); iter++)
            {
                auto obj_file = (*iter);
                if (obj_file.GetKey() == absolute_from_path)
                {
                    continue;
                }
                if (isFile(obj_file.GetKey()) && exists(obj_file.GetKey()))
                {
                    auto file_name = obj_file.GetKey().substr(absolute_from_path.size(), obj_file.GetKey().size());
                    String to_dir_ = to_path;
                    if (to_path[to_path.size() - 1] != '/')
                    {
                        to_dir_ = to_dir_ + "/";
                    }
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"),
                        "moveFile ListObject for key: '{}' get key '{}'  filename: '{}' to_dir {}",
                        from_path,
                        obj_file.GetKey(),
                        file_name,to_dir_);
                    if(!exists(to_dir_)){
                        createDirectory(to_dir_);
                    }
                    copyCosFile(obj_file.GetKey(), to_dir_ + file_name);
                    removeCosFile(obj_file.GetKey());
                }
                else
                {
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"), "ListObject for key: '{}' get key '{}' key is a dir.", from_path, obj_file.GetKey());
                    moveFile(obj_file.GetKey(), to_path);
                }
            }
            auto common_prefix_list = response.GetResult().GetCommonPrefixes();
            for (auto iter = common_prefix_list.begin(); iter != common_prefix_list.end(); iter++)
            {
                auto common_prefix = (*iter);
                if (isFile(common_prefix.GetPrefix()) && exists(common_prefix.GetPrefix()))
                {
                    auto file_name = common_prefix.GetPrefix().substr(absolute_from_path.size(), common_prefix.GetPrefix().size());
                    String to_dir_ = to_path;
                    if (to_path[to_path.size() - 1] != '/')
                    {
                        to_dir_ = to_dir_ + "/";
                    }
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"),
                        "moveFile ListObject for key: '{}' get key '{}'  filename: '{}' to_dir {}",
                        from_path,
                        common_prefix.GetPrefix(),
                        file_name,to_dir_);
                    if(!exists(to_dir_)){
                        createDirectory(to_dir_);
                    }
                    copyCosFile(common_prefix.GetPrefix(), to_dir_ + file_name);
                    removeCosFile(common_prefix.GetPrefix());
                }
                else
                {
                    moveFile(common_prefix.GetPrefix(), to_path);
                }
            }
        } while (marker.size() > 0);
    }
    remove(from_path);
}

void DiskCOS::replaceFile(const String & from_path, const String & to_path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "replaceFile from_path: '{}' to_path '{}'.", from_path, to_path);
    copyFile(from_path, to_path);
}

void DiskCOS::copyFile(const String & from_path, const String & to_path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "copyFile from_path: '{}' to_path '{}'.", from_path, to_path);
    if (exists(to_path))
    {
        remove(to_path);
    }
    copyCosFile(from_path, to_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskCOS::readFile(const String & path, size_t buf_size, size_t, size_t, size_t) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "readFile : '{}'", makeCosKey(path));
    auto reader = std::make_unique<ReadBufferFromCos>(client, bucket, makeCosKey(path), buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), min_bytes_for_seek);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskCOS::writeFile(const String & path, size_t buf_size, WriteMode /*mode*/, size_t estimated_size, size_t)
{
    //bool exist = exists(path);
    /// Path to store new S3 object.
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "writeFile : '{}'", makeCosKey(path));
    bool is_multipart = estimated_size >= min_multi_part_upload_size;
    return std::make_unique<WriteBufferFromCos>(client, bucket, makeCosKey(path), is_multipart, min_upload_part_size, buf_size);
}

void DiskCOS::remove(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "remove : '{}'", makeCosKey(path));
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(makeCosKey(path));
    auto response = client->DeleteObject(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "DeleteObject fail path: '{}' err: '{}'.", path, err.GetMessage());
    }
}

void DiskCOS::removeRecursive(const String & path)
{

    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "removeRecursive : '{}'", makeCosKey(path));
    auto absolute_from_path = makeCosKey(path);
    if (isFile(path)&&exists(path))
    {
        removeCosFile(path);
    }
    else
    {
        String marker = "";
        do
        {
            Aws::S3::Model::ListObjectsRequest request;
            request.SetBucket(bucket);
            request.SetPrefix(absolute_from_path);
            request.SetMaxKeys(1000);
            request.SetDelimiter("/");
            request.SetMarker(marker);
            auto response = client->ListObjects(request);
            if (!response.IsSuccess())
            {
                const auto & err = response.GetError();
                LOG_ERROR(&Poco::Logger::get("DiskCOS"), "removeRecursive ListObject fail path: '{}' err: '{}'.", path, err.GetMessage());
                throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
            }
            marker = response.GetResult().GetNextMarker();
            auto object_list = response.GetResult().GetContents();
            LOG_DEBUG(
                &Poco::Logger::get("DiskCOS"),
                "removeRecursive ListObject for key: '{}' get file list size: '{}'.",
                path,
                object_list.size());
            for (auto iter = object_list.begin(); iter != object_list.end(); iter++)
            {
                auto obj_file = (*iter);
                if (obj_file.GetKey() == absolute_from_path)
                {
                    continue;
                }
                if (isFile(obj_file.GetKey()))
                {
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"), "removeRecursive ListObject for key: '{}' get key '{}'", path, obj_file.GetKey());
                    removeCosFile(obj_file.GetKey());
                }
                else
                {
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"),
                        "removeRecursive ListObject for key: '{}' get key '{}' key is a dir.",
                        path,
                        obj_file.GetKey());
                    removeRecursive(obj_file.GetKey());
                }
            }
            auto common_prefix_list = response.GetResult().GetCommonPrefixes();
            for (auto iter = common_prefix_list.begin(); iter != common_prefix_list.end(); iter++)
            {
                auto common_prefix = (*iter);
                if (isFile(common_prefix.GetPrefix()))
                {
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"),
                        "removeRecursive ListObject for key: '{}' get common_prefix_list '{}' key is a dir.",
                        path,
                        common_prefix.GetPrefix());
                    removeCosFile(common_prefix.GetPrefix());
                }
                else
                {
                    LOG_DEBUG(
                        &Poco::Logger::get("DiskCOS"),
                        "removeRecursive ListObject for key: '{}' get common_prefix_list '{}' key is a dir.",
                        path,
                        common_prefix.GetPrefix());
                    removeRecursive(common_prefix.GetPrefix());
                }
            }
        } while (marker.size() > 0);
    }
    remove(path);
}


void DiskCOS::listFiles(const String & path, std::vector<String> & file_names)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "listFiles : path '{}'", makeCosKey(path));
    for (auto it = iterateDirectory(makeCosKey(path)); it->isValid(); it->next())
        file_names.push_back(it->name());
}

void DiskCOS::setLastModified(const String & path, const Poco::Timestamp & /*timestamp*/)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "setLastModified : '{}'", makeCosKey(path));
}

Poco::Timestamp DiskCOS::getLastModified(const String & path)
{
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(makeCosKey(path));
    auto response = client->HeadObject(request);
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        if (err.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND)
        {
            DateTime now;
            LOG_ERROR(&Poco::Logger::get("DiskCOS"), "getLastModified path: '{}' not exsits time: {}", path, now.timestamp().epochTime());
            return now.timestamp();
        }
        LOG_ERROR(&Poco::Logger::get("DiskCOS"), "getLastModified HeadObject fail path: '{}' err: '{}'.", path, err.GetMessage());
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
    Poco::Timestamp last_modified=Poco::Timestamp(response.GetResult().GetLastModified().Millis()*1000);
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "getLastModified : '{}' .... time: {}", makeCosKey(path),last_modified.epochTime());
    return last_modified;
}

void DiskCOS::createHardLink(const String & src_path, const String & /*dst_path*/)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "createHardLink : '{}'", makeCosKey(src_path));
}

void DiskCOS::createFile(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "createFile : '{}'", makeCosKey(path));
    createDirectory(path);
}

void DiskCOS::setReadOnly(const String & path)
{
    // Poco::File(metadata_path + path).setReadOnly(true);
    LOG_DEBUG(&Poco::Logger::get("DiskCOS"), "setReadOnly : '{}'", makeCosKey(path));
}

}
