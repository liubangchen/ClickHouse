#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include "DiskCOS.h"
#include "Disks/DiskCacheWrapper.h"
#include "Disks/DiskFactory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
}

namespace
{
    void checkWriteAccess(IDisk & disk)
    {
        auto file = disk.writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        file->write("test", 4);
    }

    void checkReadAccess(const String & disk_name, IDisk & disk)
    {
        auto file = disk.readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
        String buf(4, '0');
        file->readStrict(buf.data(), 4);
        if (buf != "test")
            throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
    }

    void checkRemoveAccess(IDisk & disk) { disk.remove("test_acl"); }

}


void registerDiskCOS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      const Context & context) -> DiskPtr {
        Poco::File disk{context.getPath() + "disks/" + name};
        disk.createDirectories();

        Aws::Client::ClientConfiguration cfg;

        S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));
        if (uri.key.back() != '/')
            throw Exception("COS path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);

        cfg.connectTimeoutMs = config.getUInt(config_prefix + ".connect_timeout_ms", 10000);
        cfg.httpRequestTimeoutMs = config.getUInt(config_prefix + ".request_timeout_ms", 5000);
        cfg.endpointOverride = uri.endpoint;

        cfg.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(config.getUInt(config_prefix + ".retry_attempts", 10));

        auto client = S3::ClientFactory::instance().create(
            cfg,
            uri.is_virtual_hosted_style,
            config.getString(config_prefix + ".access_key_id", ""),
            config.getString(config_prefix + ".secret_access_key", ""),
            context.getRemoteHostFilter());

        //String metadata_path = config.getString(config_prefix + ".metadata_path", context.getPath() + "disks/" + name + "/");

        auto cosdisk = std::make_shared<DiskCOS>(
            name,
            client,
            uri.bucket,
            uri.key,
            context.getSettingsRef().s3_min_upload_part_size,
            config.getUInt64(config_prefix + ".min_multi_part_upload_size", 10 * 1024 * 1024),
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024));

        /// This code is used only to check access to the corresponding disk.
        if (!config.getBool(config_prefix + ".skip_access_check", false))
        {
            checkWriteAccess(*cosdisk);
            checkReadAccess(name, *cosdisk);
            checkRemoveAccess(*cosdisk);
        }

        return cosdisk;
    };
    factory.registerDiskType("cos", creator);
}

}
