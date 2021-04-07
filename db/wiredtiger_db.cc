//
// Created by wujy on 1/23/19.
//
#include <iostream>
#include <sstream>

#include "wiredtiger_db.h"
#include "lib/coding.h"
#include "core/env.h"

#define MIN(a, b) a > b ? b : a

using namespace std;

namespace ycsbc {
    
    const uint32_t wt_cache_size = 1ul * 1024 * 1024; // 1MB

#ifdef __cplusplus
extern "C"{
#endif
    /* spdk_file_system begin */
    const size_t wt_gigabyte =  1073741824;
    const uint32_t wt_stream_sequence = 1;
    const uint32_t wt_stream_random = 2;
    const uint32_t wt_stream_no = 0;
    const uint32_t wt_stream_offset = 1024ul * 1024 * 3.2 * 1024;

    typedef struct {
        WT_FILE_SYSTEM iface;

        WT_EXTENSION_API *wtext; /* Extension functions */

        pthread_spinlock_t stream_id_lock;
        uint64_t g_virtual_stream_id;
    } SPDK_FILE_SYSTEM;

    typedef struct __wt_file_handle_spdk {
        WT_FILE_HANDLE iface;
        SPDK_FILE_SYSTEM *spdk_fs;

        void* fd;

        uint64_t stream_id;
    } WT_FILE_HANDLE_SPDK;

    int spdk_file_system_create(WT_CONNECTION *conn, WT_CONFIG_ARG *config);

    static bool
    byte_string_match(const char *str, const char *bytes, size_t len)
    {
        return (strncmp(str, bytes, len) == 0 && (str)[(len)] == '\0');
    }

    static int
    __spdk_fs_exist(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name, bool *existp)
    {
        int ret = 0;

        (void)(file_system);
        (void)(wt_session);

        ret = spdk_fs_exist(name);
        if (ret == 0) {
            *existp = true;
            return (0);
        } else {
            *existp = false;
            return (0);
        }
        // if (ret == ENOENT) {
        //     *existp = false;
        //     return (0);
        // }
    }

    /*
    * __wt_spdk_directory_list --
    *     Get a list of files from a directory, SPDK version.
    */
    int
    __wt_spdk_directory_list(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
    const char *directory, const char *prefix, char ***dirlistp, uint32_t *countp)
    {
        (void)(file_system);
        (void)(wt_session);
        return (
            spdk_fs_directory_list(directory, prefix, dirlistp, countp, false));
    }

    /*
    * __wt_spdk_directory_list_single --
    *     Get one file from a directory, SPDK version.
    */
    int
    __wt_spdk_directory_list_single(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session,
    const char *directory, const char *prefix, char ***dirlistp, uint32_t *countp)
    {
        (void)(file_system);
        (void)(wt_session);
        return (
            spdk_fs_directory_list(directory, prefix, dirlistp, countp, true));
    }

    /*
    * __wt_spdk_directory_list_free --
    *     Free memory returned by __wt_spdk_directory_list.
    */
    int
    __wt_spdk_directory_list_free(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, char **dirlist, uint32_t count)
    {
        (void)(file_system);
        (void)(wt_session);
        return (
            spdk_fs_directory_list_free(dirlist, count));
    }


    /*
    * __spdk_fs_remove --
    *     Remove a file.
    */
    static int
    __spdk_fs_remove(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name, uint32_t flags)
    {
        int ret = 0;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        spdk_fs = (SPDK_FILE_SYSTEM*)file_system;
        wtext = spdk_fs->wtext;

        ret = spdk_fs_remove(name);
        if (ret != 0)
            (void)wtext->err_printf(wtext, wt_session, 
                    "%s: file-remove: spdk remove", name);

        return (0);
    }

    /*
    * __spdk_fs_rename --
    *     Rename a file.
    */
    static int
    __spdk_fs_rename(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *from,
    const char *to, uint32_t flags)
    {
        int ret = 0;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        spdk_fs = (SPDK_FILE_SYSTEM*)file_system;
        wtext = spdk_fs->wtext;

        ret = spdk_fs_rename(from, to);
        if (ret != 0)
            (void)wtext->err_printf(wtext, wt_session, 
                    "%s to %s: file-rename: rename", from, to);

        return (0);
    }

    /*
    * __spdk_fs_size --
    *     Get the size of a file in bytes, by file name.
    */
    static int
    __spdk_fs_size(
    WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name, wt_off_t *sizep)
    {
        int ret = 0;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;
    
        spdk_fs = (SPDK_FILE_SYSTEM*)file_system;
        wtext = spdk_fs->wtext;

        ret = spdk_fs_size(name, sizep);
        if (ret == 0) {
            return (0);
        }
        (void)wtext->err_printf(wtext, wt_session, 
                    "%s: file-size: stat", name);
    }

    /*
    * __spdk_file_close --
    *     ANSI C close.
    */
    static int
    __spdk_file_close(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
    {
        int ret = 0;
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;

        /* Close the file handle. */
        if (pfh->fd != NULL) {
            ret = spdk_close_file((spdk_file_fd **)(&(pfh->fd)));
            if (ret != 0)
                (void)wtext->err_printf(
                    wtext, wt_session, "handle-close: %s", wtext->strerror(wtext, NULL, ret));
            pfh->fd = NULL;
        }

        free(file_handle->name);
        free(pfh);
        return (ret);
    }

    /*
    * __posix_file_lock --
    *     Lock/unlock a file.
    */
    static int
    __spdk_file_lock(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, bool lock)
    {
        /* Locks are always granted. */
        (void)file_handle; /* Unused */
        (void)wt_session;  /* Unused */
        (void)lock;        /* Unused */
        return (0);
    }

    /*
    * __spdk_file_read --
    *     POSIX pread.
    */
    static int
    __spdk_file_read(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, void *buf)
    {
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;
        size_t chunk;
        int64_t nr;
        uint8_t *addr;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;


        /* Break reads larger than 1GB into 1GB chunks. */
        /*
        for (addr = (uint8_t*)buf; len > 0; addr += nr, len -= (size_t)nr, offset += nr) {
            chunk = MIN(len, wt_gigabyte);
            if ((nr = spdk_read_file((spdk_file_fd*)pfh->fd, offset, chunk, addr)) <= 0)
                (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-read: pread: failed to read %u bytes at offset %" PRIuMAX,
                    file_handle->name, chunk, (uintmax_t)offset);
        }
        */
       if ((spdk_read_file((spdk_file_fd*)pfh->fd, offset, len, buf)) <= 0){
           (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-read: pread: failed to read %u bytes at offset %" PRIuMAX,
                    file_handle->name, len, (uintmax_t)offset);
            assert(0);
       }
                
        return (0);
    }

    /*
    * __spdk_file_size --
    *     Get the size of a file in bytes, by file handle.
    */
    static int
    __spdk_file_size(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t *sizep)
    {
        int ret = 0;
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;

        ret = spdk_size_file((spdk_file_fd*)pfh->fd, sizep);
        if (ret == 0) {
            return (0);
        }
        (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-size: fstat", file_handle->name);
    }

    /*
    * __spdk_file_sync --
    *     POSIX fsync.
    */
    static int
    __spdk_file_sync(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session)
    {
        int ret = 0;
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;

        ret = spdk_sync_file((spdk_file_fd*)pfh->fd);
        if(ret == 0)
            return 0;
        (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-sync: fsync", file_handle->name);
        return -1;
    }

    /*
    * __spdk_file_truncate --
    *     POSIX ftruncate.
    */
    static int
    __spdk_file_truncate(WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t len)
    {
        int ret = 0;
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;

        ret = spdk_truncate_file((spdk_file_fd*)pfh->fd, len);
        if(ret == 0)
            return (0);
        (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-truncate: ftruncate", file_handle->name);
        return -1;
    }

    /*
    * __spdk_file_write --
    *     POSIX pwrite.
    */
    static int
    __spdk_file_write(
    WT_FILE_HANDLE *file_handle, WT_SESSION *wt_session, wt_off_t offset, size_t len, const void *buf, uint32_t flags)
    {
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;
        size_t chunk;
        ssize_t nw;
        const uint8_t *addr;
        uint64_t stream_id;

        pfh = (WT_FILE_HANDLE_SPDK *)file_handle;
        spdk_fs = pfh->spdk_fs;
        wtext = spdk_fs->wtext;

        if(flags & wt_stream_sequence) stream_id = 0;
        else if (flags & wt_stream_random) {
            stream_id = offset > wt_stream_offset ? 2 : 1;
        } else {
            stream_id = pfh->stream_id;
        }

        /* Break writes larger than 1GB into 1GB chunks. */
        // for (addr = (uint8_t*)buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
        //     chunk = MIN(len, wt_gigabyte);
        //     if ((nw = spdk_write_file((spdk_file_fd*)pfh->fd, offset, chunk, addr/*, stream_id*/)) < 0)
        //         (void)wtext->err_printf(wtext, wt_session, 
        //             "%s: handle-write: pwrite: failed to write %u bytes at offset %" PRIuMAX,
        //             file_handle->name, chunk, (uintmax_t)offset);
        // }

        if (spdk_write_file((spdk_file_fd*)pfh->fd, offset, len, const_cast<void *>(buf)/*, stream_id*/) != 0)
                (void)wtext->err_printf(wtext, wt_session, 
                    "%s: handle-write: pwrite: failed to write %u bytes at offset %" PRIuMAX,
                    file_handle->name, len, (uintmax_t)offset);
                    
        return (0);
    }

    /*
    * __spdk_open_file --
    *     Open a file handle.
    */
    static int
    __spdk_open_file(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session, const char *name,
    WT_FS_OPEN_FILE_TYPE file_type, uint32_t flags, WT_FILE_HANDLE **file_handlep)
    {
        int ret = 0;
        WT_FILE_HANDLE *file_handle;
        WT_FILE_HANDLE_SPDK *pfh;
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_EXTENSION_API *wtext;

        spdk_fs = (SPDK_FILE_SYSTEM *)file_system;
        wtext = spdk_fs->wtext;
        file_handle = NULL;
        *file_handlep = NULL;

        if((pfh = (WT_FILE_HANDLE_SPDK*)calloc(1, sizeof(WT_FILE_HANDLE_SPDK))) == NULL) {
            (void)wtext->err_printf(
                wtext, wt_session, "open file: %s", wtext->strerror(wtext, NULL, ENOMEM));
            return (ENOMEM);
        }

        pfh->spdk_fs = spdk_fs;

        // int file_type_ = file_type == WT_FS_OPEN_FILE_TYPE_DIRECTORY ? 0 : 1;
        ret = spdk_open_file((spdk_file_fd**)(&(pfh->fd)), name, 1);
        if (ret != 0){
            (void)wtext->err_printf(
            wtext, wt_session, "open file: %s", wtext->strerror(wtext, NULL, ret));
            goto err;
        }

        // TODO: retain fd in memory
        pthread_spin_lock(&spdk_fs->stream_id_lock);
        pfh->stream_id = spdk_fs->g_virtual_stream_id;
        ++spdk_fs->g_virtual_stream_id;
        pthread_spin_unlock(&spdk_fs->stream_id_lock);

        /* Initialize public information. */
        file_handle = (WT_FILE_HANDLE *)pfh;
        file_handle->name = strdup(name);

        file_handle->close = __spdk_file_close;
        file_handle->fh_extend = NULL;
        file_handle->fh_lock = __spdk_file_lock;
        file_handle->fh_map = NULL;
        file_handle->fh_unmap = NULL;
        file_handle->fh_read = __spdk_file_read;
        file_handle->fh_size = __spdk_file_size;
        file_handle->fh_sync = __spdk_file_sync;
        file_handle->fh_truncate = __spdk_file_truncate;
        file_handle->fh_write = __spdk_file_write;

        *file_handlep = file_handle;

        return (0);

    err:
        ret = __spdk_file_close((WT_FILE_HANDLE *)pfh, wt_session);
        return (ret);
    }

    /*
    * __spdk_terminate --
    *     Terminate a POSIX configuration.
    */
    static int
    __spdk_terminate(WT_FILE_SYSTEM *file_system, WT_SESSION *wt_session)
    {
        (void)file_system;
        (void)wt_session;

        spdk_close_env();
        
        free(file_system);

        return (0);
    }

    // const char *FLAGS_spdk_dir;   //TODO
    const char *FLAGS_spdk_conf = "/usr/local/etc/spdk/wiredtiger.json";  
    const char *FLAGS_spdk_bdev = "Nvme0n1";
    const uint64_t FLAGS_spdk_cache_size = 4096;

    int
    spdk_file_system_create(WT_CONNECTION *conn, WT_CONFIG_ARG *config)
    {
        SPDK_FILE_SYSTEM *spdk_fs;
        WT_CONFIG_ITEM k, v;
        WT_CONFIG_PARSER *config_parser;
        WT_EXTENSION_API *wtext;
        WT_FILE_SYSTEM *file_system;
        int ret = 0;

        wtext = conn->get_extension_api(conn);

        if ((spdk_fs = (SPDK_FILE_SYSTEM*)calloc(1, sizeof(SPDK_FILE_SYSTEM))) == NULL) {
            (void)wtext->err_printf(
            wtext, NULL, "spdk_file_system_create: %s", wtext->strerror(wtext, NULL, ENOMEM));
            return (ENOMEM);
        }

        spdk_init_env(FLAGS_spdk_conf, FLAGS_spdk_bdev, FLAGS_spdk_cache_size);

        pthread_spin_init(&spdk_fs->stream_id_lock, 0);
        spdk_fs->wtext = wtext;
        spdk_fs->g_virtual_stream_id = 3;

        file_system = (WT_FILE_SYSTEM *)spdk_fs;

        /*
        * Applications may have their own configuration information to pass to the underlying
        * filesystem implementation. See the main function for the setup of those configuration
        * strings; here we parse configuration information as passed in by main, through WiredTiger.
        */
        if ((ret = wtext->config_parser_open_arg(wtext, NULL, config, &config_parser)) != 0) {
            (void)wtext->err_printf(wtext, NULL, "WT_EXTENSION_API.config_parser_open: config: %s",
            wtext->strerror(wtext, NULL, ret));
            goto err;
        }

        /* Step through our configuration values. */
        printf("Custom file system configuration\n");
        while ((ret = config_parser->next(config_parser, &k, &v)) == 0) {
            if (byte_string_match("config_string", k.str, k.len)) {
                printf(
                "\t"
                "key %.*s=\"%.*s\"\n",
                (int)k.len, k.str, (int)v.len, v.str);
                continue;
            }
            if (byte_string_match("config_value", k.str, k.len)) {
                printf(
                "\t"
                "key %.*s=%" PRId64 "\n",
                (int)k.len, k.str, v.val);
                continue;
            }
            ret = EINVAL;
            (void)wtext->err_printf(wtext, NULL,
            "WT_CONFIG_PARSER.next: unexpected configuration "
            "information: %.*s=%.*s: %s",
            (int)k.len, k.str, (int)v.len, v.str, wtext->strerror(wtext, NULL, ret));
            goto err;
        }

        /* Check for expected parser termination and close the parser. */
        if (ret != WT_NOTFOUND) {
            (void)wtext->err_printf(
            wtext, NULL, "WT_CONFIG_PARSER.next: config: %s", wtext->strerror(wtext, NULL, ret));
            goto err;
        }
        if ((ret = config_parser->close(config_parser)) != 0) {
            (void)wtext->err_printf(
            wtext, NULL, "WT_CONFIG_PARSER.close: config: %s", wtext->strerror(wtext, NULL, ret));
            goto err;
        }

        /* Initialize the in-memory jump table. */
        file_system->fs_directory_list = __wt_spdk_directory_list;
        file_system->fs_directory_list_single = __wt_spdk_directory_list_single;
        file_system->fs_directory_list_free = __wt_spdk_directory_list_free;
        file_system->fs_exist = __spdk_fs_exist;
        file_system->fs_open_file = __spdk_open_file;
        file_system->fs_remove = __spdk_fs_remove;
        file_system->fs_rename = __spdk_fs_rename;
        file_system->fs_size = __spdk_fs_size;
        file_system->terminate = __spdk_terminate;

        if ((ret = conn->set_file_system(conn, file_system, NULL)) != 0) {
            (void)wtext->err_printf(
            wtext, NULL, "WT_CONNECTION.set_file_system: %s", wtext->strerror(wtext, NULL, ret));
            goto err;
        }
        return (0);

    err:
        spdk_close_env();
        pthread_spin_destroy(&spdk_fs->stream_id_lock);
        free(spdk_fs);
        /* An error installing the file system is fatal. */
        exit(1);
    }
    /* spdk_file_system end */
#ifdef __cplusplus
}
#endif

    WiredTiger::WiredTiger(const char *home, utils::Properties &props) :noResult(0){
        WT_SESSION *session;
        //set option
        /* Open a connection to the database, creating it if necessary. */
        wiredtiger_open(home, NULL, SetConnOptions(props).c_str(), &conn_);
        assert(conn_ != NULL);

        /* Open a session handle for the database. */
        conn_->open_session(conn_, NULL, NULL, &session);
        assert(session != NULL);

        int ret = session->create(session, uri_.c_str(), SetCreateOptions(props).c_str());
        if (ret != 0) {
            fprintf(stderr, "create error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        session->close(session, NULL);

        session_nums_ = stoi(props.GetProperty("threadcount", "1"));
        session_ = new WT_SESSION *[session_nums_];
        for (int i = 0; i < session_nums_; i++){
            conn_->open_session(conn_, NULL, NULL, &(session_[i]));
            assert(session_[i] != NULL);
        }
    }

    std::string WiredTiger::SetConnOptions(utils::Properties &props) {
        //
        uint64_t nums = stoi(props.GetProperty(CoreWorkload::RECORD_COUNT_PROPERTY));
        uint32_t key_len = stoi(props.GetProperty(CoreWorkload::KEY_LENGTH));
        uint32_t value_len = stoi(props.GetProperty(CoreWorkload::FIELD_LENGTH_PROPERTY));
        uint32_t cache_size = nums * (key_len + value_len) * 10 / 100;

        std::stringstream conn_config;
        conn_config.str("");

        conn_config << "create";

        // string direct_io = ",direct_io=[data]"; // [data, log, checkpoint]
        // conn_config += direct_io;

        conn_config << ",cache_size=";
        conn_config << (cache_size > wt_cache_size ? cache_size : wt_cache_size);

        conn_config << ",eviction=(threads_max=4)";
        conn_config << ",transaction_sync=(enabled=true)";

        // string checkpoint = ",checkpoint=(wait=60,log_size=2G)";
        // conn_config += checkpoint;

        // string extensions = ",extensions=[/usr/local/lib/libwiredtiger_snappy.so]";
        // conn_config += extensions;

        // string log = ",log=(archive=false,enabled=true,path=journal,compressor=snappy)"; // compressor = "lz4", "snappy", "zlib" or "zstd" // 需要重新Configuring WiredTiger
        conn_config << ",log=(enabled=true,file_max=128MB)";

        conn_config << ",extensions=(local={"
            "entry=spdk_file_system_create,early_load=true,"
            "config={config_string=\"spdk-file-system\",config_value=37}"
            "})";

        conn_config << ",statistics=(all)"; // all, fast
        cout << "Connect Config: " << conn_config.str() << endl;
        return conn_config.str();
    }

    std::string WiredTiger::SetCreateOptions(utils::Properties &props) {
        //
        uri_ = "table:ycsb_wiredtiger" ; //"lsm:ycsb_wiredtiger"
        std::stringstream config;
        config.str("");

        config << "key_format=S,value_format=S";
        config << ",prefix_compression=false";
        config << ",checksum=on";
        
        config << ",internal_page_max=16kb";
        config << ",leaf_page_max=16kb";
        config << ",memory_page_max=10MB";

        // config << ",lsm=(";
        // config << ",chunk_size=20MB";
        // config << ",bloom_bit_count=10"
        // config << ")";

        cout << "Create Config: " << config.str() << endl;
        return config.str();
    }

    // int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
    //                   std::vector<KVPair> &result) {return DB::kOK;}
    int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        ret = cursor->search(cursor);
        cursor->close(cursor);
        if(!ret){
            return DB::kOK;
        }else if(ret == WT_NOTFOUND){
            noResult++;
            return DB::kOK;
        }else {
            cerr<<"read error"<<endl;
            exit(0);
        }
    }

    // int WiredTiger::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
    //                   std::vector<std::vector<KVPair>> &result) {return DB::kOK;}
    int WiredTiger::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        int i = 0;
        while (i < len && cursor->next(cursor) == 0) {
            i++;
            const char *key1;
            cursor->get_key(cursor, &key1);
            string key2 = key1;
            if(key2 >= key) break;
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    // int WiredTiger::Insert(const std::string &table, const std::string &key,
    //                     std::vector<KVPair> &values) {return DB::kOK;}
    int WiredTiger::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values, int nums){
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }

        string value = values.at(0).second;

        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        ret = cursor->insert(cursor);
        if (ret != 0) {
          fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
          exit(1);
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    // int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    //     return Insert(table,key,values);
    // }
    int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values, int nums) {
        return Insert(table,key,values,nums);
    }

    // int WiredTiger::Delete(const std::string &table, const std::string &key) {return DB::kOK;}
    int WiredTiger::Delete(const std::string &table, const std::string &key, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        if ((ret = cursor->remove(cursor)) != 0) {
          if (nums == 1 || ret != WT_NOTFOUND) {
            fprintf(stderr, "del error: key %s %s\n", key.c_str(), wiredtiger_strerror(ret));
            exit(1);
          }
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    inline void print_cursor(WT_CURSOR *cursor)
    {
        const char *desc, *pvalue;
        int64_t value;

        while (cursor->next(cursor) == 0 &&
            cursor->get_value(cursor, &desc, &pvalue, &value) == 0){
            if (value != 0)
                printf("%s=%s\n", desc, pvalue);
        }
    }

    void WiredTiger::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        /*
        char stats[1024];
        memset(stats, 0, 1024);
        // statistics must be set
        WT_CURSOR *cursor;
        std::stringstream suri;
        suri.str("");
        suri << "statistics:session" << uri_;
        for(int i = 0; i < session_nums_; i++){
            int ret = session_[i]->open_cursor(session_[i], suri.str().c_str(), NULL, "statistics=(all,clear)", &cursor);
            if (ret != 0) {
                fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
                exit(1);
            }
            printf("------ Session %d stats ------", i);
            print_cursor(cursor);   
        }
        cout<<stats<<endl;
        */
    }


    WiredTiger::~WiredTiger() {
        for(int i = 0; i < session_nums_; i++) {
            session_[i]->close(session_[i], NULL);
        }
        conn_->close(conn_, NULL);
        spdk_close_env();
    }

    void WiredTiger::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void WiredTiger::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
