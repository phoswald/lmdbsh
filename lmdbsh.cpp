#include <iostream>
#include <string>
#include <signal.h>
#include "lmdb.h"

namespace lmdb {

class RetCode {

protected:
    int rc;

protected:
    RetCode() { rc = 0; }

    void onError(const char* method) {
        std::cout << "ERROR: " << method << " failed with code " << rc << ": " << mdb_strerror(rc) << std::endl;
        throw rc;
    }
};

class Env : RetCode {

private:
    MDB_env* env;

public:
    Env() {
        rc = mdb_env_create(&env);
        if(rc)
            onError("mdb_env_create()");

        //  mdb_env_set_maxdbs(env, 2);

        //    if (info.me_maxreaders)
        //        mdb_env_set_maxreaders(env, info.me_maxreaders);

        //    if (info.me_mapsize)
        //        mdb_env_set_mapsize(env, info.me_mapsize);

        //    if (info.me_mapaddr)
        //        envflags |= MDB_FIXEDMAP;
    }

    ~Env() {
        mdb_env_close(env);
    }

    operator MDB_env* () { return env; }

    void open(std::string& name, int flags, int mode) {
        rc = mdb_env_open(env, name.c_str(), flags, mode);
        if(rc)
            onError("mdb_env_open()");
    }

    unsigned int getFlags() {
        unsigned int flags;
        rc = mdb_env_get_flags(env, &flags);
        if(rc)
            onError("mdb_env_get_flags()");
        return flags;
    }

    MDB_stat getStat() {
        MDB_stat stat;
        rc = mdb_env_stat(env, &stat);
        if(rc)
            onError("mdb_env_dstat()");
        return stat;
    }

    MDB_envinfo getInfo() {
        MDB_envinfo info;
        rc = mdb_env_info(env, &info);
        if(rc)
            onError("mdb_env_info()");
        return info;
    }
};

class Txn : RetCode {

private:
    MDB_txn* txn;

public: 
    Txn(Env& env, unsigned int flags) { 
        rc = mdb_txn_begin(env, NULL, flags, &txn);
        if(rc)
            onError("mdb_txn_begin()");
    }

    ~Txn() { 
        mdb_txn_abort(txn);
    }

    operator MDB_txn* () { return txn; }
};

class Dbi : RetCode {

private:
    Env& env;
    Txn& txn;
    MDB_dbi dbi;

public: 
    Dbi(Env& env, Txn& txn, const char *name, unsigned int flags) : env(env), txn(txn) {
        rc = mdb_dbi_open(txn, name, flags, &dbi);
        if(rc)
            onError("mdb_dbi_open()");
    }

    ~Dbi() {
        mdb_dbi_close(env, dbi);
    }

    operator MDB_dbi& () { return dbi; }

    unsigned int getFlags() {
        unsigned int flags;
        rc = mdb_dbi_flags(txn, dbi, &flags);
        if(rc)
            onError("mdb_dbi_flags()");
        return flags;
    }
};

class Val {

private:
    MDB_val val;

public:
    operator MDB_val* () { return &val; }

    std::ostream& print(std::ostream& stm) const {
        static const char hexc[] = "0123456789ABCDEF";
        unsigned char* cur = (unsigned char*) val.mv_data;
        unsigned char* end = cur + val.mv_size;
        while (cur < end) {
            if (*cur >= ' ' && *cur <= 0x7F) {
                stm << *cur;
            } else {
                stm << '\\';
                stm << hexc[*cur >> 4];
                stm << hexc[*cur & 0xF];
            }
            cur++;
        }
        return stm;
    }
};

std::ostream& operator << (std::ostream& stm, const Val& val) {
    return val.print(stm);
}

class Cursor : RetCode {

private:
    MDB_cursor* cursor;

public:
    Cursor(Txn& txn, Dbi& dbi) {
        rc = mdb_cursor_open(txn, dbi, &cursor);
        if(rc)
            onError("mdb_cursor_open()");
    }

    ~Cursor() {
        mdb_cursor_close(cursor);
    }

    operator MDB_cursor* () { return cursor; }

    bool get(lmdb::Val& key, lmdb::Val& data, MDB_cursor_op op) {
        rc = mdb_cursor_get(cursor, key, data, op);
        if(rc && rc != MDB_NOTFOUND) 
            onError("mdb_cursor_get()");
        return rc == 0;    
    }
};

} // namespace lmdb

static volatile int signal_last;

static void signal_handler( int sig ) {
    signal_last=sig;
}

static void print_env_info(lmdb::Env& env) {
    std::cout << "Environment flags: 0x" << std::hex << env.getFlags() << std::dec << std::endl;

    MDB_stat stat = env.getStat();
    std::cout << "Size of a database page: " << stat.ms_psize << std::endl;
    std::cout << "Depth (height) of the B-tree: " << stat.ms_depth << std::endl;
    std::cout << "Number of internal (non-leaf) pages: " << stat.ms_branch_pages << std::endl;
    std::cout << "Number of leaf pages: " << stat.ms_leaf_pages << std::endl;
    std::cout << "Number of overflow pages: " << stat.ms_overflow_pages << std::endl;
    std::cout << "Number of data items: " << stat.ms_entries << std::endl;

    MDB_envinfo info = env.getInfo();
    std::cout << "Address of data memory map, if fixed: " << info.me_mapaddr << std::endl;
    std::cout << "Size of the data memory map: " << (info.me_mapsize >> 10) << " KB" << std::endl;
    std::cout << "ID of the last used page: " << info.me_last_pgno << std::endl;
    std::cout << "ID of the last committed transaction: " << info.me_last_txnid << std::endl;
    std::cout << "max reader slots in the environment: " << info.me_maxreaders << std::endl;
    std::cout << "max reader slots used in the environment: " << info.me_numreaders << std::endl;
}

static void print_dbi_info(lmdb::Dbi& dbi) {
    std::cout << "Database interface flags: 0x" << std::hex << dbi.getFlags() << std::dec << std::endl;
}

static void print_all_entries(lmdb::Txn& txn, lmdb::Dbi& dbi)
{
    lmdb::Cursor cursor(txn, dbi);
    lmdb::Val key, data;
    while (cursor.get(key, data, MDB_NEXT)) {
        if (signal_last) {
            std::cout << "Received signal " << signal_last << ", aborting." << std::endl;
            return;
        }
        std::cout << key << '=' << data << '\n';
    }
}

static void print_usage() {
    std::cout << "lmdbsh -- LMDB Shell (Lightning memory-mapped database)" << std::endl;
    std::cout << "Usage: $ lmdbsh [-info] [-rdonly] [-dump] database" << std::endl;
}

enum Command {
    REPL,
    DUMP
};

int main(int argc, char** argv) {
    try {
        bool info = false;
        int env_flags = MDB_NOSUBDIR;
        int txn_flags = 0;
        int dbi_flags = 0;
        std::string envname;
        Command command = REPL;
        for(int i = 1; i < argc; i++) {
            std::string arg(argv[i]);
            if(arg == "-info") {
                info = true;
            } else if(arg == "-rdonly") {
                txn_flags |= MDB_RDONLY;
            } else if(arg == "-dump") {
                command = DUMP;
            } else if(arg[0] != '-') {
                envname = arg;
            } else {
                std::cout << "Invalid argument: " << arg << std::endl << std::endl;
                print_usage();
                return 1;
            }
        }
        if(envname.empty()) {
            print_usage();
            return 1;
        }

        signal(SIGPIPE, signal_handler);
        signal(SIGHUP, signal_handler);
        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);

        if(info) {
            std::cout << "Using liblmdb version: " << MDB_VERSION_STRING << std::endl;
        }

        lmdb::Env env;

        env.open(envname, env_flags, 0664);

        lmdb::Txn txn(env, txn_flags);

        lmdb::Dbi dbi(env, txn, NULL, dbi_flags);
   
        if(info) {
            print_env_info(env);
            print_dbi_info(dbi);
        }

        switch(command) {
            case REPL:
                break;
            case DUMP:
                print_all_entries(txn, dbi);
                break;
        }

        return 0;

    } catch(int rc) {
        std::cout << "Failing, error code " << rc << std::endl;
        return 1;
    }
}

