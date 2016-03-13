#include <iostream>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include "lmdb.h"

namespace lmdb {

class Val {

private:
    void* own;
    MDB_val val;

public:
    Val() {
        own = NULL;
        val.mv_data = NULL;
        val.mv_size = 0;        
    }

    Val(Val& other) {
        own = NULL;
        val.mv_data = NULL;
        val.mv_size = 0;        
        set(other.val.mv_data, other.val.mv_size);
    }

    ~Val() {
        if(own) 
            free(own);
    }

    operator MDB_val* () { return &val; }

    long getSize() {
        return val.mv_size;
    }

    bool startsWith(Val& other) {
        return val.mv_size >= other.val.mv_size && memcmp(val.mv_data, other.val.mv_data, other.val.mv_size) == 0;
    }

    void set(std::string line, int beg, int end) {
        set(line.c_str() + beg, end - beg);
    }

    void set(const void* data, size_t size) {
        if(own) 
            free(own);
        if(size == 0) {
            own = NULL;
            val.mv_data = NULL;
            val.mv_size = 0;        
        } else {
            own = malloc(size);
            val.mv_data = own;
            val.mv_size = size;        
            if(!val.mv_data) {
                std::cout << "ERROR: malloc() failed." << std::endl;
                throw 0;
            }
            memcpy(val.mv_data, data, size);
        }
    }

    std::ostream& print(std::ostream& stm) const {
        static const char hexc[] = "0123456789ABCDEF";
        unsigned char* cur = (unsigned char*) val.mv_data;
        unsigned char* end = cur + val.mv_size;
        while (cur < end) {
            if (*cur >= ' ' && *cur <= 0x7F) {
                stm << *cur;
            } else {
                stm << "\\x";
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
    Env& env;
    unsigned int flags;
    MDB_txn* txn;

public: 
    Txn(Env& env, unsigned int flags) : env(env), flags(flags) { 
        rc = mdb_txn_begin(env, NULL, flags, &txn);
        if(rc)
            onError("mdb_txn_begin()");
    }

    ~Txn() { 
        if(txn)
            mdb_txn_abort(txn);
    }

    operator MDB_txn* () { return txn; }

    void commit_and_begin() {
	    rc = mdb_txn_commit(txn);
        txn = NULL;
        if(rc)
            onError("mdb_txn_commit()");

        rc = mdb_txn_begin(env, NULL, flags, &txn);
        if(rc)
            onError("mdb_txn_begin()");
    }
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

    bool get(lmdb::Val& key, lmdb::Val& data) {
        rc = mdb_get(txn, dbi, key, data);
        if(rc && rc != MDB_NOTFOUND) 
            onError("mdb_get()");
        return rc == 0;    
    }

    void put(lmdb::Val& key, lmdb::Val& data, unsigned int flags) {
        rc = mdb_put(txn, dbi, key, data, flags);
        if(rc) 
            onError("mdb_put()");
    }

    bool del(lmdb::Val& key) {
        rc = mdb_del(txn, dbi, key, NULL);
        if(rc && rc != MDB_NOTFOUND) 
            onError("mdb_del()");
        return rc == 0;    
    }
};

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

    void put(lmdb::Val& key, lmdb::Val& data, unsigned int flags) {
        rc = mdb_cursor_put(cursor, key, data, flags);
        if(rc) 
            onError("mdb_cursor_put()");
    }

    void del(unsigned int flags) {
        rc = mdb_cursor_del(cursor, flags);
        if(rc) 
            onError("mdb_cursor_del()");
    }
};

} // namespace lmdb

static volatile int signal_last;

static void signal_handler( int sig ) {
    signal_last=sig;
}

static void print_entry(lmdb::Val& key, lmdb::Val& data) {
    std::cout << '+' << key << '=' << data << std::endl;
}

static void run_get(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key, bool count) {
    lmdb::Val data;
    long num = 0;
    if (dbi.get(key, data)) {
        if(!count)
            print_entry(key, data);
        num++;
    } else {
        if(!count)
            std::cout << "> not found" << std::endl;
    }
    if(count)
        std::cout << "> " << num << std::endl;            
}

static void run_get_range(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& prefix, bool count) {
    lmdb::Cursor cursor(txn, dbi);
    MDB_cursor_op op = prefix.getSize() > 0 ? MDB_SET_RANGE : MDB_NEXT;
    lmdb::Val key(prefix), data;
    long num = 0;
    while(cursor.get(key, data, op) && key.startsWith(prefix)) {
        if(!count)
            print_entry(key, data);
        num++;
        op = MDB_NEXT;
    }
    if(count)
        std::cout << "> " << num << std::endl;            
}

static void run_put(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key, lmdb::Val& data) {
    dbi.put(key, data, 0);
}

static void run_remove(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key) {
    if (!dbi.del(key))
        std::cout << "> not found" << std::endl;
}

static void run_remove_range(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& prefix) {
    lmdb::Cursor cursor(txn, dbi);
    MDB_cursor_op op = prefix.getSize() > 0 ? MDB_SET_RANGE : MDB_NEXT;
    lmdb::Val key(prefix), data;
    while(cursor.get(key, data, op) && key.startsWith(prefix)) {
        cursor.del(0);
        op = MDB_NEXT;
    }
}

static void run_commit(lmdb::Txn& txn, lmdb::Dbi& dbi) {
    txn.commit_and_begin();
}

static void run_eval_print_loop(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi, bool verbose) {
    if(verbose) {
        std::cout << std::endl;
        std::cout << "> Syntax:" << std::endl;
        std::cout << ">     ?key          get" << std::endl;
        std::cout << ">     ?prefix*      get range" << std::endl;
        std::cout << ">     #prefix       find" << std::endl;
        std::cout << ">     #prefix*      count range" << std::endl;
        std::cout << ">     +key=data     put" << std::endl;
        std::cout << ">     -key          remove" << std::endl;
        std::cout << ">     -prefix*      remove range" << std::endl;
        std::cout << ">     !             commit" << std::endl;
        std::cout << "> Valid escape sequences are: \\r, \\n, \\t, \\=, \\*, \\_ and \\x00 ... \\xFF" << std::endl;           
        std::cout << "> Press Ctrl-C to quit" << std::endl;           
        std::cout << std::endl;
    }
    while(true) {
        if(verbose) {
            std::cout << "$ ";
        }
        std::string line;
        std::getline(std::cin, line);
        if (signal_last) {
            std::cout << "> Received signal " << signal_last << ", exitting." << std::endl;
            return;
        }
        
        char command = '\0';
        int pos_assign = -1;
        int pos_asterisk = -1;
        for (int i = 0; i < line.size(); i++) {
            char c = line[i];
            if(i == 0) {
                command = c;
            }
            if(c == '\\') {
                i++;
            }
            if(c == '=' && pos_assign == -1 && command == '+') {
                pos_assign = i;
            }
            if(c == '*' && pos_asterisk == -1 && (command == '?' || command == '#' || command == '-')) {
                pos_asterisk = i;
            }
        }
        if(pos_asterisk != -1 && pos_asterisk != line.size() - 1) {
            std::cout << "> ERROR: wildcard '*' must occur at end: " << line << std::endl;
            continue;
        }

        lmdb::Val key, data;
        if(command == '?' || command == '#' || command == '+' || command == '-') {
            key.set(line, 1, pos_assign != -1 ? pos_assign : pos_asterisk != -1 ? pos_asterisk : line.size());
            if(command == '+') {
                if(pos_assign == -1) {
                    std::cout << "> ERROR: missing '=': " << line << std::endl;
                    continue;
                }
                data.set(line, pos_assign+1, line.size());
            }
        } 
        if(command == '!') {
            if(line.size() != 1) {
                std::cout << "> ERROR: unexpeced data after '" << command << "': " << line << std::endl;
                continue;
            }
        }
        bool range = (pos_asterisk != -1);
        //std::cout << "> command '" << command << "': key='" << key << "', data='" << data << "', range=" << (range ? "yes" : "no") << std::endl;
        switch(command) {
            case '?':
                if(range) 
                    run_get_range(txn, dbi, key, false);
                else 
                    run_get(txn, dbi, key, false);
                break;  
            case '#':
                if(range) 
                    run_get_range(txn, dbi, key, true);
                else 
                    run_get(txn, dbi, key, true);
                break;
            case '+':
                run_put(txn, dbi, key, data);
                break;
            case '-':
                if(range) 
                    run_remove_range(txn, dbi, key);
                else 
                    run_remove(txn, dbi, key);
                break;
            case '!':
                run_commit(txn, dbi);
                break;
            case '>':
                break;
            default:
                std::cout << "> ERROR: unknown command: " << line << std::endl;
                break;
        }        
    }
}

static void print_entries(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi, bool verbose) {
    lmdb::Cursor cursor(txn, dbi);
    lmdb::Val key, data;
    while (cursor.get(key, data, MDB_NEXT)) {
        if (signal_last) {
            std::cout << "> Received signal " << signal_last << ", aborting." << std::endl;
            return;
        }
        print_entry(key, data);
    }
}

static void print_info(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi, bool verbose) {
    std::cout << "Using liblmdb version: " << MDB_VERSION_STRING << std::endl;

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

    std::cout << "Database interface flags: 0x" << std::hex << dbi.getFlags() << std::dec << std::endl;
}

static void print_usage() {
    std::cout << "lmdbsh -- LMDB Shell (Lightning memory-mapped database)" << std::endl;
    std::cout << std::endl;
    std::cout << "Usage:" << std::endl;
    std::cout << "    $ lmdbsh database [-rdonly] [-info] [-dump]" << std::endl;
    std::cout << std::endl;
}

typedef void (Handler)(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi, bool verbose);

int main(int argc, char** argv) {
    try {
        std::string envname;
        int env_flags = MDB_NOSUBDIR;
        int txn_flags = 0;
        int dbi_flags = 0;
        Handler* handler = run_eval_print_loop;
        for(int i = 1; i < argc; i++) {
            std::string arg(argv[i]);
            if(arg[0] != '-') {
                envname = arg;
            } else if(arg == "-rdonly") {
                txn_flags |= MDB_RDONLY;
            } else if(arg == "-info") {
                handler = print_info;
            } else if(arg == "-dump") {
                handler = print_entries;
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

        lmdb::Env env;

        env.open(envname, env_flags, 0664);

        lmdb::Txn txn(env, txn_flags);

        lmdb::Dbi dbi(env, txn, NULL, dbi_flags);
   
        (*handler)(env, txn, dbi, true);

        return 0;

    } catch(int rc) {
        std::cout << "Failing, error code " << rc << std::endl;
        return 1;
    }
}

