#include "lmdb.h"
#include <string>

namespace lmdb {

class Val {

private:
    void* own;
    MDB_val val;

public:
    Val();

    Val(Val& other);

    ~Val();

    operator MDB_val* () { return &val; }

    long getSize() {
        return val.mv_size;
    }

    bool startsWith(Val& other);

    void set(std::string line, int beg, int end);

    void set(const void* data, size_t size);

    std::ostream& print(std::ostream& stm) const;
};

inline std::ostream& operator << (std::ostream& stm, const Val& val) {
    return val.print(stm);
}

class RetCode {

protected:
    int rc;

protected:
    RetCode() { rc = 0; }

    void onError(const char* method);
};

class Env : RetCode {

private:
    MDB_env* env;

public:
    Env() {
        rc = mdb_env_create(&env);
        if(rc)
            onError("mdb_env_create()");
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

