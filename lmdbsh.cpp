#include "lmdb-wrappers.h"
#include <iostream>
#include <signal.h>

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

