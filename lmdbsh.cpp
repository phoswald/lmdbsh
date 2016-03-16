#include "lmdb-wrappers.h"
#include <iostream>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>

static volatile int signal_last;

static void signal_handler( int sig ) {
    signal_last=sig;
}

static void print_entry(lmdb::Val& key, lmdb::Val& data) {
    std::cout << '+' << key << ':' << data << std::endl;
}

static void run_get(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key, bool count) {
    lmdb::Val data;
    long num = 0;
    if(key.getSize() > 0 && dbi.get(key, data)) {
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
        if(signal_last) {
            std::cout << "> received signal " << signal_last << ", aborting." << std::endl;
            return;
        }
        if(!count)
            print_entry(key, data);
        num++;
        op = MDB_NEXT;
    }
    if(count)
        std::cout << "> " << num << std::endl;            
}

static void run_put(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key, lmdb::Val& data) {
    if(key.getSize() > 0)
        dbi.put(key, data, 0);
    else 
        std::cout << "> not possible (empty key)" << std::endl;
}

static void run_remove(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& key) {
    if(key.getSize() == 0 || !dbi.del(key))
        std::cout << "> not found" << std::endl;
}

static void run_remove_range(lmdb::Txn& txn, lmdb::Dbi& dbi, lmdb::Val& prefix) {
    lmdb::Cursor cursor(txn, dbi);
    MDB_cursor_op op = prefix.getSize() > 0 ? MDB_SET_RANGE : MDB_NEXT;
    lmdb::Val key(prefix), data;
    while(cursor.get(key, data, op) && key.startsWith(prefix)) {
        if(signal_last) {
            std::cout << "> received signal " << signal_last << ", aborting." << std::endl;
            return;
        }
        cursor.del(0);
        op = MDB_NEXT;
    }
}

static void run_commit(lmdb::Txn& txn, lmdb::Dbi& dbi) {
    txn.commit_and_begin();
    std::cout << "> commited" << std::endl;
}

static bool parse_hex(char c, int& n) {
    if(c >= '0' && c <= '9') {
        n = c - '0';
        return true;
    } else if(c >= 'A' && c <= 'F') {
        n = c - 'A' + 10;
        return true;
    } else if(c >= 'a' && c <= 'f') {
        n = c - 'a' + 10;
        return true;
    } else {
        return false;
    }
}

static bool parse_char(const std::string& line, int& pos, char c) {
    if(pos < line.size() && line[pos] == c) {
        pos++;
        return true;
    }
    return false;
}

static bool parse_eol(const std::string& line, int pos) {
    if(pos == line.size()) {
        return true;
    } else {
        std::cerr << "ERROR: unexepcted characters after position " << pos << " of line '" << line << "'." << std::endl;
        return false;
    }
}

static bool parse_value(const std::string& line, int& pos, std::vector<char>& chars) {
    while(pos < line.size()) {
        char c = line[pos];
        if(c == ':' || c == '*') {
            break;
        }
        if(c == '\\') {
            pos++;
            if(pos < line.size()) {
                c = line[pos];
                switch(c) {
                    case 't':  c = '\t'; break;
                    case 'r':  c = '\r'; break;
                    case 'n':  c = '\n'; break;
                    case '\\': c = '\\'; break;
                    case '*':  c = '*';  break;
                    case ':':  c = ':';  break;
                    case 'x':  {
                        if(pos + 2 < line.size()) {
                            int hi = 0, lo = 0;
                            if(parse_hex(line[pos+1], hi) && parse_hex(line[pos+2], lo)) {
                                pos+=2;
                                c = (hi << 4) + lo;
                                break;
                            }
                        }
                        // intentionally no break here
                    }
                    default:
                        std::cerr << "ERROR: invalid escape sequence one line '" << line << "'." << std::endl;
                        return false;
                }                
            }
        }
        pos++;
        chars.push_back(c);
    }
    return true;
}

static bool parse_line(const std::string& line, char& command, std::vector<char>& key, std::vector<char>& data, bool& range) {
    int pos = 0;
    if(parse_char(line, pos, '?')) {
        if(!parse_value(line, pos, key)) {
            std::cerr << "ERROR: invalid '?'key['*'] command on line '" << line << "'." << std::endl;
            return false;            
        }
        if(parse_char(line, pos, '*')) {
           range = true; 
        }
        command = '?';
        return parse_eol(line, pos);
    }
    if(parse_char(line, pos, '#')) {
        if(!parse_value(line, pos, key)) {
            std::cerr << "ERROR: invalid '#'key['*'] command on line '" << line << "'." << std::endl;
            return false;            
        }
        if(parse_char(line, pos, '*')) {
           range = true; 
        }
        command = '#';
        return parse_eol(line, pos);
    }
    if(parse_char(line, pos, '+')) {
        if(!parse_value(line, pos, key) || !parse_char(line, pos, ':') || !parse_value(line, pos, data)) {
            std::cerr << "ERROR: invalid '+'key':'data command on line '" << line << "'." << std::endl;
            return false;            
        }
        command = '+';
        return parse_eol(line, pos);
    }
    if(parse_char(line, pos, '-')) {
        if(!parse_value(line, pos, key)) {
            std::cerr << "ERROR: invalid '-'key['*'] command on line '" << line << "'." << std::endl;
            return false;            
        }
        if(parse_char(line, pos, '*')) {
           range = true; 
        }
        command = '-';
        return parse_eol(line, pos);
    }
    if(parse_char(line, pos, '!')) {
        command = '!';        
        return parse_eol(line, pos);
    }
    if(parse_char(line, pos, '>') || line.empty()) {
        return true;
    }
    std::cerr << "ERROR: unknown command on line '" << line << "'." << std::endl;
    return false;
}

static void run_eval_print_loop(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi) {
    bool interactive = isatty(fileno(stdin));
    if(interactive) {
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
    while(!std::cin.eof()) {
        if(interactive) {
            std::cout << "$ ";
        }
        std::string line;
        std::getline(std::cin, line);
        if(signal_last) {
            std::cout << "> received signal " << signal_last << ", exitting." << std::endl;
            return;
        }
        
        char command = '\0';
        std::vector<char> key;
        std::vector<char> data;
        bool range = false;
        if(parse_line(line, command, key, data, range)) {
            lmdb::Val keyVal(key);
            lmdb::Val dataVal(data);
            // std::cout << "> command '" << command << "': key='" << keyVal << "', data='" << dataVal << "', range=" << (range ? "yes" : "no") << std::endl;
            switch(command) {
                case '?':
                    if(range) 
                        run_get_range(txn, dbi, keyVal, false);
                    else 
                        run_get(txn, dbi, keyVal, false);
                    break;  
                case '#':
                    if(range) 
                        run_get_range(txn, dbi, keyVal, true);
                    else 
                        run_get(txn, dbi, keyVal, true);
                    break;
                case '+':
                    run_put(txn, dbi, keyVal, dataVal);
                    break;
                case '-':
                    if(range) 
                        run_remove_range(txn, dbi, keyVal);
                    else 
                        run_remove(txn, dbi, keyVal);
                    break;
                case '!':
                    run_commit(txn, dbi);
                    break;
            }        
        }
    }
}

static void print_entries(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi) {
    lmdb::Val key;
    run_get_range(txn, dbi, key, false);
}

static void print_info(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi) {
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

typedef void (Handler)(lmdb::Env& env, lmdb::Txn& txn, lmdb::Dbi& dbi);

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
                std::cerr << "ERROR: Invalid argument: " << arg << std::endl << std::endl;
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

        lmdb::Env env(512 << 20);

        env.open(envname, env_flags, 0664);

        lmdb::Txn txn(env, txn_flags);

        lmdb::Dbi dbi(env, txn, NULL, dbi_flags);
   
        (*handler)(env, txn, dbi);

        return 0;

    } catch(int rc) {
        std::cerr << "Terminating with error code " << rc << std::endl;
        return 1;
    }
}

