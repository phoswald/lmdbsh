#include "lmdb-wrappers.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>

namespace lmdb {

void RetCode::onError(const char* method) {
    std::cerr << "ERROR: " << method << " failed with code " << rc << ": " << mdb_strerror(rc) << std::endl;
    throw rc;
}

Val::Val() {
    own = NULL;
    val.mv_data = NULL;
    val.mv_size = 0;        
}

Val::Val(Val& other) {
    init(other.val.mv_data, other.val.mv_size);
}

Val::Val(const std::vector<char>& data) {
    init(data.data(), data.size());
}

Val::~Val() {
    if(own) 
        free(own);
}

void Val::init(const void* data, size_t size) {
    if(size == 0) {
        own = NULL;
        val.mv_data = NULL;
        val.mv_size = 0;        
    } else {
        own = malloc(size);
        val.mv_data = own;
        val.mv_size = size;        
        if(!val.mv_data) {
            std::cerr << "ERROR: malloc() failed." << std::endl;
            throw 0;
        }
        memcpy(val.mv_data, data, size);
    }
}

bool Val::startsWith(Val& other) {
    return val.mv_size >= other.val.mv_size && memcmp(val.mv_data, other.val.mv_data, other.val.mv_size) == 0;
}

std::ostream& Val::print(std::ostream& stm) const {
    static const char hex[] = "0123456789ABCDEF";
    char* cur = (char*) val.mv_data;
    char* end = cur + val.mv_size;
    while (cur < end) {
        char c = *cur;
        if(c >= ' ' && c <= 0x7F) {
            if(c == '\\' || c == '*' || c == ':') {
                stm << '\\';
            }
            stm << c;
        } else if(c == '\t') {
            stm << '\\';
            stm << 't';
        } else if(c == '\r') {
            stm << '\\';
            stm << 'r';
        } else if(c == '\n') {
            stm << '\\';
            stm << 'n';
        } else {
            stm << "\\x";
            stm << hex[((unsigned char) c) >> 4];
            stm << hex[((unsigned char) c) & 0xF];
        }
        cur++;
    }
    return stm;
}

} // namespace lmdb

