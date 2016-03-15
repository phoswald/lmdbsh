#include "lmdb-wrappers.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>

namespace lmdb {

void RetCode::onError(const char* method) {
    std::cout << "ERROR: " << method << " failed with code " << rc << ": " << mdb_strerror(rc) << std::endl;
    throw rc;
}

Val::Val() {
    own = NULL;
    val.mv_data = NULL;
    val.mv_size = 0;        
}

Val::Val(Val& other) {
    own = NULL;
    val.mv_data = NULL;
    val.mv_size = 0;        
    set(other.val.mv_data, other.val.mv_size);
}

Val::~Val() {
    if(own) 
        free(own);
}

bool Val::startsWith(Val& other) {
    return val.mv_size >= other.val.mv_size && memcmp(val.mv_data, other.val.mv_data, other.val.mv_size) == 0;
}

void Val::set(std::string line, int beg, int end) {
    set(line.c_str() + beg, end - beg);
}

void Val::set(const void* data, size_t size) {
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

std::ostream& Val::print(std::ostream& stm) const {
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

} // namespace lmdb

