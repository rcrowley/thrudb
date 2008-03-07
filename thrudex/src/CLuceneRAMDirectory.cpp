/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/StdHeader.h"
#include "CLuceneRAMDirectory.h"

#include "CLucene/store/Lock.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/index/IndexReader.h"
#include "CLucene/util/VoidMap.h"
#include "CLucene/util/Misc.h"
#include "CLucene/debug/condition.h"

#include <iostream>

CL_NS_USE(util)
CL_NS_DEF(store)

CLuceneRAMDirectory::RAMLock::RAMLock(const char* name, CLuceneRAMDirectory* dir)
   : directory(dir)
{
    fname = STRDUP_AtoA(name);
}

CLuceneRAMDirectory::RAMLock::~RAMLock()
{
    _CLDELETE_LCaARRAY( fname );
    directory = NULL;
}

TCHAR* CLuceneRAMDirectory::RAMLock::toString(){
    return STRDUP_TtoT(_T("LockFile@RAM"));
}

bool CLuceneRAMDirectory::RAMLock::isLocked() {
    return directory->fileExists(fname);
}

bool CLuceneRAMDirectory::RAMLock::obtain()
{
    SCOPED_LOCK_MUTEX(directory->files_mutex);
    if (!directory->fileExists(fname)) {
        IndexOutput* tmp = directory->createOutput(fname);
        tmp->close();
        _CLDELETE(tmp);

        return true;
    }
    return false;
}

void CLuceneRAMDirectory::RAMLock::release(){
    directory->deleteFile(fname);
}


void CLuceneRAMDirectory::list(vector<string>* names) const{
    SCOPED_LOCK_MUTEX(files_mutex);

    FileMap::const_iterator itr = files.begin();
    while (itr != files.end()){
        names->push_back(itr->first);
        ++itr;
    }
}

CLuceneRAMDirectory::CLuceneRAMDirectory():
    Directory(),files(true,true)
{
}

CLuceneRAMDirectory::~CLuceneRAMDirectory(){
    //todo: should call close directory?
}

void CLuceneRAMDirectory::_copyFromDir(Directory* dir, bool closeDir)
{
    vector<string> names;
    dir->list(&names);
    uint8_t buf[CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE];

    for (size_t i=0;i<names.size();++i ){
        if ( !CL_NS(index)::IndexReader::isLuceneFile(names[i].c_str()))
            continue;

        // make place on ram disk
        IndexOutput* os = createOutput(names[i].c_str());
        // read current file
        IndexInput* is = dir->openInput(names[i].c_str());
        // and copy to ram disk
        //todo: this could be a problem when copying from big indexes...
        int64_t len = is->length();
        int64_t readCount = 0;
        while (readCount < len) {
            int32_t toRead = (readCount + CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE > len ? len - readCount : (int32_t)CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE);
            is->readBytes(buf, toRead);
            os->writeBytes(buf, toRead);
            readCount += toRead;
        }



        // graceful cleanup
        is->close();
        _CLDELETE(is);
        os->close();
        _CLDELETE(os);
    }

    if (closeDir)
        dir->close();
}

CLuceneRAMDirectory::CLuceneRAMDirectory(Directory* dir):
    Directory(),files(true,true)
{
    _copyFromDir(dir,false);

}

CLuceneRAMDirectory::CLuceneRAMDirectory(const char* dir):
    Directory(),files(true,true)
{
    Directory* fsdir = FSDirectory::getDirectory(dir,false);
    try{
        _copyFromDir(fsdir,false);
    }_CLFINALLY(fsdir->close();_CLDECDELETE(fsdir););

}

bool CLuceneRAMDirectory::fileExists(const char* name) const {
    SCOPED_LOCK_MUTEX(files_mutex);
    return files.exists(name);
}

int64_t CLuceneRAMDirectory::fileModified(const char* name) const {
    SCOPED_LOCK_MUTEX(files_mutex);
    const RAMFile* f = files.get(name);
    return f->lastModified;
}

int64_t CLuceneRAMDirectory::fileLength(const char* name) const{
    SCOPED_LOCK_MUTEX(files_mutex);
    RAMFile* f = files.get(name);
    return f->length;
}


IndexInput* CLuceneRAMDirectory::openInput(const char* name) {
    SCOPED_LOCK_MUTEX(files_mutex);
    RAMFile* file = files.get(name);
    if (file == NULL) {
        return NULL;
    }

    return _CLNEW RAMIndexInput( file );
}

void CLuceneRAMDirectory::close(){
    SCOPED_LOCK_MUTEX(files_mutex);
    files.clear();
}

bool CLuceneRAMDirectory::doDeleteFile(const char* name) {
    SCOPED_LOCK_MUTEX(files_mutex);
    files.remove(name);
    return true;
}

void CLuceneRAMDirectory::renameFile(const char* from, const char* to) {
    SCOPED_LOCK_MUTEX(files_mutex);
    FileMap::iterator itr = files.find(from);

    /* DSR:CL_BUG_LEAK:
    ** If a file named $to already existed, its old value was leaked.
    ** My inclination would be to prevent this implicit deletion with an
    ** exception, but it happens routinely in CLucene's internals (e.g., during
    ** IndexWriter.addIndexes with the file named 'segments'). */
    if (files.exists(to)) {
        files.remove(to);
    }
    if ( itr == files.end() ){
        char tmp[1024];
        _snprintf(tmp,1024,"cannot rename %s, file does not exist",from);
        _CLTHROWT(CL_ERR_IO,tmp);
    }
    CND_PRECONDITION(itr != files.end(), "itr==files.end()")
        RAMFile* file = itr->second;
    files.removeitr(itr,false,true);
    files.put(STRDUP_AtoA(to), file);
}


void CLuceneRAMDirectory::touchFile(const char* name) {
    RAMFile* file = NULL;
    {
        SCOPED_LOCK_MUTEX(files_mutex);
        file = files.get(name);
    }
    uint64_t ts1 = file->lastModified;
    uint64_t ts2 = Misc::currentTimeMillis();

    //make sure that the time has actually changed
    while ( ts1==ts2 ) {
        _LUCENE_SLEEP(1);
        ts2 = Misc::currentTimeMillis();
    };

    file->lastModified = ts2;
}

IndexOutput* CLuceneRAMDirectory::createOutput(const char* name) {
    /* Check the $files VoidMap to see if there was a previous file named
    ** $name.  If so, delete the old RAMFile object, but reuse the existing
    ** char buffer ($n) that holds the filename.  If not, duplicate the
    ** supplied filename buffer ($name) and pass ownership of that memory ($n)
    ** to $files. */

    SCOPED_LOCK_MUTEX(files_mutex);

    const char* n = files.getKey(name);
    if (n != NULL) {
        RAMFile* rf = files.get(name);
        _CLDELETE(rf);
    } else {
        n = STRDUP_AtoA(name);
    }

    RAMFile* file = _CLNEW RAMFile();
#ifdef _DEBUG
    file->filename = n;
#endif
    files[n] = file;

    return _CLNEW RAMIndexOutput(file);
}

LuceneLock* CLuceneRAMDirectory::makeLock(const char* name) {
    return _CLNEW RAMLock(name,this);
}

TCHAR* CLuceneRAMDirectory::toString() const{
    return STRDUP_TtoT( _T("CLuceneRAMDirectory") );
}
CL_NS_END
