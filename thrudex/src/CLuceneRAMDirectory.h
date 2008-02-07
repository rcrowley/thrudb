/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Hijacked by Jake
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#ifndef _thrudex_lucene_store_RAMDirectory_
#define _thrudex_lucene_store_RAMDirectory_

#if defined(_LUCENE_PRAGMA_ONCE)
# pragma once
#endif

#include "CLucene/store/Lock.h"
#include "CLucene/store/Directory.h"
#include "CLucene/util/VoidMap.h"
#include "CLucene/util/Arrays.h"
#include "CLucene/store/RAMDirectory.h"

CL_NS_DEF(store)

/**
 * A memory-resident {@link Directory} implementation.
 */
class CLuceneRAMDirectory:public Directory{

    class RAMLock: public LuceneLock{
    private:
        CLuceneRAMDirectory* directory;
        char* fname;
    public:
        RAMLock(const char* name, CLuceneRAMDirectory* dir);
        virtual ~RAMLock();
        bool obtain();
        void release();
        bool isLocked();
        virtual TCHAR* toString();
    };

    typedef CL_NS(util)::CLHashMap<const char*,RAMFile*,
        CL_NS(util)::Compare::Char, CL_NS(util)::Equals::Char,
        CL_NS(util)::Deletor::acArray , CL_NS(util)::Deletor::Object<RAMFile> > FileMap;
 protected:
    /// Removes an existing file in the directory.
    virtual bool doDeleteFile(const char* name);

    /**
     * Creates a new <code>RAMDirectory</code> instance from a different
     * <code>Directory</code> implementation.  This can be used to load
     * a disk-based index into memory.
     * <P>
     * This should be used only with indices that can fit into memory.
     *
     * @param dir a <code>Directory</code> value
     * @exception IOException if an error occurs
     */
    void _copyFromDir(Directory* dir, bool closeDir);
    FileMap files; // unlike the java Hashtable, FileMap is not synchronized, and all access must be protected by a lock
 public:
#ifndef _CL_DISABLE_MULTITHREADING //do this so that the mutable keyword still works without mt enabled
    mutable DEFINE_MUTEX(files_mutex) // mutable: const methods must also be able to synchronize properly
#endif

        /// Returns a null terminated array of strings, one for each file in the directory.
        void list(vector<string>* names) const;

    /** Constructs an empty {@link Directory}. */
    CLuceneRAMDirectory();

    ///Destructor - only call this if you are sure the directory
    ///is not being used anymore. Otherwise use the ref-counting
    ///facilities of dir->close
    virtual ~CLuceneRAMDirectory();
    CLuceneRAMDirectory(Directory* dir);

    /**
     * Creates a new <code>RAMDirectory</code> instance from the {@link FSDirectory}.
     *
     * @param dir a <code>String</code> specifying the full index directory path
     */
    CLuceneRAMDirectory(const char* dir);

    /// Returns true iff the named file exists in this directory.
    bool fileExists(const char* name) const;

    /// Returns the time the named file was last modified.
    int64_t fileModified(const char* name) const;

    /// Returns the length in bytes of a file in the directory.
    int64_t fileLength(const char* name) const;

    /// Removes an existing file in the directory.
    virtual void renameFile(const char* from, const char* to);

    /** Set the modified time of an existing file to now. */
    void touchFile(const char* name);

    /// Creates a new, empty file in the directory with the given name.
    ///     Returns a stream writing this file.
    virtual IndexOutput* createOutput(const char* name);

    /// Construct a {@link Lock}.
    /// @param name the name of the lock file
    LuceneLock* makeLock(const char* name);

    /// Returns a stream reading an existing file.
    IndexInput* openInput(const char* name);

    virtual void close();

    TCHAR* toString() const;

    static const char* DirectoryType() { return "RAM"; }
    const char* getDirectoryType() const{ return DirectoryType(); }
};
CL_NS_END
#endif
