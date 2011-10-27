#ifndef _HEAPFILE_H
#define _HEAPFILE_H

#include "minirel.h"
#include "page.h"



#define TEMPORARY 0
#define PERMENANT 1

class HeapPage;

class HeapFile 
{
	friend class Scan;

private :
	
	char *filename;
	int   type;

	PageID dirPid;
	PageID lastDirPid;

	PageID NextPage (PageID pid);
	Status NewPage(PageID &pid, PageID &dirPid);

	PageID GetFirstDirPage() { return dirPid; }


public:

    HeapFile( const char* name, Status& returnStatus ); 
    ~HeapFile();
	
    int GetNumOfRecords();
    Status InsertRecord(char* recPtr, int recLen, RecordID& outRid); 
    Status DeleteRecord(const RecordID& rid); 
    Status UpdateRecord(const RecordID& rid, char* recPtr, int recLen);
    Status GetRecord(const RecordID& rid, char* recPtr, int& recLen); 
    class Scan* OpenScan(Status& status);

    Status DeleteFile();
};


#endif
