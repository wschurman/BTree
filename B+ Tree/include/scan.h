/* -*- C++ -*- */
/* 
 * scan.h -  class Scan
 */

#ifndef _SCAN_H_
#define _SCAN_H_


#include "minirel.h"
#include "dirpage.h"
#include "heappage.h"

class HeapFile;
class HeapPage;

class Scan
{
public:

  Scan(HeapFile* hf, Status& status);
  ~Scan();

  Status GetNext(RecordID& rid, char* recPtr, int& recLen );
  Status MoveTo(RecordID rid);

private:

	PageID currDirPid;
	PageID firstDirPid;
	DirPage *dirPage;
	int currEntry;

	PageID currPid;
	HeapPage *page;

	RecordID currRid;

	Bool noMore;
};

#endif
