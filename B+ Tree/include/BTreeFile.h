#ifndef _B_TREE_FILE_H_
#define _B_TREE_FILE_H_

#include "BTreeheaderPage.h"
#include "BTreeFileScan.h"
#include "BTreeTest.h"
#include "BTreeInclude.h"

enum SplitStatus {
	NEEDS_SPLIT,
	CLEAN_INSERT
};

class BTreeFile {

public:
	friend class BTreeDriver;

	BTreeFile(Status& status, const char *filename);

	Status DestroyFile();

	~BTreeFile();
	
	Status Insert(const char *key, const RecordID rid);

	BTreeFileScan* OpenScan(const char* lowKey, const char* highKey);

	Status PrintTree (PageID pageID, bool printContents);
	Status PrintWhole (bool printContents = false);	


private:

	BTreeHeaderPage* header;

	Status BTreeFile::InsertHelper(PageID currPid, SplitStatus& st, char*& newChildKey, PageID & newChildPageID, const char *key, const RecordID rid);
	Status BTreeFile::SplitPage(LeafPage* oldPage, LeafPage* newPage, const char *key, const RecordID rid);

	//Please don't delete this method. It's used for testing, 
	// and may be useful for you.
	PageID GetLeftLeaf();



};


#endif