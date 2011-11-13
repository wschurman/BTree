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
	const char * dbfile;

	Status BTreeFile::DestroyHelper(PageID currPid);
	Status BTreeFile::InsertHelper(PageID currPid, SplitStatus& st, char*& newChildKey, PageID & newChildPageID, const char *key, const RecordID rid);
	Status BTreeFile::SplitLeafPage(LeafPage* oldPage, LeafPage* newPage, const char *key, const RecordID rid);
	Status BTreeFile::SplitIndexPage(IndexPage* oldPage, IndexPage* newPage, const char *key, const PageID rid);

	//Please don't delete this method. It's used for testing, 
	// and may be useful for you.
	PageID GetLeftLeaf();

	Status BTreeFile::_searchTree( const char *key,  PageID currentID, PageID& lowIndex);
	Status BTreeFile::_searchIndexNode(const char *key,  PageID currentID, IndexPage *currentIndex, PageID& lowIndex);

};


#endif