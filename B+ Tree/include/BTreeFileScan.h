#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "BTreeFile.h"
#include "BTreeInclude.h"

class BTreeFileScan {// : public IndexFileScan {

public:
	// BTreeFile is a friend, which means that it can access the private
	// members of BTreeFileScan. You should use this to initialize the
	// scan appropriately. 
	friend class BTreeFile;

	typedef SortedKVPage<RecordID> LeafPage;

	// Retrieves the next (key, value) pair in the tree.
    Status GetNext(RecordID & rid, char*& keyptr);

	// Deletes the key value pair most recently returned from 
	// GetNext. Note that this should delete the key value pair
	// from the appropriate leaf page, but does not need to
	// merge/redistribute keys.
	Status DeleteCurrent();

	~BTreeFileScan();	

private:

	//Note that the constructor is prova
	BTreeFileScan();

	// You may add private methods here. 
	const char * lowKey; // min key in scan
    const char * highKey; // max key in scan
	PageID currentPageID; // pageID that scan is currently on
    bool done; // true when scan is done
	PageKVScan<RecordID>* scan; // scan for a given page
	LeafPage* currentPage; // page that scan is currently on

	Status BTreeFileScan::_SetIter(); //function to initialize PageKVScan scan to starting point for the low key

};

#endif