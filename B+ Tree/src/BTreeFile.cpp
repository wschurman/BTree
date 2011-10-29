#include "BTreeFile.h"
//#include "BTreeLeafPage.h"
#include "db.h"
#include "bufmgr.h"
#include "system_defs.h"

//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.  
// Output  : returnStatus - status of execution of constructor. 
//           OK if successful, FAIL otherwise.
// Purpose : Open the index file, if it exists. 
//			 Otherwise, create a new index, with the specified 
//           filename. You can use 
//                MINIBASE_DB->GetFileEntry(filename, headerID);
//           to retrieve an existing index file, and 
//                MINIBASE_DB->AddFileEntry(filename, headerID);
//           to create a new one. You should pin the header page
//           once you have read or created it. You will use the header
//           page to find the root node. 
//-------------------------------------------------------------------
BTreeFile::BTreeFile(Status& returnStatus, const char *filename) {
	PageID headerID = NULL;
	Status s = MINIBASE_DB->GetFileEntry(filename, headerID);
	if (s == FAIL) {
		Page *p;
		returnStatus= MINIBASE_BM->NewPage(headerID, p);
		if (returnStatus == OK){
			this->header = (BTreeHeaderPage*)p; 
			this->header->Init(headerID); 
			this->header->SetRootPageID(INVALID_PAGE);
			//may need to initialize next and previous page to Invalid?
			returnStatus = MINIBASE_DB->AddFileEntry(filename, headerID);
		}
	}
	
	if (MINIBASE_BM->PinPage(headerID, (Page*&) this->header) != OK) {
		std::cout << "Unable to pin header page in BTreeFile constructor" << std::endl;
	}
}



//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None 
// Return  : None
// Output  : None
// Purpose : Free memory and clean Up. You should be sure to 
//           unpin the header page if it has not been unpinned 
//           in DestroyFile.
//-------------------------------------------------------------------

BTreeFile::~BTreeFile() {
	MINIBASE_BM->UnpinPage(header->GetRootPageID(), true);
}

//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Free all pages and delete the entire index file. Once you have
//           freed all the pages, you can use MINIBASE_DB->DeleteFileEntry (dbname)
//           to delete the database file. 
//-------------------------------------------------------------------
Status BTreeFile::DestroyFile() {

	return FAIL;
}





//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.  
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status BTreeFile::Insert(const char *key, const RecordID rid) {
	
	PageID rootPid;
	Status s;
	rootPid = header->GetRootPageID();
	// If no root page, create one
	if(rootPid == INVALID_PAGE) {
		LeafPage* leafpage;
		s = MINIBASE_BM->NewPage(rootPid, (Page*&)leafpage);
		if(s == OK){
			// Need to initialize?
			leafpage->Init(rootPid, LEAF_PAGE);
			leafpage->SetNextPage(INVALID_PAGE);
			leafpage->SetPrevPage(INVALID_PAGE);
			leafpage->Insert(key,rid);

			//Make this the root page
			header->SetRootPageID(rootPid);
			UNPIN(rootPid, DIRTY);
			return OK;
		} else {
			return s;
		}
	}
	//there is root already
	else{
		PageID currPid = rootPid;
		PageID insertPid = NULL;

		SplitStatus split;
		char * new_child_key;
		PageID new_child_pageid;

		s = this->InsertHelper(currPid, split, new_child_key, new_child_pageid, key, rid);

		if(s != OK)
			return s;
		else{ // do something....
			return OK;
		}
	}
}

//recursive helper function, the bool return type and parentPid is used for splitting recursively, i.e. if it returns 
Status BTreeFile::InsertHelper(PageID currPid, SplitStatus& st, char*& newChildKey, PageID & newChildPageID, const char *key, const RecordID rid) {
	ResizableRecordPage* currPage;
	PageID nextPid;
	Status s;

	SplitStatus split;
	char * new_child_key;
	PageID new_child_pageid;

	PIN(currPid, currPage);

	if (currPage->GetType() == INDEX_PAGE) {
		IndexPage* indexPage = (IndexPage*) currPage;
		PageKVScan<PageID>* iter;
		indexPage->OpenScan(iter);
		indexPage->Search(key, *iter);
		char * c;
		iter->GetNext(c, nextPid);
		// may need to deallocate iter
		
		s = this->InsertHelper(nextPid, split, new_child_key, new_child_pageid, key, rid); // traverse to child

		if (split == NEEDS_SPLIT) {
			// split this child index node, will need to insert new leftval into this Index page with
			// pointer to new child node

			// TODO: ensure there is space in indexPage, if not, split, insert, and propagate up
			// TODO: need special case if indexPage is root
			indexPage->Insert(new_child_key, new_child_pageid);

			UNPIN(currPid, DIRTY);
			return s;
		} else {
			UNPIN(currPid, CLEAN);
			return s;
		}

	} else if(currPage->GetType() == LEAF_PAGE){
		// does not use split
		// in the case that this leaf page needs to split:
			// split
			// set st to true, set newChildKey and newChildPageID
		LeafPage* leafPage = (LeafPage*) currPage;
		if (leafPage->Insert(key, rid) != OK) {
			// split this page.

			// make new page
			LeafPage* newLeafPage;
			PageID newLeafPid;
			s = MINIBASE_BM->NewPage(newLeafPid, (Page*&)newLeafPage);
			this->SplitPage(leafPage, newLeafPage, key, rid);

			st = NEEDS_SPLIT;
			newLeafPage->GetMinKey(newChildKey);
			newChildPageID = newLeafPid;
		}
	} else {
		return FAIL; // ???
	}
}


Status BTreeFile::SplitPage(LeafPage* oldPage, LeafPage* newPage, const char *key, const RecordID rid) {
	// replace scans with pointer swapping in the future
	PageKVScan<RecordID>* oldScan;
	PageKVScan<RecordID>* newScan;
	oldPage->OpenScan(oldScan);

	char* currKey;
	RecordID currID;
	Status ds;
	bool insertedNew = false;

	while (oldScan->GetNext(currKey, currID) != DONE) {
		newPage->Insert(currKey, currID);
		ds = oldScan->DeleteCurrent();
		if (ds != OK) {
			std::cout << "SplitPage move delete failed" << std::endl;
		}
	}

	newPage->OpenScan(newScan);
	newScan->GetNext(currKey, currID);

	while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {
		if (strcmp(currKey, key) < 0) { // currKey < key
			oldPage->Insert(currKey, currID);
			newScan->DeleteCurrent();
			newScan->GetNext(currKey, currID);
		} else if (strcmp(currKey, key) > 0 && insertedNew == false) { // currKey > key
			oldPage->Insert(key, rid);
		} else {
			// huh, should not reach here
		}
	}

	if (insertedNew == false) {
		newPage->Insert(key, rid);
	}
}


//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to BTreeFileScan class.
// Purpose : Initialize a scan.  
// Note    : Usage of lowKey and highKey :
//
//           lowKey   highKey   range
//			 value	  value	
//           --------------------------------------------------
//           NULL     NULL      whole index
//           NULL     !NULL     minimum to highKey
//           !NULL    NULL      lowKey to maximum
//           !NULL    =lowKey   exact match (may not be unique)
//           !NULL    >lowKey   lowKey to highKey
//-------------------------------------------------------------------
BTreeFileScan* BTreeFile::OpenScan(const char* lowKey, const char* highKey) {
	//Your code here
	return NULL;
}

//-------------------------------------------------------------------
// BTreeFile::GetLeftLeaf
//
// Input   : None
// Output  : None
// Return  : The PageID of the leftmost leaf page in this index. 
// Purpose : Returns the pid of the leftmost leaf. 
//-------------------------------------------------------------------
PageID BTreeFile::GetLeftLeaf() {
	PageID leafPid = header->GetRootPageID();

	while(leafPid != INVALID_PAGE) {
		ResizableRecordPage* rrp;
		if(MINIBASE_BM->PinPage(leafPid, (Page*&)rrp) == FAIL) {
			std::cerr << "Error pinning page in GetLeftLeaf." << std::endl;
			return INVALID_PAGE;
		}
		//If we have reached a leaf page, then we are done.
		if(rrp->GetType() == LEAF_PAGE) {
			break;
		}
		//Otherwise, traverse down the leftmost branch.
		else {
			PageID tempPid = rrp->GetPrevPage();
			if(MINIBASE_BM->UnpinPage(leafPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning page in OpenScan." << std::endl;
				return INVALID_PAGE;
			}
			leafPid = tempPid;
		}
	}
	if(leafPid != INVALID_PAGE && (MINIBASE_BM->UnpinPage(leafPid, CLEAN) == FAIL)) {
		std::cerr << "Error unpinning page in OpenScan." << std::endl;
		return INVALID_PAGE;
	}
	return leafPid;
}


//-------------------------------------------------------------------
// BTreeFile::GetLeftLeaf
//
// Input   : pageID,  the page to start printing at. 
//           printContents,  whether to print the contents
//                           of the page or just metadata. 
// Output  : None
// Return  : None
// Purpose : Prints the subtree rooted at the given page. 
//-------------------------------------------------------------------
Status BTreeFile::PrintTree (PageID pageID, bool printContents) {

	ResizableRecordPage* page;
	PIN(pageID, page);

	if(page->GetType() == INDEX_PAGE) {
		IndexPage* ipage = (IndexPage*) page;
		ipage->PrintPage(printContents);

		PageID pid = ipage->GetPrevPage();
		assert(pid != INVALID_PAGE);

		PrintTree(pid, printContents);

		PageKVScan<PageID> scan;
		if(ipage->OpenScan(&scan) != OK) {
			return FAIL;
		}
		while(true) {
			char* key;
			PageID val;
			Status stat = scan.GetNext(key, val);
			assert(val != INVALID_PAGE);
			if(stat == DONE) {
				break;
			}
			PrintTree(val, printContents);
		}

	}
	else {
		LeafPage *lpage = (LeafPage*) page;
		lpage->PrintPage(printContents);
	}
	
	UNPIN(pageID, CLEAN);
	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::PrintWhole
//
// Input   : printContents,  whether to print the contents
//                           of each page or just metadata. 
// Output  : None
// Return  : None
// Purpose : Prints the B Tree. 
//-------------------------------------------------------------------
Status BTreeFile::PrintWhole (bool printContents) {
	if(header == NULL || header->GetRootPageID() == INVALID_PAGE) {
		return FAIL;
	}
	return PrintTree(header->GetRootPageID(), printContents);
}
