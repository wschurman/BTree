#include "BTreeFile.h"
//#include "BTreeLeafPage.h"
#include "db.h"
#include "bufmgr.h"
#include "system_defs.h"

#define _CRTDBG_MAPALLOC
#include <stdlib.h>
#include <crtdbg.h>

#ifdef _DEBUG
#ifndef DBG_NEW
#define DBG_NEW new(_NORMAL_BLOCK, __FILE__, __LINE__)
#define new DBG_NEW
#endif
#endif // _DEBUG

#include <iostream>
using namespace std;

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
	if (s == FAIL) { // no database header page yet, create it
		Page *p;
		returnStatus = MINIBASE_BM->NewPage(headerID, p);
		if (returnStatus == OK){
			this->header = (BTreeHeaderPage*)p; 
			this->header->Init(headerID); 
			this->header->SetRootPageID(INVALID_PAGE);
			returnStatus = MINIBASE_DB->AddFileEntry(filename, headerID);
		}
	}

	returnStatus = MINIBASE_BM->PinPage(headerID, (Page*&) this->header); // pin the header
	if (returnStatus != OK) {
		std::cout << "Unable to pin header page in BTreeFile constructor" << std::endl;
	}

	this->dbfile = filename;
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
	MINIBASE_BM->UnpinPage(((HeapPage*)this->header)->PageNo(), true); // unpin header
	//_CrtDumpMemoryLeaks();
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
	cout << "DestroyFile()" << endl;

	PageID rootPid;
	Status s;
	rootPid = header->GetRootPageID();

	if (rootPid == INVALID_PAGE) { // no root, done deleting already
		return OK;
	} else {
		s = this->DestroyHelper(rootPid); // recursively delete from root
		if (s != OK) {
			cout << "First call to destroy helper failed" << endl;
			return s;
		}
		s = MINIBASE_BM->FreePage(rootPid); // free root
		if (s != OK) {
			cout << "Freeing root failed" << endl;
			return s;
		}
		s = MINIBASE_DB->DeleteFileEntry(this->dbfile); // delete db file
		return s;
	}
}


//-------------------------------------------------------------------
// BTreeFile::DestroyHelper
//
// Input   : currPid
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Recursively traverse the tre and free all pages along the way.
//-------------------------------------------------------------------
Status BTreeFile::DestroyHelper(PageID currPid) {
	ResizableRecordPage* currPage;
	Status s = OK;

	PIN(currPid, currPage);

	if (currPage->GetType() == INDEX_PAGE) {
		IndexPage* indexPage = (IndexPage*) currPage;
		PageKVScan<PageID>* iter = new PageKVScan<PageID>();
		indexPage->OpenScan(iter);

		char* currKey;
		PageID currID;
		Status ds = OK;

		while (iter->GetNext(currKey, currID) != DONE) {
			// index page, so recursively call helper on all child pages
			ds = this->DestroyHelper(currID);
			if (ds != OK) {
				cout << "Destroy helper failed" << endl;
				return ds;
			}
			// delete child page after recursively deleting all its children
			ds = MINIBASE_BM->FreePage(currID);
			if (ds != OK) {
				cout << "Free Page failed" << endl;
				return ds;
			}
		}

		delete iter;

		UNPIN(currPid, DIRTY);
		return s;
	} else if(currPage->GetType() == LEAF_PAGE) {
		// will be deleted by parent index page call to recursive method
		UNPIN(currPid, CLEAN);
		return s;
	} else { // should not happen
		UNPIN(currPid, CLEAN);
		return FAIL;
	}

	return s;
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
		if(s == OK) {
			leafpage->Init(rootPid, LEAF_PAGE);
			leafpage->SetNextPage(INVALID_PAGE);
			leafpage->SetPrevPage(INVALID_PAGE);
			s = leafpage->Insert(key,rid); // insert first key, value into root leaf

			//Make this the root page
			header->SetRootPageID(rootPid);
			UNPIN(rootPid, DIRTY);
			return s;
		} else {
			return s;
		}
	} else { //there is root already

		PageID currPid = rootPid;
		PageID insertPid = NULL;

		SplitStatus split;
		char * new_child_key;
		PageID new_child_pageid;

		// recursively traverse
		s = this->InsertHelper(currPid, split, new_child_key, new_child_pageid, key, rid);

		// split will propagate up through the split variable above, signaling that the child was split and a
		// new page was created with new_child_key and new_child_pageid that need to be inserted into the root
		// page which also needs to be split
		if (split == NEEDS_SPLIT) {

			ResizableRecordPage* currPage;
			PIN(rootPid, currPage);

			if (currPage->GetType() == INDEX_PAGE) {

				IndexPage* indexPage = (IndexPage*) currPage;

				IndexPage* newRoot;
				PageID newRootPid;
				s = MINIBASE_BM->NewPage(newRootPid, (Page*&)newRoot);
				if (s != OK) {
					cout << "Error allocating new root" << endl;
					return s;
				}
				newRoot->Init(newRootPid, INDEX_PAGE);
				newRoot->SetNextPage(INVALID_PAGE);
				newRoot->SetPrevPage(INVALID_PAGE);

				char * minKey;
				Status s2;
				s2 = indexPage->GetMinKey(minKey);
				if (s2 != OK) {
					cout << "Error getting MinKey in Insert split root" << endl;
					return s2;
				}

				// insert two children into new root index page
				s2 = newRoot->Insert(minKey, rootPid);
				s2 = newRoot->Insert(new_child_key, new_child_pageid);
				if (s2 != OK) {
					cout << "Error inserting into new root" << endl;
					return s2;
				}

				this->header->SetRootPageID(newRootPid); // update root pid variable

				// update prev page pointer
				char* minKey2;
				PageID minVal;
				newRoot->GetMinKeyValue(minKey2, minVal);
				newRoot->SetPrevPage(minVal);

				// delete the first entry that is now the prev page pointer
				PageKVScan<PageID>* iter = new PageKVScan<PageID>();
				newRoot->OpenScan(iter);
				iter->GetNext(minKey2, minVal);
				iter->DeleteCurrent();
				delete iter;

				UNPIN(rootPid, DIRTY);
				UNPIN(newRootPid, DIRTY);

				return s;

			} else { // root is leaf page and needs split into index page with two leaf children

				IndexPage* newIndexPage;
				PageID newIndexPid;
				s = MINIBASE_BM->NewPage(newIndexPid, (Page*&)newIndexPage);
				if (s != OK) {
					cout << "Error allocating new root" << endl;
					return s;
				}
				newIndexPage->Init(newIndexPid, INDEX_PAGE);
				newIndexPage->SetNextPage(INVALID_PAGE);
				newIndexPage->SetPrevPage(INVALID_PAGE);

				char * minKey;
				Status s2;
				LeafPage* leafPage = (LeafPage*) currPage;
				s2 = leafPage->GetMinKey(minKey);

				if (s2 != OK) {
					cout << "Error getting MinKey in Insert split root" << endl;
					return s2;
				}

				// insert two pages
				s2 = newIndexPage->Insert(minKey, rootPid);
				s2 = newIndexPage->Insert(new_child_key, new_child_pageid);

				if (s2 != OK) {
					cout << "Error inserting into new root" << endl;
					return s2;
				}

				this->header->SetRootPageID(newIndexPid); // update header pid

				// set prev pointer
				char* minKey2;
				PageID minVal;
				newIndexPage->GetMinKeyValue(minKey2, minVal);
				newIndexPage->SetPrevPage(minVal);

				// need to delete first key value that is now the prev pointer
				PageKVScan<PageID>* iter = new PageKVScan<PageID>();
				newIndexPage->OpenScan(iter);
				iter->GetNext(minKey2, minVal);
				iter->DeleteCurrent();
				delete iter;

				UNPIN(newIndexPid, DIRTY);
				UNPIN(rootPid, DIRTY);
				return s;
			}
		}

		if(s != OK) {
			return s;
		} else {
			return OK;
		}
	}
}

//-------------------------------------------------------------------
// BTreeFile::InsertHelper
//
// Input   : currPid - the current pid being traversed
//           st - split status of the current node
//			 newChildKey - if new sibling was created, fill this and newChildPageID
//			 key, rid - the key, record ID being inserted
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Recursively traverse the tree to insert an index entry with this rid and key.
//			 Split if necessary and propagate up to parents.  
//-------------------------------------------------------------------
Status BTreeFile::InsertHelper(PageID currPid, SplitStatus& st, char*& newChildKey, PageID & newChildPageID, const char *key, const RecordID rid) {

	ResizableRecordPage* currPage;
	PageID nextPid;
	Status s = OK;
	Status s2 = OK;

	SplitStatus split;
	char * new_child_key;
	PageID new_child_pageid;

	PIN(currPid, currPage);

	if (currPage->GetType() == INDEX_PAGE) { // current page is an index page

		IndexPage* indexPage = (IndexPage*) currPage;
		PageKVScan<PageID>* iter = new PageKVScan<PageID>();

		indexPage->Search(key, *iter);

		char * largest_key; // lte key

		// check if done, in which case use the prev page pointer
		Status s3 = iter->GetNext(largest_key, nextPid);
		if (s3 != OK) {
			nextPid = indexPage->GetPrevPage();
		}
		PageID largest = nextPid;
		delete iter;

		// recursive call on next pid
		s = this->InsertHelper(nextPid, split, new_child_key, new_child_pageid, key, rid);

		if (split == NEEDS_SPLIT) {

			// child split, insert new child info into this page

			if (indexPage->Insert(new_child_key, new_child_pageid) != OK) {

				// no room in current page, split this page and propagate up

				IndexPage* newIndexPage;
				PageID newIndexPid;
				s2 = MINIBASE_BM->NewPage(newIndexPid, (Page*&)newIndexPage);
				if (s2 != OK) {
					cout << "Error allocating new index page in InsertHelper split" << endl;
					return s2;
				}
				newIndexPage->Init(newIndexPid, INDEX_PAGE);
				newIndexPage->SetNextPage(INVALID_PAGE);
				newIndexPage->SetPrevPage(INVALID_PAGE);
				char * new_page_key;
				s2 = this->SplitIndexPage(indexPage, newIndexPage, new_child_key, new_child_pageid, new_page_key);
				if (s2 != OK) {
					cout << "Error in Split Index" << endl;
					return s2;
				}

				st = NEEDS_SPLIT; // propagate up a level of recursion
				newChildKey = new_page_key;
				newChildPageID = newIndexPid;

				UNPIN(newIndexPid, DIRTY);

			}

			UNPIN(currPid, DIRTY);
			return s;
		} else {
			UNPIN(currPid, CLEAN);
			return s;
		}
	} else if (currPage->GetType() == LEAF_PAGE) {
		
		// reached leaf page, insert the key, rid, splitting if necessary

		LeafPage* leafPage = (LeafPage*) currPage;
		char * maxkey;
		leafPage->GetMaxKey(maxkey);

		if (leafPage->Insert(key, rid) != OK) {
			// split this page, propagate up to parent
			
			LeafPage* newLeafPage;
			PageID newLeafPid;

			s2 = MINIBASE_BM->NewPage(newLeafPid, (Page*&)newLeafPage);
			if (s2 != OK) {
				cout << "Error allocating new leaf page: "<<newLeafPid<<" in InsertHelper split" << endl;
				return s2;
			}
			newLeafPage->Init(newLeafPid, LEAF_PAGE);
			newLeafPage->SetNextPage(INVALID_PAGE);
			newLeafPage->SetPrevPage(INVALID_PAGE);

			s2 = this->SplitLeafPage(leafPage, newLeafPage, key, rid);
			if (s2 != OK) {
				cout << "Error in Split Leaf" << endl;
				return s2;
			}

			st = NEEDS_SPLIT; // propagate up
			newLeafPage->GetMinKey(newChildKey);
			newChildPageID = newLeafPid;

			UNPIN(newLeafPid, DIRTY);
		}
		UNPIN(currPid, DIRTY);
		return s;
	} else {
		UNPIN(currPid, CLEAN);
		return FAIL;
	}
}

//-------------------------------------------------------------------
// BTreeFile::SplitLeafPage
//
// Input   : oldPage - the current (full) page
//           newPage - the empty page
//			 key, rid - the key, record ID to be inserted during the split
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the page into two and insert the key, rid into the correct page 
//-------------------------------------------------------------------
Status BTreeFile::SplitLeafPage(LeafPage* oldPage, LeafPage* newPage, const char *key, const RecordID rid) {

	Status s1 = OK;
	PageKVScan<RecordID>* oldScan = new PageKVScan<RecordID>();
	PageKVScan<RecordID>* newScan = new PageKVScan<RecordID>(); // need to cleanup
	s1 = oldPage->OpenScan(oldScan);
	if (s1 != OK) {
		cout << "Error opening scan of oldPage in SplitLeafPage" << endl;
		return s1;
	}

	char* currKey;
	RecordID currID;
	Status ds = OK;

	bool insertedNew = false;

	// move items to new page
	while (oldScan->GetNext(currKey, currID) != DONE) {
		ds = newPage->Insert(currKey, currID);
		if (ds != OK) {
			std::cout << "SplitLeafPage transfer insert failed" << std::endl;
			return ds;
		}
	}
	oldPage->DeleteAll();
	delete oldScan;

	s1 = newPage->OpenScan(newScan);
	if (s1 != OK) {
		cout << "Error opening scan of newPage in SplitLeafPage" << endl;
		return s1;
	}
	s1 = newScan->GetNext(currKey, currID);
	if (s1 != OK) {
		cout << "Scan GetNext error in SplitLeafPage" << endl;
		return s1;
	}

	// move items back while keeping balance
	while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {		
		if (strcmp(currKey, key) > 0 && insertedNew == false) { // currKey > key, so put val into oldPage
			ds = oldPage->Insert(key, rid);
			insertedNew = true;
			if (ds != OK) {
				cout << "Insert new key in split failed SplitLeafPage" << endl;
				return ds;
			}
		} else {
			ds = oldPage->Insert(currKey, currID);
			if (ds != OK) {
				cout << "Moving Page Failed in SplitLeafPage" << endl;
				return ds;
			}

			ds = newScan->DeleteCurrent();
			if (ds != OK) {
				cout << "Deleting after move page failed in SplitLeafPage" << endl;
				return ds;
			}

			s1 = newScan->GetNext(currKey, currID);
			if (s1 != OK) {
				cout << "Scan GetNext error in SplitLeafPage" << endl;
				return s1;
			}
		}
	}

	delete newScan;

	// key, rid not inserted during split, insert now
	if (insertedNew == false) {
		ds = newPage->Insert(key, rid);
		if (ds != OK) {
			cout << "Insert new key after split failed SplitLeafPage" << endl;
			return ds;
		}
	}

	// set prev/next pointers
	PageID oldNextPageID = oldPage->GetNextPage();
	ResizableRecordPage* oldNextPage;
	if (oldNextPageID != INVALID_PAGE) {
		PIN(oldNextPageID, oldNextPage);
	}

	oldPage->SetNextPage(newPage->PageNo());
	newPage->SetPrevPage(oldPage->PageNo());

	if (oldNextPageID != INVALID_PAGE) {
		oldNextPage->SetPrevPage(newPage->PageNo());
		newPage->SetNextPage(oldNextPageID);
		UNPIN(oldNextPageID, DIRTY);
	}

	return ds;
}

//-------------------------------------------------------------------
// BTreeFile::SplitIndexPage
//
// Input   : oldPage - the current (full) page
//           newPage - the empty page
//			 key, rid - the key, record ID to be inserted during the split
//			 newPageKey - the min key to propagate up
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the page into two and insert the key, rid into the correct page
//-------------------------------------------------------------------
Status BTreeFile::SplitIndexPage(IndexPage* oldPage, IndexPage* newPage, const char *key, const PageID rid, char *&newPageKey) {

	Status s1 = OK;

	PageKVScan<PageID>* oldScan = new PageKVScan<PageID>();
	PageKVScan<PageID>* newScan = new PageKVScan<PageID>(); //need to cleanup
	s1 = oldPage->OpenScan(oldScan);
	if (s1 != OK) {
		cout << "Error opening scan of oldPage in SplitIndexPage" << endl;
		return s1;
	}

	char* currKey;
	PageID currID;
	Status ds = OK;
	bool insertedNew = false;

	while (oldScan->GetNext(currKey, currID) != DONE) {
		ds = newPage->Insert(currKey, currID);
		if (ds != OK) {
			std::cout << "SplitIndexPage transfer insert failed" << std::endl;
			return ds;
		}
	}
	oldPage->DeleteAll();
	delete oldScan;

	s1 = newPage->OpenScan(newScan);
	if (s1 != OK) {
		cout << "Error opening scan of newPage in SplitIndexPage" << endl;
		return s1;
	}
	s1 = newScan->GetNext(currKey, currID);
	if (s1 != OK) {
		cout << "Scan GetNext error in SplitIndexPage" << endl;
		return s1;
	}

	while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {
		if (strcmp(currKey, key) > 0 && insertedNew == false) { // currKey > key
			ds = oldPage->Insert(key, rid);
			if (ds != OK) {
				cout << "Insert new key in split failed SplitIndexPage" << endl;
				return ds;
			}
		} else { // currKey < key
			ds = oldPage->Insert(currKey, currID);
			if (ds != OK) {
				cout << "Moving Page Failed in SplitIndexPage" << endl;
				return ds;
			}
			ds = newScan->DeleteCurrent();
			if (ds != OK) {
				cout << "Deleting after move page failed in SplitIndexPage" << endl;
				return ds;
			}
			s1 = newScan->GetNext(currKey, currID);
			if (s1 != OK) {
				cout << "Scan GetNext error in SplitIndexPage" << endl;
				return s1;
			}
		}
	}

	delete newScan;

	if (insertedNew == false) {
		ds = newPage->Insert(key, rid);
		if (ds != OK) {
			cout << "Insert new key after split failed SplitIndexPage" << endl;
			return ds;
		}
	}

	char* minKey;
	PageID minVal;

	// set prev page pointer
	newPage->GetMinKeyValue(minKey, minVal);
	newPage->SetPrevPage(minVal);

	// propagate minKey
	newPageKey = minKey;

	// delete minKey
	PageKVScan<PageID>* iter = new PageKVScan<PageID>();
	newPage->OpenScan(iter);
	iter->GetNext(minKey, minVal);
	iter->DeleteCurrent();
	delete iter;

	return ds;
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
	BTreeFileScan* newScan = new BTreeFileScan();

	if (header->GetRootPageID() != INVALID_PAGE) { //found a root
		PageID lowIndex;
		Status s;

		if (lowKey == NULL) { // no lower bound so start at left most leaf using given function
			//initialize scan
			newScan->done = false;
			newScan->lowKey = lowKey;
			newScan->highKey = highKey;
			newScan->currentPageID = this->GetLeftLeaf();
		    newScan->_SetIter();
			return newScan; 
		}
		s = _searchTree(lowKey,  header->GetRootPageID(), lowIndex); //look for page with lowIndex
		if (s != OK) { //if search fails, initialize scan to done
			//initialize scan
			newScan->done = true; 
			newScan->lowKey = lowKey;
			newScan->highKey = highKey;
			newScan->currentPageID = INVALID_PAGE;
			return newScan;
		} else{ // found page to start at so initialize scan to that page
			//initialize scan
			newScan->done = false;
			newScan->lowKey = lowKey;
			newScan->highKey = highKey;
			newScan->currentPageID = lowIndex;
		    newScan->_SetIter();
			return newScan; 
		}
	} else { //root not found so just initialize scan to done
		//initialize scan
		newScan->done= true;
		newScan->lowKey = lowKey;
		newScan->highKey = highKey;
		newScan->currentPageID = INVALID_PAGE;
		return newScan;
	}
}

//function to find leaf page with lowkey or key just before that 
Status BTreeFile::_searchTree( const char *key,  PageID currentID, PageID& lowIndex)
{
    ResizableRecordPage *page;
	Status s;
    PIN (currentID, page);
    if (page->GetType()==INDEX_PAGE) { //current page is index page so call helper function
		s =	_searchIndexNode(key, currentID, (IndexPage*)page, lowIndex);
		return s;
	} else if(page->GetType()==LEAF_PAGE) { //current page is leaf page so store it so it can be used in scan
		lowIndex = page->PageNo();
		UNPIN(currentID,CLEAN);
	} else {
		return FAIL;
	}
	return OK;
}

//function to find the child for an index page that is on path to leaf page with lowkey on it
Status BTreeFile::_searchIndexNode(const char *key,  PageID currentID, IndexPage *currentIndex, PageID& lowIndex)
{
	PageID nextPid;
	PageKVScan<PageID>* iter = new PageKVScan<PageID>();

	//search for the key (or the next smallest key) on this page
	Status s1 = currentIndex->Search(key, *iter);

	char * sk;
	if (iter->GetNext(sk, nextPid) != OK) { // if no valid key, go to previous page
		delete iter;
		nextPid = currentIndex->GetPrevPage();
		if (nextPid == INVALID_PAGE) {
			cout << "GetPrevPage on Index Page during scan failed" << endl;
			return FAIL;
		}
		UNPIN(currentID, CLEAN);
		Status s = _searchTree(key, nextPid, lowIndex);
		return s;
	}

	char * mink;
	currentIndex->GetMinKey(mink);
	if (sk == mink) { // if key is first key on page then go back a page
		PageID prevpage = currentIndex->GetPrevPage();
		if (prevpage != INVALID_PAGE) {
			nextPid = prevpage;
		}
	} else {
		iter->GetPrev(sk, nextPid);
	}

	delete iter;

	UNPIN(currentID, CLEAN);
	Status s = _searchTree(key, nextPid, lowIndex); //return next page in search down tree in nextPid
	return s;
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