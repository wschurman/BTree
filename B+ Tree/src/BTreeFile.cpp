#include "BTreeFile.h"
//#include "BTreeLeafPage.h"
#include "db.h"
#include "bufmgr.h"
#include "system_defs.h"

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
	if (s == FAIL) {
		Page *p;
		returnStatus = MINIBASE_BM->NewPage(headerID, p);
		if (returnStatus == OK){
			this->header = (BTreeHeaderPage*)p; 
			this->header->Init(headerID); 
			this->header->SetRootPageID(INVALID_PAGE);
			//may need to initialize next and previous page to Invalid?
			returnStatus = MINIBASE_DB->AddFileEntry(filename, headerID);
		}
	}
	
	returnStatus = MINIBASE_BM->PinPage(headerID, (Page*&) this->header);

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
	MINIBASE_BM->UnpinPage(((HeapPage*)this->header)->PageNo(), true);
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
	/*BTreeFileScan* scan = this->OpenScan(NULL, NULL);

	while (scan->GetNext() != DONE) {
		scan->DeleteCurrent();
	}*/

	PageID rootPid;
	Status s;
	rootPid = header->GetRootPageID();

	if (rootPid == INVALID_PAGE) {
		return OK;
	} else {
		s = this->DestroyHelper(rootPid);
		if (s != OK) {
			cout << "First call to destroy helper failed" << endl;
			return s;
		}
		s = MINIBASE_BM->FreePage(rootPid);
		if (s != OK) {
			cout << "Freeing root failed" << endl;
			return s;
		}
		MINIBASE_DB->DeleteFileEntry(this->dbfile);
	}
}

Status BTreeFile::DestroyHelper(PageID currPid) {
	ResizableRecordPage* currPage;
	Status s = OK;

	PIN(currPid, currPage);

	if (currPage->GetType() == INDEX_PAGE) {
		IndexPage* indexPage = (IndexPage*) currPage;
		PageKVScan<PageID>* iter = new PageKVScan<PageID>();
		indexPage->OpenScan(iter); // maybe include if stuff breaks

		char* currKey;
		PageID currID;
		Status ds = OK;

		while (iter->GetNext(currKey, currID) != DONE) {
			ds = this->DestroyHelper(currID);
			if (ds != OK) {
				cout << "Destroy helper failed" << endl;
				return ds;
			}
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
		
		UNPIN(currPid, CLEAN);

		//s = MINIBASE_BM->FreePage(currPid);

		return s;
	} else {
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
	// TODO: ensure all statuses are handled
	//cout << "Call to insert: " << key << endl;
	PageID rootPid;
	Status s;
	rootPid = header->GetRootPageID();
	// If no root page, create one
	if(rootPid == INVALID_PAGE) {
		cout << "No Root, creating" << endl;
		LeafPage* leafpage;
		s = MINIBASE_BM->NewPage(rootPid, (Page*&)leafpage);
		if(s == OK){
			// Need to initialize?
			leafpage->Init(rootPid, LEAF_PAGE);
			leafpage->SetNextPage(INVALID_PAGE);
			leafpage->SetPrevPage(INVALID_PAGE);
			s = leafpage->Insert(key,rid);

			//Make this the root page
			header->SetRootPageID(rootPid);
			UNPIN(rootPid, DIRTY);
			return s;
		} else {
			return s;
		}
	} else { //there is root already
		//cout << "Traversing" << endl;

		PageID currPid = rootPid;
		PageID insertPid = NULL;

		SplitStatus split;
		char * new_child_key;
		PageID new_child_pageid;

		s = this->InsertHelper(currPid, split, new_child_key, new_child_pageid, key, rid);

		if (split == NEEDS_SPLIT) { // needs to split root
			cout << "Needs to split root" << endl;
			ResizableRecordPage* currPage;
			PIN(rootPid, currPage);

			// new root
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
			if (currPage->GetType() == INDEX_PAGE) {
				IndexPage* indexPage = (IndexPage*) currPage;
				s2 = indexPage->GetMinKey(minKey);
			} else if(currPage->GetType() == LEAF_PAGE) {
				LeafPage* leafPage = (LeafPage*) currPage;
				s2 = leafPage->GetMinKey(minKey);
			} else {
				// huh
			}

			if (s2 != OK) {
				cout << "Error getting MinKey in Insert split root" << endl;
				return s2;
			}

			s2 = newIndexPage->Insert(minKey, rootPid);
			s2 = newIndexPage->Insert(new_child_key, new_child_pageid);

			if (s2 != OK) {
				cout << "Error inserting into new root" << endl;
				return s2;
			}

			this->header->SetRootPageID(newIndexPid);

			UNPIN(newIndexPid, DIRTY);
			UNPIN(rootPid, DIRTY);
		}

		if(s != OK) {
			return s;
		} else {
			return OK;
		}
	}
}

//recursive helper function, the bool return type and parentPid is used for splitting recursively, i.e. if it returns 
Status BTreeFile::InsertHelper(PageID currPid, SplitStatus& st, char*& newChildKey, PageID & newChildPageID, const char *key, const RecordID rid) {
	ResizableRecordPage* currPage;
	PageID nextPid;
	Status s = OK;
	Status s2 = OK;

	SplitStatus split;
	char * new_child_key;
	PageID new_child_pageid;

	PIN(currPid, currPage);

	if (currPage->GetType() == INDEX_PAGE) {
		//cout << "Insert helper into index page" << endl;
		IndexPage* indexPage = (IndexPage*) currPage;
		PageKVScan<PageID>* iter = new PageKVScan<PageID>();
		//indexPage->OpenScan(iter); // maybe include if stuff breaks
		indexPage->Search(key, *iter);
		char * c;
		iter->GetNext(c, nextPid);
		// may need to deallocate iter
		delete iter;
		
		s = this->InsertHelper(nextPid, split, new_child_key, new_child_pageid, key, rid); // traverse to child

		if (split == NEEDS_SPLIT) {
			// split this child index node, will need to insert new leftval into this Index page with
			// pointer to new child node

			if (indexPage->Insert(new_child_key, new_child_pageid) != OK) {
				// split this page.

				// make new page
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
				s2 = this->SplitIndexPage(indexPage, newIndexPage, new_child_key, new_child_pageid);
				if (s2 != OK) {
					cout << "Error in Split Index" << endl;
					return s2;
				}

				st = NEEDS_SPLIT; // propagate up a level of recursion
				newIndexPage->GetMinKey(newChildKey);
				newChildPageID = newIndexPid;
				
				UNPIN(newIndexPid, DIRTY);
			}

			UNPIN(currPid, DIRTY);
			return s;
		} else {
			UNPIN(currPid, CLEAN);
			return s;
		}
	} else if(currPage->GetType() == LEAF_PAGE) {
		//cout << "Insert helper into leaf page" << endl;

		// in the case that this leaf page needs to split:
			// split
			// set st to true, set newChildKey and newChildPageID
		LeafPage* leafPage = (LeafPage*) currPage;
		if (leafPage->Insert(key, rid) != OK) {
			// split this page.
			//cout << "insert into leaf failed, needs to split" << endl;
			// make new page
			LeafPage* newLeafPage;
			PageID newLeafPid;
			s2 = MINIBASE_BM->NewPage(newLeafPid, (Page*&)newLeafPage);
			if (s2 != OK) {
				cout << "Error allocating new leaf page in InsertHelper split" << endl;
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

Status BTreeFile::SplitLeafPage(LeafPage* oldPage, LeafPage* newPage, const char *key, const RecordID rid) {

	//cout << "Call to split leaf page" << endl;
	Status s1 = OK;
	// replace scans with pointer swapping in the future
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

	while (oldScan->GetNext(currKey, currID) != DONE) {
		ds = newPage->Insert(currKey, currID);
		if (ds != OK) {
			std::cout << "SplitLeafPage transfer insert failed" << std::endl;
			return ds;
		}
		ds = oldScan->DeleteCurrent();
		if (ds != OK) {
			std::cout << "SplitLeafPage move delete failed" << std::endl;
			return ds;
		}
	}

	delete oldScan;

	//cout << "DEBUG: size of oldPage should be 0:" << oldPage->GetNumOfRecords() << endl;
	//cout << "DEBUG: size of newPage should be 59: " << newPage->GetNumOfRecords() << endl;

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

	while (oldPage->AvailableSpace() > newPage->AvailableSpace()) {
		if (strcmp(currKey, key) < 0) { // currKey < key
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
		} else if (strcmp(currKey, key) > 0 && insertedNew == false) { // currKey > key
			ds = oldPage->Insert(key, rid);
			if (ds != OK) {
				cout << "Insert new key in split failed SplitLeafPage" << endl;
				return ds;
			}
		} else {
			// huh, should not reach here
			cout << "Don't know how this got printed" << endl;
			return FAIL;
		}
	}

	delete newScan;

	if (insertedNew == false) {
		ds = newPage->Insert(key, rid);
		if (ds != OK) {
			cout << "Insert new key after split failed SplitLeafPage" << endl;
			return ds;
		}
	}

	// prev/next pointers
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
	}

	return ds;
}

// Lots of duplicated code, may try to template out
Status BTreeFile::SplitIndexPage(IndexPage* oldPage, IndexPage* newPage, const char *key, const PageID rid) {
	//cout << "Call to splitindexpage" << endl;

	Status s1 = OK;

	// replace scans with pointer swapping in the future
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
			cout << "IndexSplitPage transfer insert failed" << endl;
			return ds;
		}
		ds = oldScan->DeleteCurrent();
		if (ds != OK) {
			cout << "IndexSplitPage move delete failed" << endl;
			return ds;
		}
	}

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
		if (strcmp(currKey, key) < 0) { // currKey < key
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
		} else if (strcmp(currKey, key) > 0 && insertedNew == false) { // currKey > key
			ds = oldPage->Insert(key, rid);
			if (ds != OK) {
				cout << "Insert new key in split failed SplitIndexPage" << endl;
				return ds;
			}
		} else {
			// huh, should not reach here
			cout << "Don't know how this got printed 2" << endl;
			return FAIL;
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
	
	// prev/next pointers
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
	}

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
