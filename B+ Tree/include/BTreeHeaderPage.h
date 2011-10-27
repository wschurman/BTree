#ifndef BT_HEADER_PAGE_H_
#define BT_HEADER_PAGE_H_

#include "heappage.h"

class BTreeHeaderPage : HeapPage {

private:
    //Don't add any private members. 	
public:

	// Initializes the header page and sets the root to be invalid.
	void Init(PageID hpid) {
		HeapPage::Init(hpid);
		SetRootPageID(INVALID_PAGE);
	}

	// Returns the page id of the root.
	PageID GetRootPageID() {
		return *((PageID*) HeapPage::data);
	}

	// Sets the page id of the root. 
	void SetRootPageID(PageID pid) {
		PageID* ptr = (PageID*) (HeapPage::data);
		*ptr = pid;
	}
};


#endif