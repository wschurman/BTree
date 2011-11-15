#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "BTreeFile.h"
#include "BTreeFileScan.h"

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean Up the B+ tree scan.
//-------------------------------------------------------------------
BTreeFileScan::~BTreeFileScan ()
{
	//TODO: add your code here
}


//-------------------------------------------------------------------
// BTreeFileScan::BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Constructs a new BTreeFileScan object. Note that this constructor
//           is private and can only be called from 
//-------------------------------------------------------------------
BTreeFileScan::BTreeFileScan() {
	//Add our code here.
}

//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           keyPtr - and a pointer to it's key value.
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read
//           or if high key has been passed.
//-------------------------------------------------------------------

Status BTreeFileScan::GetNext (RecordID & rid, char*& keyPtr)
{	
	PIN(currentPageID, currentPage);
    if(this->done){
		return DONE;
    }

    while (!(this->done)) {

		Status s = scan->GetNext(keyPtr, rid);
		
		//std::cout << "GOT 2" << std::endl;
        if (s != DONE) {
			if (this->highKey == NULL || strcmp(keyPtr, this->highKey) <= 0) { //within upper bound
				if(this->lowKey == NULL || strcmp(keyPtr, this->lowKey) >= 0) { //within lower bound
					UNPIN(currentPageID, CLEAN);
                    return OK;
                } else {
					continue; //haven't reached range yet
                }
            } else { //exceeded upper bound
				this->done = true;
                rid.pageNo = INVALID_PAGE;
                rid.slotNo = -1;
                UNPIN(currentPageID, CLEAN);
                return DONE;
            }
		} else { // s == DONE; current scanned page = done
			currentPageID = currentPage->GetNextPage();
            if(currentPageID == INVALID_PAGE){
				//no more pages
				this->done = true;
                UNPIN(currentPage->PageNo(), CLEAN);
				delete scan;
                return DONE;
            }
			delete scan;
            UNPIN(currentPage->PageNo(), CLEAN);
			this->_SetIter();
			return GetNext(rid, keyPtr);
		}
	}
	return DONE;
}


//-------------------------------------------------------------------
// BTreeFileScan::DeleteCurrent
//
// Input   : None
// Output  : None
// Purpose : Delete the entry currently being scanned (i.e. returned
//           by previous call of GetNext()). Note that this method should
//           call delete on the page containing the previous key, but it 
//           does (and should) NOT need to redistribute or merge keys. 
// Return  : OK 
//-------------------------------------------------------------------
Status BTreeFileScan::DeleteCurrent () {  
	if (done) {
		return DONE;
	}
	PIN(currentPageID, currentPage);
	Status s = scan->DeleteCurrent();
	UNPIN(currentPageID, DIRTY);
	return s;
}

Status BTreeFileScan::_SetIter() {  
    PIN(currentPageID, currentPage);
	scan = new PageKVScan<RecordID>();
	currentPage->OpenScan(scan);
	UNPIN(currentPageID, CLEAN);

	return OK;
}