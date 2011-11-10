#ifndef _PAGE_KV_SCAN_
#define _PAGE_KV_SCAN_

template<typename ValType> class SortedKVPage;


template<typename ValType>
class PageKVScan {
private:
	SortedKVPage<ValType>* page;
	RecordID curRid;
	int curValNum, numValsWithKey;
	bool toInit;
	char* curKey;

	// Private method that initializes the iterator to the 
	// first value of the key in the record rid. 
	void setKey(RecordID rid, bool prev = false) {
		int recLen;
		page->ReturnRecord(rid, curKey, recLen);

		int keyLength = strlen(curKey) + 1;
		//curVal = (ValType*)(curKey + keyLength);

		assert(((recLen - keyLength) % sizeof(ValType)) == 0);
		numValsWithKey = (recLen - keyLength) / sizeof(ValType);

		if(prev) {
			curValNum = numValsWithKey - 1;
		}
		else {
			curValNum = 0;
		}
	}

	// Initializes the iterator. Should only be called by methods in SortedKVPage. 
	void reset(SortedKVPage<ValType>* page, RecordID rid) {
		toInit = true;
		assert(rid.pageNo == page->PageNo());
		this->page = page;
		curRid = rid;

		if(page->IsEmpty() || rid.slotNo < 0 || rid.slotNo >= page->GetNumOfRecords()) {
			curKey = NULL;
		}
		else {
			setKey(rid);
		}
		//started = false;

	}


	ValType GetVal(char* key, int valNum) {
		return *((ValType*)(curKey + strlen(key) + 1 + valNum * sizeof(ValType)));
	}

public:
	
	friend class SortedKVPage<ValType>;

	//-------------------------------------------------------------------
	// PageKVScan::GetNext
	//
	// Input   : None. 
	// Output  : key, Pointer to current key. 
    //           val, Pointer to current value. 	
	// Return  : OK   if successful. 
	//           DONE if there are no more key-value pairs on this page 
	// Purpose : Retrieves the next key-value pair on this page. 
	//-------------------------------------------------------------------
	Status GetNext(char*& key, ValType& val) {
		if(curKey == NULL || curRid.slotNo > page->GetNumOfRecords()) {
			curKey = NULL;
			return DONE;
		}

		// Return current key value pair directly. 
		if(toInit) {
			toInit = false;
		}

		//We've exhausted all of the keys with the given value. 
		else if(curValNum == numValsWithKey - 1) {
			RecordID nextRid;

			//There are no more keys on this page.
			if(page->NextRecord(curRid, nextRid) == DONE) {
				//curKey = NULL;
				return DONE;
			}
			//Move to the next key.
			else {
				setKey(nextRid);
				curRid = nextRid;
			}
		}
		else {
			curValNum++;
		}
		key = curKey;
		val = GetVal(curKey, curValNum);
		return OK;
	}


	//-------------------------------------------------------------------
	// PageKVScan::GetPrev
	//
	// Input   : None. 
	// Output  : key, Pointer to previous key. 
    //           val, Pointer to previous value. 	
	// Return  : OK   if successful. 
	//           DONE if there are no previous key-value pairs on this page. 
	//                Note that this will not return the GetPrevPage pointer
	//                on index pages!
	// Purpose : Retrieves the previous key-value pair on this page. 
	//-------------------------------------------------------------------
	Status GetPrev(char*& key, ValType& val) {
		if(curKey == NULL) {
			return DONE;
		}

		// Return current key value pair directly. 
//		if(toInit) {
//			toInit = false;
//		}

		//We've exhausted all of the keys with the given value. 
		if(curValNum == 0) {
			RecordID nextRid;

			//There are no more keys on this page.
			if(curRid.slotNo == 0) {
				curKey = NULL;
				return DONE;
			}
			//Move to the next key.
			else {
				curRid.slotNo -= 1;
				//std::cout << "curRid: " << curRid << " nextRid: " << nextRid << std::endl;
				//prevNumVals = numValsWithKey;
				setKey(curRid);
			}
		}
		else {
			curValNum--;
		}
		key = curKey;
		val = GetVal(curKey, curValNum);
		return OK;

	}



	//-------------------------------------------------------------------
	// PageKVScan::DeleteCurrent
	//
	// Input   : None. 
	// Output  : None.
	// Return  : OK   if successful. 
	//           DONE if there have been no calls to GetNext 
	//                or the last call returned DONE.
	//           FAIL if the underlying call to Delete failed. 
	// Purpose : Deletes the "current" key-value pair, i.e. the one returned 
	// 	         by the last call to GetNext or GetPrev. After this has been called. 
	//           The "cursor" will be reset to immediately before the deleted 
	//           keyValue pair.
	//-------------------------------------------------------------------
	Status DeleteCurrent() {
		if(toInit || curKey == NULL || page->IsEmpty()) {
			return DONE;
		}

		char* keyToDelete = curKey;
		ValType valToDelete = GetVal(curKey, curValNum);

		if(curRid.slotNo == 0 && page->GetNumOfRecords() == 1 && numValsWithKey == 1) {
			curKey = NULL;
		}
		else {
			toInit = true;
		}

		if(page->Delete(keyToDelete, valToDelete) == FAIL) {
			return FAIL;
		}

		return OK;
	}
};

#endif