#ifndef _PAGE_KV_SCAN_
#define _PAGE_KV_SCAN_

template<typename ValType> class SortedKVPage;

template<typename ValType>
class PageKVScan {
private:

	SortedKVPage<ValType>* page;
	int numValsWithKey, curValNum;
	char* curKey;
	RecordID curRid;
	ValType* curVal;

	//For deleting.
	char* prevKey;
	ValType* prevVal;
	int prevNumVals;


	// Private method that initializes the iterator to the 
	// first value of the key in the record rid. 
	void setKey(RecordID rid) {
		int recLen;
		page->ReturnRecord(rid, curKey, recLen);

		int keyLength = strlen(curKey) + 1;
		curVal = (ValType*)(curKey + keyLength);

		assert(((recLen - keyLength) % sizeof(ValType)) == 0);
		numValsWithKey = (recLen - keyLength) / sizeof(ValType);

		curValNum = 0;
	}

	// Initializes the iterator. Should only be called by methods in SortedKVPage. 
	void reset(SortedKVPage<ValType>* page, RecordID rid) {

		assert(rid.pageNo == page->PageNo());
		this->page = page;
		curRid = rid;
		prevKey = NULL;

		if(page->IsEmpty() || rid.slotNo < 0 || rid.slotNo >= page->GetNumOfRecords()) {
			curKey = NULL;
		}
		else {
			setKey(rid);
		}
		//started = false;

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
		//std::cout << "In GetNext: curRid="<< curRid << " curKey=" << curKey << std::endl;

		if(curKey == NULL) {
			prevKey = NULL;
			return DONE;
		}

		//std::cout << "In GetNext: curKey="<< curKey << std::endl;

		// Set output values.
		key = curKey;
		val = *curVal;

		//Update previous key/value for DeleteCurrent method.
		prevKey = curKey;
		prevVal = curVal;

		// We've exhausted all of the values with the same key.
		if(curValNum == numValsWithKey - 1) {
			RecordID nextRid;

			//There are no more keys on this page.
			if(page->NextRecord(curRid, nextRid) == DONE) {
				curKey = NULL;
			}
			//Move to the next key.
			else {
				//std::cout << "curRid: " << curRid << " nextRid: " << nextRid << std::endl;
				prevNumVals = numValsWithKey;
				setKey(nextRid);
				curRid = nextRid;
			}
		}
		else {
			curValNum++;
			curVal++;
		}

		//std::cout << "In GetNext: setting curKey="<< curKey << std::endl;
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
	// 	         by the last call to GetNext. 
	//-------------------------------------------------------------------
	Status DeleteCurrent() {
		if(prevKey == NULL) {
			return DONE;
		}

		int prevLength = strlen(prevKey) + 1;

		//std::cout << "In DeleteCurrent: Deleting " << prevKey << " " << *prevVal << std::endl;
		//std::cout << "curKey: " << curKey << " curVal: "<< *curVal << std::endl;
		if(page->Delete(prevKey, *prevVal) == FAIL) {
			return FAIL;
		}

		// If we haven't switched to a new record, 
		// decrement numValsWithKey and curVal.
		if(curValNum != 0) {
			numValsWithKey--;
			curValNum--;
			curVal--;

			//std::cout << "in case 1" << std::endl;
			  
		}
		// If we have moved to a new record, then the key will 
		// have been compacted as well.
		else {
			// We are deleting the only value associated with the previous key. 
			// In this case, we need to increase the shift amount by the previous key length 
			//
			//std::cout << "curKey: " << curKey << " curVal: "<< *curVal << std::endl;
			if(prevNumVals == 1) {
				curKey -= prevLength;
				curVal = (ValType*)(((char*)curVal) - prevLength);
				curRid.slotNo -= 1;
			}
			
			curKey -= sizeof(ValType);
			curVal -= 1; // decrements by sizeof(ValType)

			//std::cout << "curKey: " << curKey << " curVal: "<< *curVal << std::endl;
		}

		return OK;
	}

};



#endif