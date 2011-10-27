#ifndef _SORTED_KV_PAGE_
#define _SORTED_KV_PAGE_

#include "ResizableRecordPage.h"
#include "PageKVScan.h"

template<typename ValType>
class SortedKVPage : public ResizableRecordPage {

private:
	//-------------------------------------------------------------------
	// SortedKVPage::FindKey
	//
	// Input   : key, the key to search for
	// Output  : rid, the RecordID of the key on this page. 
	// Return  : OK   if the key was found on the page and rid is set.
	//           DONE if the key was not found, but rid was set to the 
	//                largest key smaller than the search key.
	//           FAIL if there is no key on this page smaller than the search key.
	// Purpose : Private search function to locate keys. External callers should 
	// 	         use SortedKVPage::Search.
	//-------------------------------------------------------------------
	Status FindKey(const char* key, RecordID& rid) {
		// The page is empty if it contains only one empty slot.
		if(numOfSlots == 1 && SlotIsEmpty(GetFirstSlotPointer())) {
			return FAIL;
		}
		
		char* firstKey;
		// There is no first key, so the page is empty.
		if(GetMinKey(firstKey) == FAIL) {
			return FAIL;
		}
		// The search key is smaller than all keys on this page. 
		else if(strcmp(firstKey, key) > 0) {
			return FAIL; 
		}

		rid.pageNo = pid;
		for (int i = 0; i < numOfSlots; i++) {
			assert(!SlotIsEmpty(GetFirstSlotPointer() - i));

			Slot* slot = GetFirstSlotPointer() - i;

			const char* str = data + slot->offset;

			// We find the key directly.
			if(strcmp(str, key) == 0) {
				rid.slotNo = i;
				return OK;
			}
			// When we see a key larger than the search key, then we set 
			// rid to point to previous key and return DONE.
			else if(strcmp(str, key) > 0) {
				rid.slotNo = i - 1;
				return DONE;
			}
		}
		rid.slotNo = numOfSlots - 1;
		return DONE;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::PrintPID
	//
	// Input   : pid, The PageID to print. 
	// Output  : None.
	// Return  : None.
	// Purpose : Helper function that prints a page id 
	//           or 'INVALID_PAGE' as appropriate.
	//-------------------------------------------------------------------
	void PrintPID(PageID pid) {
		if(pid == INVALID_PAGE)
			std::cout << "INVALID_PAGE";
		else			
			std::cout << pid;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::PrintRecord
	//
	// Input   : slotNo, The slot number pointing to the record to print.
	// Output  : None.
	// Return  : None.
	// Purpose : Prints the record (key + list of values) stored at 
    //           the given sot.
	//-------------------------------------------------------------------
	void PrintRecord(int slotNo) {
		if(slotNo < 0 || slotNo >= numOfSlots) {
			return;
		}

		Slot* slot = GetFirstSlotPointer() - slotNo;

		if((SlotIsEmpty(slot) && numOfSlots == 1)) {
			return;
		}
		assert(!SlotIsEmpty(slot));

		// Print the key
		char* start = data + slot->offset;
		std::cout << start << "[";

		// Print the array of values. Note that this assume that ValType 
		// can be printed with cout
		ValType* valArray = (ValType*) (start + strlen(start) + 1);
		int numVals = (slot->length - strlen(start) - 1) / sizeof(ValType);
		for(int j = 0; j < numVals; j++) {
			std::cout << *(valArray + j);
			if(j != numVals - 1)
				std::cout << " ";
		}
		std::cout << "]" << std::endl;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::PrintPageInfo
	//
	// Input   : None.
	// Output  : None.
	// Return  : None.
	// Purpose : Prints metadata about this page. 
	//-------------------------------------------------------------------
	void PrintPageInfo() {
		std::cout << "page_id: "; PrintPID(pid); std::cout << " ";

		std::cout << "type: ";
		if(type == 0/*INDEX_PAGE*/) {
			std::cout << "INDEX_PAGE ";
		}
		else {
			std::cout << "LEAF_PAGE ";
		}


		std::cout << "prevPage: "; PrintPID(prevPage); std::cout << " ";
		std::cout << "nextPage: "; PrintPID(nextPage); std::cout << std::endl;
		std::cout << "numRecords: " << numOfSlots << " freeSpace: " << freeSpace << " ";
		if(numOfSlots > 0) {
			Slot* first = GetFirstSlotPointer();
			Slot* last = GetFirstSlotPointer() - (numOfSlots - 1);
			std::cout << "minKey: " << (data + first->offset) << " ";
			std::cout << "maxKey: " << (data + last->offset);
		}
		std::cout << std::endl;
	}





public:

	//-------------------------------------------------------------------
	// SortedKVPage::Init
	//
	// Input   : pid, the PageID of this page.
	//           indexType, the type of this page 
	//                      -- whether it is index or leaf.
	// Output  : None. 
	// Return  : None.
	// Purpose : Initializes this page the appropriate type.
	//-------------------------------------------------------------------
	void Init(PageID pid, short indexType) {
		HeapPage::Init(pid);
		type = indexType;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::GetMinKey
	//
	// Input   : None
	// Output  : minKey, A pointer to the smallest key on the page.
	// Return  : OK   if minKey was set correctly.
	//           FAIL if the page is empty. 
	// Purpose : Gets the smallest key on the page. 
	//-------------------------------------------------------------------
	Status GetMinKey(char*& minKey) {
		// Page is empty
		if(IsEmpty()) {
			return FAIL;
		}
		Slot* slot = GetFirstSlotPointer();
		minKey = data + slot->offset;
		return OK;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::GetMinKeyValue
	//
	// Input   : None
	// Output  : minKey, A pointer to the smallest key on the page.
	//           minVal, The first value associated with the given key.
	// Return  : OK   if minKey was set correctly.
	//           FAIL if the page is empty. 
	// Purpose : Gets the smallest key on the page and the first value associated
	//           with that key.
	//-------------------------------------------------------------------
	Status GetMinKeyValue(char*& minKey, ValType& minVal) {
		if(GetMinKey(minKey) == FAIL) {
			return FAIL;
		}

		minVal = *((ValType*) (minKey + strlen(minKey) + 1));
		return OK;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::GetMaxKey
	//
	// Input   : None
	// Output  : maxKey, A pointer to the largest key on the page.
	// Return  : OK   if maxKey was set correctly.
	//           FAIL if the page is empty. 
	// Purpose : Gets the largest key on the page. 
	//-------------------------------------------------------------------
	Status GetMaxKey(char*& maxKey) {
		// Page is empty
		if(IsEmpty()) {
			return FAIL;
		}
		Slot* slot = GetFirstSlotPointer() - (numOfSlots - 1);
		maxKey = data + slot->offset;
		return OK;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::GetMaxKeyValue
	//
	// Input   : None
	// Output  : maxKey, A pointer to the largest key on the page.
	//           maxVal, The first value associated with the given key.
	// Return  : OK   if maxKey was set correctly.
	//           FAIL if the page is empty. 
	// Purpose : Gets the largest key on the page and the largest value associated
	//           with that key.
	//-------------------------------------------------------------------
	Status GetMaxKeyValue(char*& maxKey, ValType& maxVal) {
		if(IsEmpty()) {
			return FAIL;
		}

		Slot* slot = GetFirstSlotPointer() - (numOfSlots - 1);
		maxKey = data + slot->offset;
		maxVal = *((ValType*)(data + slot->offset + slot->length - sizeof(ValType)));
		return OK;
	}

	


	//-------------------------------------------------------------------
	// SortedKVPage::Insert
	//
	// Input   : key, the key to insert.
	//           val, the value to insert. 
	// Output  : None. 
	// Return  : OK   if the key-value pair was inserted successfully. 
	//           FAIL if there is no space for the key value pair, 
	//                or another error occurred.
	// Purpose : Inserts a key value pair into this page. If the key is already 
	//           present on the page, val will be appended to the existing record.
	// 	         Otherwise a new record will be created. 
	//-------------------------------------------------------------------
	Status Insert(const char* key, ValType val) {
		RecordID rid;
		if(FindKey(key, rid) == OK) {
			if(AvailableSpaceForAppend() < sizeof(ValType)) {
				return FAIL;
			}

			//RecordID rid;
			return AppendToRecord((char*) &val, sizeof(ValType), rid);
			
		}
		else {
			int recSize = strlen(key) + 1 + sizeof(ValType);
			if(AvailableSpace() < recSize) {
				return FAIL;
			}

			//Create record to pass into insert. 
			char recPtr[200]; 
			memcpy(recPtr, key, strlen(key) + 1);
			memcpy(recPtr + strlen(key) + 1, &val, sizeof(ValType));

			RecordID rid2;
			if(HeapPage::InsertRecord(recPtr, recSize, rid2) != OK) {
				return FAIL;
			}
			//Record has been put into last slot. We will remove it and 
			//add it to the appropriate location based on the sort order.
			assert(rid2.slotNo == numOfSlots - 1);

			//Slot array of length 1 is trivially sorted. 
			if(numOfSlots == 1) {
				return OK;
			}

			Slot* keySlot = GetFirstSlotPointer() - rid2.slotNo; 
			int keyOffset = keySlot->offset;
			int keyLength = keySlot->length;

			for(int i = 0; i < numOfSlots; i++) {
				Slot* slot = GetFirstSlotPointer() - i;
				char* keyInSlot = data + slot->offset;

				// We've found the location at which to insert the new slot. 
				if(strcmp(key, keyInSlot) <= 0) {
					
					// Move this slot and all following slots down one position. 
					Slot* dest = GetFirstSlotPointer() - (numOfSlots - 1);
					Slot* src = GetFirstSlotPointer() - (numOfSlots - 2);

					// Want to mv all slots, except the last slot and those 
					// preceding slot i.
					int mvLength = (numOfSlots - 1 - i) * sizeof(Slot);
					memmove(dest, src, mvLength);

					// Update slot at appropriate location. 
					slot->offset = keyOffset;
					slot->length = keyLength;

					return OK;
				}
			}
		}
		return FAIL;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::Delete
	//
	// Input   : key, the key to delete.
	//           val, the value to delete.
	// Output  : None. 
	// Return  : OK   if the key-value pair was deleted successfully. 
	//           FAIL If the key-value pair is not present or the underlying
	//                delete fails. 
	// Purpose : Deletes a key-value pair from this page. Compacts the records
	//           and slot array. 
	//-------------------------------------------------------------------
	Status Delete(const char* key, ValType val) {
		RecordID rid;
		if(FindKey(key, rid) != OK) {
			return FAIL;
		}

		Slot* slot = GetFirstSlotPointer() - rid.slotNo;

		int numVals = (slot->length - (strlen(key) + 1)) / sizeof(ValType);
		ValType* valPtr = (ValType*)(data + slot->offset + strlen(key) + 1);

		// The value we are deleting is the only value for this key 
		// (on this page), so we delete the key as well.
		if(numVals == 1) {
			return DeleteKey(key);
		}

		//Else iterate through values and cut the one that matches.  
		for(int i = 0; i < numVals; i++) {
			if((*valPtr) == val) {
				return CutFromRecord(strlen(key) + 1 + i*sizeof(ValType), sizeof(ValType), rid);
			}
			valPtr += 1;
		}
		return FAIL;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::DeleteKey
	//
	// Input   : key, the key to delete.
	// Output  : None. 
	// Return  : OK   if the key was deleted successfully.
	//           FAIL If the key is not present or the underlying
	//                delete fails. 
	// Purpose : Deletes a key and all values associated with it. 
	//-------------------------------------------------------------------
	Status DeleteKey(const char* key) {
		RecordID rid;

		if(FindKey(key, rid) != OK) {
			return FAIL;
		}

		if(HeapPage::DeleteRecord(rid) == FAIL) {
			return FAIL;
		}

		// If we deleted the record in the last slot, 
		// then the slot array was already compacted.
		if(rid.slotNo == numOfSlots) {
			return OK;
		}

		// Otherwise we need to compact slot array. This will change the
		// record id of records on this page with slots after the slot 
		// that was deleted!
		assert(SlotIsEmpty(GetFirstSlotPointer() - rid.slotNo));
		Slot* mvSource = GetFirstSlotPointer() - (numOfSlots - 1);
		Slot* mvDest = mvSource + 1;
		int mvLength = numOfSlots * sizeof(Slot) - ((rid.slotNo + 1) * sizeof(Slot));
		memmove(mvDest, mvSource, mvLength);
		numOfSlots--;
		freeSpace += sizeof(Slot);
		return OK;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::DeleteAll
	//
	// Input   : None
	// Output  : None. 
	// Return  : OK   if all keys were deleted correctly.
	//           FAIL if an underlying delete call failed. 
	//                
	// Purpose : Deletes all keys on the page, leaving this page empty. 
	//-------------------------------------------------------------------
	Status DeleteAll() {
		numOfSlots = 1;
		freePtr = 0;
		freeSpace = HEAPPAGE_DATA_SIZE - sizeof(Slot);
		SetSlotEmpty(GetFirstSlotPointer() - 0);
		return OK;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::Search
	//
	// Input   : key, the key to search for
	// Output  : scan, A ValueIterator initialized to the first value 
	//                    associated with the search key.
	// Return  : OK   if the key was found on the page and valIter is set.
	//           DONE if the key was not found, but valIter was set to the 
	//                largest key smaller than the serach key.
	//           FAIL if there is no key on this page smaller than the search key.
	// Purpose : Search function to locate keys. 
	//-------------------------------------------------------------------
	Status Search(const char* key, PageKVScan<ValType>& scan) {
		RecordID rid;
		Status retStat = FindKey(key, rid);


		if(retStat != FAIL) {
			assert(pid == rid.pageNo);
			scan.reset(this, rid);
		}		
		return retStat;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::Contains
	//
	// Input   : key, the to test for.
	//           val, the value to test for.
	// Output  : None. 
	// Return  : true  If the key value pair is present in this page 
    //           false otherwise.      
	// Purpose : Checks whether the given key-value pair is 
	//           present on this page. 
	//-------------------------------------------------------------------
	bool Contains(const char* key, ValType val) {
		PageKVScan<ValType> scan;
		if(Search(key, &scan) != OK) {
			return false;
		}

		char* nextKey;
		ValType nextVal;
		while(scan.GetNext(nextKey, nextVal) != DONE) {
			if(strcmp(key, nextKey) != 0) {
				return false;
			}
			else if(nextVal == val) {
				return true;
			}
		}
		return false;
	}


	//-------------------------------------------------------------------
	// SortedKVPage::ContainsKey
	//
	// Input   : key, the to test for.
	// Output  : None. 
	// Return  : true  If there is some value on this 
	//                 page with the specified key.
	// Purpose : Checks whether the given key is present on this page. 
	//-------------------------------------------------------------------
	bool ContainsKey(const char* key) {
		PageKVScan<ValType> scan;
		if(Search(key, &scan) != OK) {
			return false;
		}
		return true;
	}

	//-------------------------------------------------------------------
	// SortedKVPage::HasSpaceForValue
	//
	// Input   : key, the key associated with the value to check. 
	// Output  : None. 
	// Return  : true  if there is space on this page to insert the 
    //                 given key and a new value. 
	//           false otherwise.
	// Purpose : Checks whether there is space to insert a new key-value pair
	//           with the given key. Takes into account whether the key is already
	//           present on the page. 
	//-------------------------------------------------------------------
	bool HasSpaceForValue(const char* key) {
		RecordID rid;
		if(FindKey(key, rid) == OK) {
			return (AvailableSpaceForAppend() > sizeof(ValType));
		}
		else {
			return (AvailableSpace() > (int)(strlen(key) + 1 + sizeof(ValType)));
		}
	}


	//-------------------------------------------------------------------
	// SortedKVPage::GetNumValuesForKey
	//
	// Input   : key, the key.  
	// Output  : None. 
	// Return  : The number of value on this page asosciated 
	//           with the given key.
	// Purpose : Returns the number of values on this page 
	//           associated with the given key.
	//-------------------------------------------------------------------
	int GetNumValuesForKey(const char* key) {
		RecordID rid;
		if(FindKey(key, rid) == OK) {
			Slot* slot = GetFirstSlotPointer() - rid.slotNo;
			char* keyPtr = data + slot->offset;

			int numValues = (slot->length - (strlen(keyPtr) + 1)) / sizeof(ValType);
			return numValues;
		}
		else {
			return 0;
		}
	}



	//-------------------------------------------------------------------
	// SortedKVPage::OpenScan
	//
	// Input   : scan, Uninitialized PageKVScan.
	// Output  : Initialized PageKVScan object set at virst key-value on page.
	// Return  : OK.
	// Purpose : Opens a new scan on this page, starting at the 
    //           beginning of the page.
	//-------------------------------------------------------------------
    Status OpenScan(PageKVScan<ValType>* scan) {
		RecordID firstRid;
		firstRid.pageNo = pid;
		firstRid.slotNo = 0;
		scan->reset(this, firstRid);
		return OK;
    }



	
	//-------------------------------------------------------------------
	// SortedKVPage::PrintPage
	//
	// Input   : printContents, Indicates whether records should be printed.
	// Output  : None.
	// Return  : None.
	// Purpose : Prints metadata about this page and records if specified. 
	//-------------------------------------------------------------------
	void PrintPage(bool printContents = true) {
		PrintPageInfo();

		if(!printContents) {
			return;
		}

		std::cout << "--------------------- Page Contents --------------------------" << std::endl;

		for (int i = 0; i < numOfSlots; i++) {
			std::cout << i << ": ";

			if (!SlotIsEmpty(GetFirstSlotPointer() - i)) {
				Slot* slot = GetFirstSlotPointer() - i;
				std::cout << slot->offset << ", " << slot->length << ", ";
				PrintRecord(i);
			}
			else {
				std::cout << -1 << std::endl;
			}
		}
		std::cout << "--------------------------------------------------------------" << std::endl;
	}


};


#endif