#include "BTreeTest.h"
#include "bufmgr.h"
#include <vector>

//-------------------------------------------------------------------
// BTreeDriver::toString
//
// Input   : n,   The number to convert
//           pad, The number of digits to use. Should be larger than the 
//                number of digits in n. Leading digits will be 0. 
// Output  : str, The converted number. 
// Return  : None
// Purpose : Converts a number of a strings so that it can be used as a key. 
//-------------------------------------------------------------------
void BTreeDriver::toString(const int n, char* str, int pad) {

	char format[200];
	sprintf(format, "%%0%dd", pad);
	//std::cout << "format: "	 << format << std::endl;
	//pad the prefix of the string with 0's
	sprintf(str, format, n);
	//cout<<str<<endl;
}

//-------------------------------------------------------------------
// BTreeDriver::InsertKey
//
// Input   : btf,  The BTree to insert into. 
//           key,  The integer key.
//           ridOffset, The offset used to compute the rid. 
//                   rid = (key + ridOffset, key + ridOffset + 1)
// Output  : None
// Return  : True if this operation completed succesfully. 
// Purpose : Inserts a single key into the B-Tree. 
//-------------------------------------------------------------------
bool BTreeDriver::InsertKey(BTreeFile* btf, int key, int ridOffset, int pad) {


	char skey[MAX_KEY_LENGTH];
	BTreeDriver::toString(key, skey, pad);
	RecordID rid;
	rid.pageNo=key + ridOffset;
	rid.slotNo=key + ridOffset + 1;
	if(btf->Insert(skey, rid) != OK) {
		std::cerr << "Inserting key " << skey << " failed"  << std::endl;
		return false;
	}
	return true;
}


//-------------------------------------------------------------------
// BTreeDriver::InsertRange
//
// Input   : btf,  The BTree to insert into. 
//           low,  The beginning of the range (inclusive).
//           high, The end of the range (inclusive).
//           ridOffset, The offset used to compute the rid. 
//           pad,  The amount of padding to use for the keys.
//           reverse, Whether to insert in reverse order. 
// Output  : None
// Return  : True if this operation completed succesfully. 
// Purpose : Inserts a range of keys into the B-Tree. 
//-------------------------------------------------------------------
bool BTreeDriver::InsertRange(BTreeFile* btf, int low, int high, 
                              int ridOffset, int pad, bool reverse) {

	int numKeys = high - low + 1;
	int keyNum;

	char skey[MAX_KEY_LENGTH];

	int i;
	if(reverse)
		i = numKeys - 1;
	else
		i = 0;

	while(reverse && i >= 0 || !reverse && i < numKeys) {
		RecordID rid;
		keyNum = i+low;
		rid.pageNo=keyNum + ridOffset;
		rid.slotNo=keyNum + ridOffset + 1;
		BTreeDriver::toString(keyNum, skey, pad);

		if (btf->Insert(skey, rid) != OK) {
			std::cerr << "Insertion of range failed at key=" << skey 
				      << " rid=" << rid << std::endl;
			minibase_errors.show_errors();
			return false;
		}

		if(reverse)
			i--;
		else
			i++;
	}
	return true;
}


//-------------------------------------------------------------------
// BTreeDriver::TestPresent
//
// Input   : btf,  The BTree to test.
//           key,  The key to test. 
//           ridOffset, The offset used to compute the rid. 
//           pad,  The amount of padding to use for the keys.
// Output  : None
// Return  : True if the key was present in the tree. 
// Purpose : Tests whether a key is present in the tree. 
//-------------------------------------------------------------------
bool BTreeDriver::TestPresent(BTreeFile* btf, int key, int ridOffset, int pad) {
	char skey[MAX_KEY_LENGTH];
	BTreeDriver::toString(key, skey, pad);
	RecordID rid;
	rid.pageNo = key + ridOffset;
	rid.slotNo = key + ridOffset + 1;

	BTreeFileScan* scan = btf->OpenScan(skey, NULL);
	if(scan == NULL) {
		std::cerr << "Error opening scan. " << std::endl;
		return false;
	}

	char* curKey;
	RecordID curRid;
	bool ret = false;

	while(scan->GetNext(curRid, curKey) != DONE) {
		if(strcmp(curKey, skey) == 0 && curRid == rid) {
			ret = true;
			break;
		}
		else if(strcmp(curKey, skey) > 0) {
			break;
		}
	}

	if(!ret) {
		std::cerr << "Error: Expected key " << skey << " not present.";
	}

	delete scan;
	return ret;
}

//-------------------------------------------------------------------
// BTreeDriver::TestAbsent
//
// Input   : btf,  The BTree to test.
//           key,  The key to test. 
//           ridOffset, The offset used to compute the rid. 
//           pad,  The amount of padding to use for the keys.
// Output  : None
// Return  : True if the key was not present in the tree. 
// Purpose : Tests whether a key is absent from the tree. 
//-------------------------------------------------------------------
bool BTreeDriver::TestAbsent(BTreeFile* btf, int key, int ridOffset, int pad) {
	char skey[MAX_KEY_LENGTH];
	BTreeDriver::toString(key, skey, pad);
	RecordID rid;
	rid.pageNo = key + ridOffset;
	rid.slotNo = key + ridOffset + 1;

	BTreeFileScan* scan = btf->OpenScan(skey, NULL);
	if(scan == NULL) {
		std::cerr << "Error opening scan. " << std::endl;
		return false;
	}

	char* curKey;
	RecordID curRid;
	bool ret = true;

	while(scan->GetNext(curRid, curKey) != DONE) {
		if(strcmp(curKey, skey) == 0 && curRid == rid) {
			std::cerr << "Error: Unexpected key " << skey << " present.";
			ret = false;
			break;
		}
		else if(strcmp(curKey, skey) > 0) {
			break;
		}
	}
	delete scan;
	return ret;
}


//-------------------------------------------------------------------
// BTreeDriver::InsertDuplicates
//
// Input   : btf,  The BTree to insert into. 
//           key,  The key to use for all duplicates. 
//           numDups, The number of duplicates to insert  
//           startOffset, The starting rid offset to use. It will
//                        be incremented for each duplicate. 
//           pad,  The amount of padding to use for the keys.
// Output  : None
// Return  : True if the operation completed successfully. 
// Purpose : Inserts duplicates into a B-Tree. 
//-------------------------------------------------------------------
bool BTreeDriver::InsertDuplicates(BTreeFile* btf, int key, int numDups,
	                               int startOffset, int pad) {

	char skey[MAX_KEY_LENGTH];
	BTreeDriver::toString(key, skey, pad);

    for(int i = 0; i < numDups; i++) {
		RecordID rid;
		rid.pageNo = key + startOffset + i;
		rid.slotNo = key + startOffset + i + 1;
		if(btf->Insert(skey, rid) != OK) {
			std::cerr << "Insertion of duplicate key=" << skey 
				      << " failed at rid=" << rid << std::endl;
			return false;
		}
	}
	return true;
}
/*
bool BTreeDriver::DeleteStride(BTreeFile* btf, int low, int high,
	                           int stride, int pad) {

	char lowKey[MAX_KEY_LENGTH];
	char highKey[MAX_KEY_LENGTH];
    BTreeDriver::toString(low, lowKey, pad);
    BTreeDriver::toString(high, highKey, pad);

	BTreeFileScan* scan = btf->OpenScan(lowKey, highKey);
	RecordID rid;
	char* curKey;
	int count = 0;

	std::vector<std::pair<char*, RecordID>*> deleteVec;

	while(scan->GetNext(rid, curKey) != DONE) {

		char* tempKey = new char[MAX_KEY_LENGTH];
		memcpy(tempKey, curKey, strlen(curKey) + 1);

		//std::cout << "Considering "	<< tempKey << " " << rid << " count: " << count << std::endl;

		if(count == 0) {
			deleteVec.push_back(new std::pair<char*, RecordID>(tempKey, rid));
		}
		count = (count + 1)	% stride;
	}

	delete scan;

	int numToDelete = deleteVec.size();
	int numDeleted = 0;
	//std::cout << "numToDelete "	<< numToDelete << std::endl;

	std::vector<std::pair<char*, RecordID>*>::iterator it;
	for(it = deleteVec.begin(); it != deleteVec.end(); it++) {
		std::pair<char*, RecordID>* pair = *it;
		//std::cout << "attempting to delete " << pair->first << " " << pair->second << std::endl;

		if(btf->Delete(pair->first, pair->second) == OK) {
			numDeleted++;
		}
	}

	for(it = deleteVec.begin(); it != deleteVec.end(); it++) {
		delete [] (*it)->first;
		delete *it;
	}

	if(numDeleted != numToDelete) {
		std::cerr << "Error: Attempted to delete " << numToDelete 
         	      << "pairs. Only able to delete " << numDeleted << std::endl;
		return false;
	}

	return true;
}
*/


//-------------------------------------------------------------------
// BTreeDriver::TestNumLeafPages
//
// Input   : btf,  The BTree to test
//           expected,  The number of leaf pages you expect. 
// Output  : None
// Return  : True if the number of leaf pages = epxected. 
// Purpose : Tests the number of leaf pages in the tree. 
//-------------------------------------------------------------------
bool BTreeDriver::TestNumLeafPages(BTreeFile* btf, int expected) {
	PageID pid = btf->GetLeftLeaf();
	if(pid == INVALID_PAGE) {
		std::cerr << "Unable to access left leaf" << std::endl;
		return false;
	}
	int numPages = 0;

	while(pid != INVALID_PAGE) {
		numPages++;
		

		LeafPage* leaf;
		if(MINIBASE_BM->PinPage(pid, (Page*&)leaf) == FAIL) {
			std::cerr << "Unable to pin leaf page" << std::endl;
			return false;
		}

		pid = leaf->GetNextPage();

		if(MINIBASE_BM->UnpinPage(leaf->PageNo(), CLEAN) == FAIL) {
			std::cerr << "Unable to unpin leaf page" << std::endl;
			return false;
		}

	}

	if(numPages != expected) {
		std::cerr << "Unexpected number of leaf pages. Expected " << expected
			      << " Got " << numPages << std::endl;
		return false;
	}

	return true;
}

//-------------------------------------------------------------------
// BTreeDriver::TestNumLeafPages
//
// Input   : scan,  The BTreeFileScan object to test.  
//           expected,  The number of elements you expect in the scan. 
// Output  : None
// Return  : True if the number of elements scanned = epxected. 
// Purpose : Tests the number of elements in a scan. 
//-------------------------------------------------------------------
bool BTreeDriver::TestScanCount(BTreeFileScan* scan, int expected) {
	RecordID rid;
	char* prevKey = NULL;
	char* keyPtr;
	int numEntries = 0;
	while(scan->GetNext(rid, keyPtr) != DONE) {
		if(prevKey != NULL && strcmp(prevKey, keyPtr) > 0) {
			std::cerr << "Error: Keys are not sorted. Saw" 
				      << prevKey << " before " << keyPtr << std::endl;
			return false;
		}
		//std::cout << "key: " << keyPtr <<  " ";
		numEntries++;
	}

	if(numEntries != expected) {
		std::cerr << "Unexpected number of entries in scan. Expected " << expected 
			      << " Got " << numEntries << std::endl;
		return false;
	}
	return true;
}

//-------------------------------------------------------------------
// BTreeDriver::TestNumEntries
//
// Input   : btf,  The B-Tree to test. 
//           expected,  The expected number of elements in the tree. 
// Output  : None
// Return  : True if the number of elements = epxected. 
// Purpose : Tests the number of elements in a tree. 
//-------------------------------------------------------------------
bool BTreeDriver::TestNumEntries(BTreeFile* btf, int expected) {
	BTreeFileScan* scan = btf->OpenScan(NULL, NULL);
	bool test = TestScanCount(scan, expected);
	delete scan;
	return test;
}


//-------------------------------------------------------------------
// BTreeDriver::TestBalance
//
// Input   : btf,   The B-Tree to test. 
//           left,  The left page after the split. 
//           right, The right page after the split. 
// Output  : None
// Return  : True if the pages are balanced. 
// Purpose : Checks whether pages are balanced after a split. 
//-------------------------------------------------------------------
bool BTreeDriver::TestBalance(BTreeFile* btf, 
	                          ResizableRecordPage* left, 
 							  ResizableRecordPage* right) {
								  
	int leftSpace = left->AvailableSpace();
	int rightSpace = right->AvailableSpace();
	int slotSize = left->AvailableSpaceForAppend() - leftSpace;

	char* leftMax;
	char* rightMin;

	// This is a bit ugly, because we are trying to be general enough 
	// support both leaf pages and index pages. 
	RecordID rid;
	int len;

	rid.pageNo = left->PageNo();
	rid.slotNo = left->GetNumOfRecords() - 1;
	left->ReturnRecord(rid, leftMax, len);

	right->FirstRecord(rid);
	right->ReturnRecord(rid, rightMin, len);

	assert(strcmp(leftMax, rightMin) <= 0);

	// Can we move a record from left to right and increase the amount of 
	// available space without changing which one has more
	if(leftSpace < rightSpace) {
		int diff = rightSpace - leftSpace;
		
		// We can move a key/value pair without creating a new slot/key.
		if(strcmp(leftMax, rightMin) == 0) {
			if(diff > sizeof(RecordID)) {
				std::cerr << "Could have moved a record from left to right"
					      << "and reduced difference in freespace. " 
 						  << std::endl;
				return false;
			}
		}
		// We would have to create a new slot. 
		else {
			if(diff > (int)sizeof(RecordID) + (int)strlen(leftMax) + 1 + slotSize) {
				std::cerr << "Could have moved a record from left to right"
					      << "and reduced difference in freespace. " 
 						  << std::endl;
				return false;
			}

		}

	}
	else if(rightSpace < leftSpace)  {
		int diff = leftSpace - rightSpace;
		
		// We can move a key/value pair without creating a new slot/key.
		if(strcmp(leftMax, rightMin) == 0) {
			if(diff > sizeof(RecordID)) {
				std::cerr << "Could have moved a record from right to left"
					      << "and reduced difference in freespace. " 
 						  << std::endl;
				return false;
			}
		}
		// We would have to create a new slot. 
		else {
			if(diff > (int)sizeof(RecordID) + (int)strlen(leftMax) + 1 + slotSize) {
				std::cerr << "Could have moved a record from right to left"
					      << "and reduced difference in freespace. " 
 						  << std::endl;
				return false;
			}
		}
	}

	return true;
}





//-----------------------------------------------------------------------------
// Test Cases
//-----------------------------------------------------------------------------

bool BTreeDriver::TestSinglePage() {
	Status status;
	BTreeFile *btf;
	
	btf = new BTreeFile(status, "BTreeTest1");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	std::cout << "Starting Test 1..." << std::endl;
	std::cout << "BTreeIndex created successfully." << std::endl;

	bool res;

	std::cout << "Inserting 59 initial keys..."	<< std::endl;
	res = InsertRange(btf, 1, 59);
	std::cout << "Got Here" << std::endl;
	res = res && TestNumLeafPages(btf, 1);
	std::cout << "Got Here" << std::endl;
	res = res && TestNumEntries(btf, 59);


	std::cout << "Checking a few individual keys..." << std::endl;
	res = res && TestAbsent(btf, 0);
	res = res && TestPresent(btf, 2);
	res = res && TestPresent(btf, 59);
	res = res && TestAbsent(btf, 60);

	std::cout << "Checking scans..." << std::endl;

	char low[MAX_KEY_LENGTH];
	char high[MAX_KEY_LENGTH];
	toString(5, low);
	toString(15, high);
	BTreeFileScan* scan = btf->OpenScan(low, high);
	res = res && TestScanCount(scan, 11);
	delete scan;

	toString(58, low);
	toString(64, high);
	scan = btf->OpenScan(low, high);
	res = res && TestScanCount(scan, 2);
	delete scan;

	toString(0, low);
	toString(5, high);
	scan = btf->OpenScan(low, high);
	res = res && TestScanCount(scan, 5);
	delete scan;



	// We destroy and rebuild the file before to reduce 
	// dependence on working Delete function.
	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;

	btf = new BTreeFile(status, "BTreeTest2");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}

	std::cout << "Inserting 124 duplicates." << std::endl;
	res = res && InsertDuplicates(btf, 3, 124);
	res = res && TestNumLeafPages(btf, 1);
	res = res && TestNumEntries(btf, 124);

	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;
	return res;
}

bool BTreeDriver::TestInsertsWithLeafSplits() {
	Status status;
	BTreeFile *btf;
	bool res;
	LeafPage* leftPage;
	LeafPage* rightPage;
	btf = new BTreeFile(status, "BTreeTest2");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	std::cout << "Starting Test 2..." << std::endl;
	std::cout << "BTreeIndex created successfully." << std::endl;

	std::cout << "Inserting 30 large keys..."	<< std::endl;
	res = InsertRange(btf, 1, 30, 1, 20);
	res = res && TestNumLeafPages(btf, 1);
	res = res && TestNumEntries(btf, 30);


	//Should cause split. 
	std::cout << "Causing split with new key in right node..."	<< std::endl;
	res = res && InsertKey(btf, 31, 1, 20);
	res = res && TestNumLeafPages(btf, 2);
	res = res && TestNumEntries(btf, 31);
	

	PageID leftPid = btf->GetLeftLeaf();

	if(MINIBASE_BM->PinPage(leftPid, (Page*&) leftPage) == FAIL) {
		std::cerr << "Error pinning left leaf page." << std::endl;
		res = false;
	}
	PageID rightPid = leftPage->GetNextPage();

	if(MINIBASE_BM->PinPage(rightPid, (Page*&) rightPage) == FAIL) {
		std::cerr << "Error pinning right leaf page." << std::endl;
		res = false;
	}

	res = res && TestBalance(btf, leftPage, rightPage);

	if(MINIBASE_BM->UnpinPage(leftPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning left leaf page." << std::endl;
		res = false;
	}

	if(MINIBASE_BM->UnpinPage(rightPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning right leaf page." << std::endl;
		res = false;
	}




	// We destroy and rebuild the file before to reduce 
	// dependence on working Delete function.
	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;

	btf = new BTreeFile(status, "BTreeTest2");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}



	std::cout << "Inserting 30 large keys..."	<< std::endl;
	res = InsertRange(btf, 2, 31, 1, 20);



	res = res && TestNumLeafPages(btf, 1);
	res = res && TestNumEntries(btf, 30);

	//Should cause split. 
	std::cout << "Causing split with new key in left node..."	<< std::endl;
	res = res && InsertKey(btf, 1, 1, 20);



	res = res && TestNumLeafPages(btf, 2);
	res = res && TestNumEntries(btf, 31);



	leftPid = btf->GetLeftLeaf();
	if(MINIBASE_BM->PinPage(leftPid, (Page*&) leftPage) == FAIL) {
		std::cerr << "Error pinning left leaf page." << std::endl;
		res = false;
	}
	rightPid = leftPage->GetNextPage();
	if(MINIBASE_BM->PinPage(rightPid, (Page*&) rightPage) == FAIL) {
		std::cerr << "Error pinning right leaf page." << std::endl;
		res = false;
	}

	res = res && TestBalance(btf, leftPage, rightPage);

	if(MINIBASE_BM->UnpinPage(leftPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning left leaf page." << std::endl;
		res = false;
	}

	if(MINIBASE_BM->UnpinPage(rightPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning right leaf page." << std::endl;
		res = false;
	}


	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;

	btf = new BTreeFile(status, "BTreeTest2");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}

	std::cout << "Begining tests with duplicate keys..." << std::endl;

	res = res && InsertDuplicates(btf, 5, 121, 1, 30);
	res = res && TestNumLeafPages(btf, 1);
	res = res && TestNumEntries(btf, 121);

	std::cout << "Causing Split..." << std::endl;	
	res = res && InsertKey(btf, 5, 121, 30);

	res = res && TestNumLeafPages(btf, 2);
	res = res && TestNumEntries(btf, 122);


	leftPid = btf->GetLeftLeaf();
	if(MINIBASE_BM->PinPage(leftPid, (Page*&) leftPage) == FAIL) {
		std::cerr << "Error pinning left leaf page." << std::endl;
		res = false;
	}
	rightPid = leftPage->GetNextPage();
	if(MINIBASE_BM->PinPage(rightPid, (Page*&) rightPage) == FAIL) {
		std::cerr << "Error pinning right leaf page." << std::endl;
		res = false;
	}

	res = res && TestBalance(btf, leftPage, rightPage);

	if(MINIBASE_BM->UnpinPage(leftPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning left leaf page." << std::endl;
		res = false;
	}

	if(MINIBASE_BM->UnpinPage(rightPid, CLEAN) == FAIL) {
		std::cerr << "Error unpinning right leaf page." << std::endl;
		res = false;
	}


	char keyArr[MAX_KEY_LENGTH];
	toString(5, keyArr, 30);

	std::cout << "Testing search and scan..." << std::endl;
	BTreeFileScan* scan = btf->OpenScan(keyArr, keyArr);
	res = res && TestScanCount(scan, 122);
	delete scan;


	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;


	return res;
}

bool BTreeDriver::TestInsertsWithIndexSplits() {
	Status status;
	BTreeFile *btf;
	bool res;

	btf = new BTreeFile(status, "BTreeTest3");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	std::cout << "Starting Test 3..." << std::endl;
	std::cout << "BTreeIndex created successfully." << std::endl;

	std::cout << "Inserting 31 large keys..."	<< std::endl;
	res = InsertRange(btf, 1, 31, 1, 20);
	res = res && TestNumLeafPages(btf, 2);
	res = res && TestNumEntries(btf, 31);

	std::cout << "Inserting keys until root splits." << std::endl;

	PageID rootId = btf->header->GetRootPageID();
	PageID newRootId;
	int key = 32;
	int numInserted = 0;

	PageID leftPid, rightPid;
	IndexPage* leftPage;
	IndexPage* rightPage;

	while(true) {
		res = res && InsertKey(btf, key, 1, 20);
		newRootId = btf->header->GetRootPageID();

		// there was a split. Check balance. 
		if(newRootId != rootId) {
			IndexPage* ip;

			if(MINIBASE_BM->PinPage(newRootId, (Page*&) ip) == FAIL) {
				std::cerr << "Error pinning root page." << std::endl;
				res = false;
			}

			if(ip->GetNumOfRecords() != 1) {
				std::cerr << "Error: Expected 1 key in root, got " 
  					      << ip->GetNumOfRecords() << std::endl;
				res = false;
			}

			leftPid = ip->GetPrevPage();

			char* rightKey;
			ip->GetMinKeyValue(rightKey, rightPid);

			if(MINIBASE_BM->PinPage(leftPid, (Page*&) leftPage) == FAIL) {
				std::cerr << "Error pinning left leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->PinPage(rightPid, (Page*&) rightPage) == FAIL) {
				std::cerr << "Error pinning right leaf page." << std::endl;
				res = false;
			}

			res = res && TestBalance(btf, leftPage, rightPage);
			
			if(MINIBASE_BM->UnpinPage(leftPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning left leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->UnpinPage(rightPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning right leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->UnpinPage(newRootId, CLEAN) == FAIL) {
				std::cerr << "Error unpinning root page." << std::endl;
				res = false;
			}


			break;
		}
		key++;
		numInserted++;
	}

	res = res && TestNumEntries(btf, key);


	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete btf;

	btf = new BTreeFile(status, "BTreeTest2");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}

	std::cout << "Begining tests with duplicate keys..." << std::endl;


	res = res && InsertDuplicates(btf, 5, 122, 1, 30);
	res = res && TestNumLeafPages(btf, 2);
	res = res && TestNumEntries(btf, 122);


	std::cout << "Inserting duplicate keys until root splits." << std::endl;
	//Insert keys until the root splits.
	rootId = btf->header->GetRootPageID();
	int offset = 123;
	while(true) {
		res = res && InsertKey(btf, 5, offset, 30);
		newRootId = btf->header->GetRootPageID();

		// there was a split. Check balance. 
		if(newRootId != rootId) {
			IndexPage* ip;
			if(MINIBASE_BM->PinPage(newRootId, (Page*&) ip) == FAIL) {
				std::cerr << "Error pinning index page." << std::endl;
				res = false;
			}

			if(ip->GetNumOfRecords() != 1) {
				std::cerr << "Error: Expected 1 key in root, got " 
  					      << ip->GetNumOfRecords() << std::endl;
				res = false;
			}

			leftPid = ip->GetPrevPage();

			char* rightKey;
			PageID rightPid;
			ip->GetMinKeyValue(rightKey, rightPid);

			if(MINIBASE_BM->PinPage(leftPid, (Page*&) leftPage) == FAIL) {
				std::cerr << "Error pinning left leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->PinPage(rightPid, (Page*&) rightPage) == FAIL) {
				std::cerr << "Error pinning right leaf page." << std::endl;
				res = false;
			}

			res = res && TestBalance(btf, leftPage, rightPage);
			
			if(MINIBASE_BM->UnpinPage(leftPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning left leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->UnpinPage(rightPid, CLEAN) == FAIL) {
				std::cerr << "Error unpinning right leaf page." << std::endl;
				res = false;
			}

			if(MINIBASE_BM->UnpinPage(newRootId, CLEAN) == FAIL) {
				std::cerr << "Error unpinning right leaf page." << std::endl;
				res = false;
			}

			break;
		}
		offset++;
	}

	char keyArr[MAX_KEY_LENGTH];
	toString(5, keyArr, 30);

	res = res && TestNumEntries(btf, offset);
	BTreeFileScan* scan = btf->OpenScan(keyArr, NULL);
	res = res && TestScanCount(scan, offset);
	delete scan;

	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}
	delete btf;

	return res;
}

bool BTreeDriver::TestModifiedInserts() {
	Status status;
	BTreeFile *btf;
	bool res;

	btf = new BTreeFile(status, "BTreeTest5");
	if(status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	std::cout << "Starting Test 5..." << std::endl;
	std::cout << "BTreeIndex created successfully." << std::endl;


	res = InsertDuplicates(btf, 1, 60, 1, 30);


	res = res && InsertDuplicates(btf, 3, 55, 1, 30);
	res = res && InsertDuplicates(btf, 7, 65, 1, 30);

	res = res && TestNumLeafPages(btf, 3);
	res = res && TestNumEntries(btf, 180);

	res = res && InsertDuplicates(btf, 4, 75, 1, 30);	
	res = res && TestNumLeafPages(btf, 4);
	res = res && TestNumEntries(btf, 255);

	char searchKey[MAX_KEY_LENGTH];
	toString(4, searchKey, 30);
	

	BTreeFileScan* scan = btf->OpenScan(searchKey, searchKey);
	res = res && TestScanCount(scan, 75);
	
	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}

	delete scan;
	delete btf;
	return res;
}


bool BTreeDriver::TestLargeWorkload() {
	Status status;
	BTreeFile *btf;
	bool res;

	btf = new BTreeFile(status, "BTreeTest4");
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	std::cout << "Starting Test 4..." << std::endl;
	std::cout << "BTreeIndex created successfully." << std::endl;


	res = InsertRange(btf, 1, 1000, 1, 5, true);
	res = res && TestNumEntries(btf, 1000);

	res = InsertRange(btf, 1, 1000, 2, 5, false);
	res = res && TestNumEntries(btf, 2000);

	res = InsertRange(btf, 501, 1500, 3, 5, false);
	res = res && TestNumEntries(btf, 3000);

	res = InsertRange(btf, 2001, 4000, 1, 5, false);
	res = res && TestNumEntries(btf, 5000);

	char low[MAX_KEY_LENGTH];
	char high[MAX_KEY_LENGTH];

	toString(1000, low, 5);
	toString(1000, high, 5);
	BTreeFileScan* scan = btf->OpenScan(low, high);
	res = res && TestScanCount(scan, 3);
	delete scan;

	toString(3000, low, 5);
	toString(3000, high, 5);
	scan = btf->OpenScan(low, high);
	res = res && TestScanCount(scan, 1);
	delete scan;

	res = res && TestAbsent(btf, 1700, 1, 5);

	if(btf->DestroyFile() != OK) {
		std::cerr << "Error destroying BTreeFile" << std::endl;
		res = false;
	}
	delete btf;

	return res;

	
}