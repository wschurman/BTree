#ifndef _B_TREE_DRIVER_H_
#define _B_TREE_DRIVER_H_

#include "BTreeFile.h"

class BTreeDriver {
public:

	
	static void toString(const int n, char* str, int pad = 4);




	static bool InsertKey(BTreeFile* btf, int key, int ridOffset = 1, int pad = 4);

	static bool InsertRange(BTreeFile* btf, int low, int high, 
		                    int ridOffset = 1, int pad = 4, bool reverse = false);

	static bool InsertDuplicates(BTreeFile* btf, int key, int numDups,
		                         int startOffset = 1, int pad = 4);


	static bool DeleteStride(BTreeFile* btf, int low, int high, 
		                     int stride, int pad = 4);

	static bool TestPresent(BTreeFile* btf, int key, 
 		                    int ridOffset = 1, int pad = 4);

	static bool TestAbsent(BTreeFile* btf, int key, 
	                       int ridOffset = 1, int pad = 4);

	static bool TestNumLeafPages(BTreeFile* btf, int expected);
	static bool TestScanCount(BTreeFileScan* scan, int expected);
	static bool TestNumEntries(BTreeFile* btf, int expected);
	
	static bool TestBalance(BTreeFile* btf,
                            ResizableRecordPage* left,
   					        ResizableRecordPage* right);



	// Larger test cases. 


	static bool TestSinglePage();
	static bool TestInsertsWithLeafSplits();
	static bool TestInsertsWithIndexSplits();
	static bool TestModifiedInserts();
	static bool TestLargeWorkload();

};

#endif
