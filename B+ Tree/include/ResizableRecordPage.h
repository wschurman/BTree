#ifndef _RESIZABLE_RECORD_PAGE_H_
#define _RESIZABLE_RECORD_PAGE_H_

#include "heappage.h"

class ResizableRecordPage : public HeapPage {
public:
	// Appends data to an existing record, 
	// shifting other records as appropriate.
	Status AppendToRecord(const char* newData, 
		                  int dataLength, 
						  const RecordID& rid);


	// Cuts data from an existing record and compacts appropriately.
	Status CutFromRecord(int relativeStartOffset, 
		                 int length, 
						 const RecordID& rid);

	// Returns the mount of space available to append. 
	int AvailableSpaceForAppend() { return freeSpace; }	


	// Accessor methods.
	void  SetType(short t)  { type = t; }
	short GetType()         { return type; }
	int   GetNumOfRecords() { return numOfSlots; }


};


#endif