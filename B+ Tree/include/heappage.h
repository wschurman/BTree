#ifndef HFPAGE_H
#define HFPAGE_H

#include "minirel.h"
#include "page.h"

const int INVALID_SLOT =  -1;

//	Size of the data array in the class HeapPage
const int HEAPPAGE_DATA_SIZE = MAX_SPACE - 3 * sizeof(PageID) - 4 * sizeof(short);

class HeapPage {

protected :
	struct Slot 
	{
		short offset;	// Offset of record from the start of data area.
		short length;	// Length of the record.
	};

	short   numOfSlots;	// Number of slots available (maybe filled or empty).
	short   freePtr;	// Offset from start of data area, where begins the free space for adding new records.
	short   freeSpace;	// Amount of free space in bytes in this page.
	short   type;		// Not used for HeapFile assignment, but will be used in B+-tree assignment.

	PageID  pid;		// Page ID of this page  
	PageID  nextPage;	// Page ID of the next page.
	PageID  prevPage;	// Page ID of the prev page.

	char data[HEAPPAGE_DATA_SIZE];	// Data area for this page.  Actual records
									// grow from start towards the end of a page. 

	//	Check if the slot is empty.
	bool SlotIsEmpty(Slot *slot) {
		return slot->length == INVALID_SLOT;
	}

	//	Fill the slot with the given <offset, length> pair.
	void FillSlot(Slot *slot, int offset, int length) {
		slot->offset = offset;
		slot->length = length;
	}
	
	//	Set the slot to be empty.
	void SetSlotEmpty(Slot *slot) {
		slot->length = INVALID_SLOT;
	}
	
	//	Get the first slot pointer, which is lcoated in the end of data area.
	Slot* GetFirstSlotPointer() {
		return (Slot*)(data + HEAPPAGE_DATA_SIZE - sizeof(Slot));
	}




public:
	//	Inialize the page with given PageID.
	void Init(PageID pageNo);
	
	//	Insert a record into the page.
	Status InsertRecord(const char* recPtr, int recLen, RecordID& rid);
	
	//	Delete a record from the page.
	Status DeleteRecord(RecordID rid);
	
	//	To find the first record on a page.
	Status FirstRecord(RecordID& firstRid);
	
	//	To find the next record on a page.
	Status NextRecord (RecordID curRid, RecordID& nextRid);
	
	//	To retrieve a COPY of a record with ID rid from a page.
	Status GetRecord(RecordID rid, char* recPtr, int& len);
	
	//	To retrieve a POINTER to the record.
	Status ReturnRecord(RecordID rid, char*& recPtr, int& len);
	
	//	To return the amount of available space.
	int    AvailableSpace();
	
	//	Check if there is any record in the page.
	bool   IsEmpty();
	
	//	Counts the number of records in the page.
	int    GetNumOfRecords();

	//	Get the PageID of next page.
	PageID GetNextPage();

	//	Get the PageID of previous page.
	PageID GetPrevPage();

	//	Set the PageID for next page.
	void   SetNextPage(PageID pageNo);

	//	Set the PageID for previous page.
	void   SetPrevPage(PageID pageNo);

	//	Get the PageID of this page.
	PageID PageNo();  

	void PrintSlotArray();
};

//#define SLOT_IS_EMPTY(s)  ((s).length == INVALID_SLOT)
//#define SLOT_FILL(s, o, l) do { (s).offset = (o); (s).length = (l);} while (0)
//#define SLOT_SET_EMPTY(s)  (s).length = INVALID_SLOT





#endif
