#include "ResizableRecordPage.h"

//-------------------------------------------------------------------
// SortedKVPage::AppendToRecord
//
// Input   : newData,    A pointer to the data to append. 
//           dataLength, The length of the data to append.
//           rid,        The record id of the record to append to.
// Output  : None.
// Return  : OK    if successful.
//           FAIL  if rid is invalid or there isn't space on this page.
// Purpose : Appends data to an existing record, shifting other 
//           records as appropriate.
//-------------------------------------------------------------------
Status ResizableRecordPage::AppendToRecord(const char* newData, int dataLength, const RecordID& rid) {
	// Invalid record id.
	if(rid.pageNo != pid || (rid.slotNo < 0 || rid.slotNo > numOfSlots)) {
		return FAIL;
	}
	// Not enough space to append new data. 
	if(AvailableSpaceForAppend() < dataLength) {
		return FAIL;
	}

	Slot* slot = (Slot*)GetFirstSlotPointer() - rid.slotNo;


	// Move records following the target location to make space.  
	memmove(data + slot->offset + slot->length + dataLength,
		    data + slot->offset + slot->length, 
			freePtr - (slot->offset + slot->length));

	// Copy data to append to appropriate spot on page. 
	memcpy(data + slot->offset + slot->length, newData, dataLength);

	slot->length += dataLength;

	//	Change the offset of all slots pointing to pages after the one we modified. 
	for (int i = 0; i < numOfSlots; i++) {
		assert(!SlotIsEmpty(GetFirstSlotPointer() - i));
		Slot *curSlot = GetFirstSlotPointer() - i;

		if (curSlot->offset > slot->offset) {
			curSlot->offset += dataLength;
		}
	}		

	// Update freePtr and freeSpace appropriately.
	freePtr += dataLength;
	freeSpace -= dataLength;

	return OK;
}



//-------------------------------------------------------------------
// SortedKVPage::CutFromRecord
//
// Input   : relativeStartOffset, The offset relative to the start of the 
//                                record at which to start cutting. 
//           length,              The length of data to cut. 
//           rid,                 The record id of the record in which to cut. 
// Output  : None.
// Return  : OK    if successful.
//           FAIL  if rid is invalid or tries 
//                 to cut after the end of the record.
// Purpose : Cuts data from an existing record and compacts appropriately.
//-------------------------------------------------------------------
Status ResizableRecordPage::CutFromRecord(int relativeStartOffset,
	                                      int length,
      									  const RecordID& rid) {

    //std::cout << "In CutFromRecord, relativeStartOffset=" << relativeStartOffset << ", length=" << length << ", rid=" << rid.pageNo << ", " << rid.slotNo << std::endl;

	if(rid.pageNo != pid || rid.slotNo < 0 || rid.slotNo >= numOfSlots) {
		return FAIL;
	}

	Slot* slot = GetFirstSlotPointer() - rid.slotNo;


	// Use the DeleteRecord method to cut the entire record. 
	if((relativeStartOffset == 0) && (length == slot->length)) {

		std::cout << "Returning HeapPage::DeleteRecord " << rid.pageNo << ", "  << rid.slotNo << std::endl;
		return HeapPage::DeleteRecord(rid);
	}

	if(relativeStartOffset + length > slot->length) {
		std::cerr << "Error: Trying to cut too much from record."
			      << "Record length=" << slot->length << std::endl;
		return FAIL;
	}

	// Move data up to fill in cut region. 
	int mvLength = freePtr - (slot->offset + relativeStartOffset + length);
	char* src = data + slot->offset + relativeStartOffset + length;
	char* dest = data + slot->offset + relativeStartOffset;
	memmove(dest, src, mvLength);

	// Update free space appropriately.  
	freePtr -= length;
	freeSpace += length;

	slot->length -= length;

	// Update offsets for slots following deleted region.
	for (int k = 0; k < numOfSlots; k++) {
		assert(!SlotIsEmpty(GetFirstSlotPointer() - k));
		Slot *curSlot = GetFirstSlotPointer() - k;
		if (curSlot->offset > slot->offset)
			curSlot->offset -= length;
	}

	return OK;
}