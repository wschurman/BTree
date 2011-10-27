
#ifndef _BUF_H
#define _BUF_H

#include "db.h"
#include "page.h"
#include "frame.h"
#include "replacer.h"
#include "hash.h"

class BufMgr 
{
	private:

		HashTable *hashTable;
		ClockFrame **frames;
		Replacer *replacer;
		int   numOfBuf;

		int FindFrame( PageID pid );
		long totalCall;
		long totalHit;

	public:

		BufMgr( int bufsize );
		~BufMgr();      
		Status PinPage( PageID pid, Page*& page, bool emptyPage=false );
		Status UnpinPage( PageID pid, bool dirty=false );
		Status NewPage( PageID& pid, Page*& firstpage,int howmany=1 ); 
		Status FreePage( PageID pid ); 
		Status FlushPage( PageID pid );
		Status FlushAllPages();
		Status  GetStat(long& pinNo, long& missNo) { pinNo = totalCall; missNo = totalCall-totalHit; return OK;}
		void   ResetStat() { totalHit = 0; totalCall = 0; }

		unsigned int GetNumOfBuffers();
		unsigned int GetNumOfUnpinnedBuffers();
};


#endif // _BUF_H
