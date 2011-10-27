#ifndef _DIRPAGE_H
#define _DIRPAGE_H

#include "heappage.h"

struct PageInfo 
{
	PageID pid;
	short  spaceAvailable;
	short  numOfRecords;
};


class DirPage 
{
	friend class DirPageIterator;
	friend class PageInfoIterator;

private : 
	int numOfEntry;
	int spaceAvailable;
	PageID curr;
	PageID next;
	PageID prev;

	#define DIR_PAGE_SIZE (MAX_SPACE - 2*sizeof(int) - 3*sizeof(PageID))

	char data[DIR_PAGE_SIZE];

public :
	Status Init (PageID pid);
	PageInfo *FindPageInfo (PageID pid);
	int FindPageInfoEntry (PageID pid);
	PageInfo *GetPageInfo (int entry);
	Status InsertPage (PageID pid, HeapPage *page);
	Status DeletePage (PageID pid);
	Status InsertRecordIntoPage (PageID pid, HeapPage *page);
	Status DeleteRecordFromPage (PageID pid, HeapPage *page);
	void   SetNextPage (PageID pid) { next = pid; }
	void   SetPrevPage (PageID pid) { prev = pid; }
	PageID GetNextPage();
	PageInfo *GetEntry(int entry);
	Bool HasFreeSpace();
	Bool IsEmpty()   { return (numOfEntry == 0); }
	Bool Deletable() { return (prev != INVALID_PAGE) || (next != INVALID_PAGE); }
	Bool IsHead()    { return (prev == INVALID_PAGE); }
	Status DeleteItSelf();
};


class PageInfoIterator 
{

private :

	int currEntry;
	DirPage *page;

public :

	PageInfoIterator(DirPage *page);
	~PageInfoIterator();
	PageInfo *operator() ();
};


class DirPageIterator 
{

private :

	PageID curr;

public :

	DirPageIterator(PageID pid);
	~DirPageIterator();
	PageID operator() ();
};

#endif