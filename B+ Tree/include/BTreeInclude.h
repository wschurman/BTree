#ifndef _B_TREE_INCLUDE_H_
#define _B_TREE_INCLUDE_H_

#include "SortedKVPage.h"

// Useful definitions. 
#define MAX_KEY_LENGTH 128
#define INDEX_PAGE 0
#define LEAF_PAGE 1

// This is likely unnecessary, but if you want a bound on path length, 
// this should suffice. 
#define MAX_TREE_DEPTH 4

// Define index and leaf page types 
typedef SortedKVPage<PageID> IndexPage;
typedef SortedKVPage<RecordID> LeafPage;


// Helper Macros. Feel free you use these if you want. 
#define PIN(a, b)   if (MINIBASE_BM->PinPage((a), (Page *&)(b)) != OK) {\
						std::cerr << "Unable to pin page " << a << std::endl; return FAIL;}
#define UNPIN(a, b) if (MINIBASE_BM->UnpinPage((a), (b)) != OK) {\
						std::cerr << "Unable to unpin page " << a << std::endl; return FAIL;}
#define FREEPAGE(a) if (MINIBASE_BM->FreePage((a)) != OK) {\
						std::cerr << "Unable to free page " << a << std::endl; return FAIL;}
#define NEWPAGE(a, b)  if (MINIBASE_BM->NewPage((a), (Page *&)(b)) != OK) {\
						std::cerr << "Unable to allocate new page " << a << std::endl; return FAIL;}

#define DIRTY TRUE
#define CLEAN FALSE



#endif