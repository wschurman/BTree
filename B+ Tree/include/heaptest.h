#ifndef _HEAP_DRIVER_H_
#define _HEAP_DRIVER_H_

#include "test.h"

class HeapDriver : public TestDriver
{
public:

    HeapDriver();
   ~HeapDriver();

    Status RunTests();

private:
    int choice;

    int Test1();
    int Test2();
    int Test3();
    int Test4();
    int Test5();
    int Test6();

    Status RunAllTests();
    const char* TestName();
};

#endif
