#include <iostream>
#include <ctime>

#include "SortedKVPage.h"
#include "InteractiveBTreeTest.h"

int MINIBASE_RESTART_FLAG = 0;

void printHelp() {
	cout << "Commands should be of the form:"<<endl;
	cout << "insert <low> <high>"<<endl;
	cout << "scan <low> <high>"<<endl;
	cout << "test <testnum>"<<endl;
	cout << "\tTest 1: Test a tree with single leaf." << endl;
	cout << "\tTest 2: Test inserts with leaf splits." << endl;
	cout << "\tTest 3: Test inserts with index splits." << endl;
	cout << "\tTest 4: Test a large workload." << endl;
	cout << "\tTest 5: Test modified inserts." << endl;
	cout << "\tTest 6: Added performance test." << endl;
	cout << "print"<<endl;
	cout << "quit (not required)"<<endl;
	cout << "Note that (<low>==-1)=>min and (<high>==-1)=>max"<<endl;
}

int btreeTestManual() {
	cout << "btree tester with manual input" <<endl;
	printHelp();

	InteractiveBTreeTest btt;
	Status dbstatus = btt.RunTests(cin);

	if (dbstatus != OK) {       
		cout << "Error encountered during btree tests: " << endl;
		minibase_errors.show_errors();      
		return 1;
	}
	return 0;
}


int main(int argc, char* argv[])
{

	int ret = btreeTestManual();

	std::cin.get();
	return 0;
}

