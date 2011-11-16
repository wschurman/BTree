#include "BTreeFile.h"
#include "BTreeTest.h"
//#include "index.h"
#include <set>
using namespace std;

class InteractiveBTreeTest {
	int nEntered; 
	int lastOffset;
public:

	InteractiveBTreeTest() : nEntered(0) { 
		lastOffset = 1;
	}
	Status RunTests(istream &in);
	BTreeFile *createIndex(char *name);
	void destroyIndex(BTreeFile *btf, char *name);

	void insertHighLow(BTreeFile *btf, int low, int high);
	void insertHighLowReverse(BTreeFile *btf, int low, int high);
	void insertDups(BTreeFile *btf, int key, int num);
	void scanHighLow(BTreeFile *btf, int low, int high);
};







