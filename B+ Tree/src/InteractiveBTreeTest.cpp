#include <cmath> 
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cassert>
using namespace std;

#include "bufmgr.h"
#include "db.h"

#include "InteractiveBTreeTest.h"

#define MAX_COMMAND_SIZE 1000

#define MAX_INT_LENGTH 5



Status InteractiveBTreeTest::RunTests(istream &in) {

	char *dbname="btdb";
	char *logname="btlog";
	char *btfname="BTreeIndex";

	remove(dbname);
	remove(logname);

	Status status;
	minibase_globals = new SystemDefs(status, dbname, logname, 1000,500,200);
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}

	BTreeFile* btf=createIndex(btfname);
	if (btf==NULL) {
		cout << "Error: Unable to create index file"<< endl;
	}

	char command[MAX_COMMAND_SIZE];
	in >> command;
	while(in) {
		if(!strcmp(command, "insert")) {
			int high, low;
			in >> low >> high ;
			insertHighLow(btf,low,high);
		}
		else if(!strcmp(command, "insertrev")) {
			int low, high;
			in >> low >> high;
			insertHighLowReverse(btf,low,high);
		}
		else if(!strcmp(command, "insertdup")) {
			int val, num;
			in >> val >> num;
			insertDups(btf,val,num);
		}
		else if(!strcmp(command, "scan")) {
			int high, low;
			in >> low >> high;
			scanHighLow(btf,low,high);
		}
		else if(!strcmp(command, "print")) {
			btf->PrintWhole(true);
		}
		else if(!strcmp(command, "test")) {
			int testNum; 
			in >> testNum;

			switch(testNum) {
			case 1:
				if(!BTreeDriver::TestSinglePage()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			case 2:
				if(!BTreeDriver::TestInsertsWithLeafSplits()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			case 3:
				if(!BTreeDriver::TestInsertsWithIndexSplits()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			case 4:
				if(!BTreeDriver::TestLargeWorkload()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			case 5:
				if(!BTreeDriver::TestModifiedInserts()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			case 6:
				if(!BTreeDriver::TestPerformance()) {
					std::cerr << "FAILED Test " << testNum << std::endl;
				}
				else {
					std::cerr << "PASSED Test " << testNum << std::endl;
				}
				break;
			}

		}

		else if(!strcmp(command, "quit")) {
			break;
		}
		else {
			cout << "Error: Unrecognized command: "<<command<<endl;
		}
		in >> command;
	}

	destroyIndex(btf, btfname);

	delete minibase_globals;
	remove(dbname);
	remove(logname);

	return OK;
}


BTreeFile *InteractiveBTreeTest::createIndex(char *name) {
    cout << "Create B+tree." << endl;
    cout << "  Page size="<<MINIBASE_PAGESIZE<< " Max space="<<MAX_SPACE<<endl;

    Status status;
    BTreeFile *btf = new BTreeFile(status, name);
    if (status != OK) {
        minibase_errors.show_errors();
        cout << "  Error: can not open index file."<<endl;
		if (btf!=NULL)
			delete btf;
		return NULL;
    }
    cout<<"  Success."<<endl;
	return btf;
}


void InteractiveBTreeTest::destroyIndex(BTreeFile *btf, char *name) {
	cout << "Destroy B+tree."<<endl;
	Status status = btf->DestroyFile();
	if (status != OK)
		minibase_errors.show_errors();
    delete btf;
}


void InteractiveBTreeTest::insertHighLow(BTreeFile *btf, int low, int high) {

	int numkey=high-low+1;
	cout << "Inserting: ("<<low<<" to "<<high<<")"<<endl;

	bool res = BTreeDriver::InsertRange(btf, low, high, lastOffset++);

	if(res) {
		cout << "  Success."<<endl;
	}		
	else {
		cout << "Insert failed." << endl;
	}
}

void InteractiveBTreeTest::insertHighLowReverse(BTreeFile *btf, int low, int high) {

	int numkey=high-low+1;
	cout << "Inserting: ("<<low<<" to "<<high<<") in reverse"<<endl;

	bool res = BTreeDriver::InsertRange(btf, low, high, lastOffset++, 4, true);

	if(res) {
		cout << "  Success."<<endl;
	}		
	else {
		cout << "Insert failed." << endl;
	}
}

void InteractiveBTreeTest::insertDups(BTreeFile *btf, int key, int num) {

	cout << "Inserting " << num << " Duplicate: "<<key<<endl;

	bool res = BTreeDriver::InsertDuplicates(btf, key, num,
	                               1, 4);

	if(res) {
		cout << "  Success."<<endl;
	}		
	else {
		cout << "Insert failed." << endl;
	}

}

void InteractiveBTreeTest::scanHighLow(BTreeFile *btf, int low, int high) {

	cout << "Scanning ("<<low<<" to "<<high<<"):"<<endl;

	//int *plow=&low, *phigh=&high;
	//if(low==-1) plow=NULL;
	//if(high==-1) phigh=NULL;
	//IndexFileScan *scan = btf->OpenScan(plow, phigh);

	char strLow[MAX_INT_LENGTH], strHigh[MAX_INT_LENGTH];
	BTreeDriver::toString(low, strLow);
	BTreeDriver::toString(high, strHigh);

	char* lowPtr = (low == -1) ? NULL : strLow;
	char* highPtr = (high == -1) ? NULL : strHigh;


	BTreeFileScan *scan = btf->OpenScan(lowPtr, highPtr);


	if(scan == NULL) {
		cout << "  Error: cannot open a scan." << endl;
		minibase_errors.show_errors();
		return;
	}

	RecordID rid;
	int count=0;
	//char skey[MAX_INT_LENGTH];
	//char skey[50];
	char* skey;

	Status status = scan->GetNext(rid, skey);
	while (status == OK) {
		count++;
		cout<<"  Scanned @[pg,slot]=["<<rid.pageNo<<","<<rid.slotNo<<"]";
		cout<<" key="<<skey<<endl;
		status = scan->GetNext(rid, skey);
	}
	delete scan;
	cout << "  "<< count << " records found."<<endl;

	if (status!=DONE) {
		minibase_errors.show_errors();
		return;
	}
	cout << "  Success."<<endl;
}
