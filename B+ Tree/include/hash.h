#ifndef _HASH_H
#define _HASH_H

#include "minirel.h"
#include "frame.h"

#define NUM_OF_BUCKETS 21
#define HASH(k)  k % NUM_OF_BUCKETS

	
class Map
{
	friend class MapIterator;

private :

	int pid;
	int frameNo;
	class Map *next;
	class Map *prev;

public :
	
	Map(PageID p, int f);
	~Map();
	void AddBehind(Map *m);
	void DeleteMe();
	Bool HasPageID(PageID p);
	int FrameNo();

};


class MapIterator
{
	Map *head;
	Map *current;

public :

	MapIterator(Map *maps);
	Map* operator() ();

};


class Bucket
{
private:

	Map *maps;

public :

	Bucket();
	~Bucket();
	void Insert(PageID pid, int frameNo);
	Status Delete(PageID pid);
	int Find(PageID pid);
	void EmptyIt();
};


class HashTable
{
private:

	Bucket buckets[NUM_OF_BUCKETS];

public :

	void Insert(PageID pid, int frameNo);
	Status Delete(PageID pid);
	int LookUp(PageID pid);
	void EmptyIt();
};


#endif