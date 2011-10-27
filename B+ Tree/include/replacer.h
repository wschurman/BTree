#include "clockframe.h"
#include "hash.h"

class Replacer 
{
	public :

		Replacer();
		~Replacer();

		virtual int PickVictim() = 0;
};

class Clock : public Replacer
{
	private :
		
		int current;
		int numOfBuf;
		ClockFrame **frames;
		HashTable *hashTable;

	public :
		
		Clock( int bufSize, ClockFrame **frames, HashTable *hashTable );
		~Clock();
		int PickVictim();
};