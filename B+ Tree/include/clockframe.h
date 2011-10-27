#ifndef _CLOCKFRAME_H
#define _CLOCKFRAME_H

#include "frame.h"

class ClockFrame : public Frame 
{
	private :
		
		Bool referenced;

 	public :

		ClockFrame();
		~ClockFrame();
	
		void Unpin();
		Status Free();
		void UnsetReferenced();
		Bool IsReferenced();
		Bool IsVictim();
};

#endif