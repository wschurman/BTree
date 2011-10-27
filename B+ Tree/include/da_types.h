//
// da_types.h
//

#ifndef da_types_h
#define da_types_h

#ifndef False
#define False 0
#endif

#ifndef True
#define True !False
#endif

#ifndef TRUE
#define TRUE True
#endif

#ifndef FALSE
#define FALSE False
#endif

#ifndef NULL
// #include <_null.h>  // removed by sch
#define NULL 0
#endif

typedef unsigned long ulong;
typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned char uchar;

enum ErrorCode
{
        ErrRANGE, ErrMEM, ErrNULLPTR, ErrSIZE, ErrCOPY
};

void FatalError(ErrorCode ec);
#endif
