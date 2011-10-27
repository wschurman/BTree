/*
 * The DB class
 * $Id
 */

#ifndef _DB_H
#define _DB_H

#include <string.h>
#include <stdlib.h>

#include "page.h"

// Each database is basically a UNIX file and consists of several relations
// (viewed as heapfiles and their indexes) within it.

// Class definition for DB which manages a database.


  // This is the maximum length of the name of a "file" within a database.
const int MAX_NAME = 50;
  

enum dbErrCodes {
    DB_FULL,
    DUPLICATE_ENTRY,
    UNIX_ERROR,
    BAD_PAGE_NO,
    FILE_IO_ERROR,
    FILE_NOT_FOUND,
    FILE_NAME_TOO_LONG,
    NEG_RUN_SIZE,
};

// oooooooooooooooooooooooooooooooooooooo

class DB {

  public:
    // Constructors
    // Create a database with the specified number of pages where the page
    // size is the default page size.
    DB( const char* name, unsigned num_pages, Status& status );

    // Open the database with the given name.
    DB( const char* name, Status& status );

    // Destructor: closes the database
   ~DB();

    // Destroy the database, removing the file that stores it. 
    Status Destroy();

    // Read the contents of the specified page into the given memory area.
    Status ReadPage(PageID pageno, Page* pageptr);

    // Write the contents of the specified page.
    Status WritePage(PageID pageno, Page* pageptr);

    // Allocate a set of pages where the run size is taken to be 1 by default.
    // Gives back the page number of the first page of the allocated run.
    Status AllocatePage(PageID& start_page_num, int run_size = 1);

    // Deallocate a set of pages starting at the specified page number and
    // a run size can be specified.
    Status DeallocatePage(PageID start_page_num, int run_size = 1);


    // oooooooooooooooooooooooooooooooooooooo

    // Adds a file entry to the header page(s).
    Status AddFileEntry(const char* fname, PageID start_page_num);

    // Delete the entry corresponding to a file from the header page(s).
    Status DeleteFileEntry(const char* fname);

    // Get the entry corresponding to the given file.
    Status GetFileEntry(const char* name, PageID& start_pg);

    // Functions to return some characteristics of the database.
    const char* GetName() const;
    int GetNumOfPages() const;
    int GetPageSize() const;

    // Print out the space map of the database.
    // The space map is a bitmap showing which
    // pages of the db are currently allocated.
    Status dump_space_map();

  private:
    int fd;
    unsigned num_pages;
    char* name;

    struct file_entry {
        PageID pagenum;         // INVALID_PAGE if no entry.
        char   fname[MAX_NAME];
    };

    struct directory_page {
        PageID     next_page;
        unsigned   num_entries;
        file_entry *entries;  // Variable-sized struct
//      file_entry entries[0];  // Variable-sized struct
    };

      // A first_page structure appears on the first page of the database.
    struct first_page {
        unsigned int   num_db_pages; // How big the database is.
        directory_page dir;          // The first directory page.
    };               

      /* Internal structure of a Minibase DB:

         The DB keeps two basic kinds of global information.  The first is
         the space map, a map of which database pages have been allocated.
         The second is a directory of the "files" created within the database.

         Page 0 of the database is reserved for a special structure
         that holds global information about the database, like the
         number of pages in the database.  Following this information is
         the first "directory page".  A directory page is where the DB
         keeps track of the files created within the database.

         Page 1 of the database, and as many subsequent pages as needed,
         holds the "space map," which is a bitmap representing pages
         allocated in the database.
     */


      // Set runsize bits starting from start to value specified
    Status set_bits( PageID start, unsigned runsize, int bit );

      // Initializes the given directory page to contain no entries.
    void init_dir_page( directory_page* dp, unsigned used_bytes );
};

// oooooooooooooooooooooooooooooooooooooo

#endif    // _DB_H
