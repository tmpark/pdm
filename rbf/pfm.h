#ifndef _pfm_h_
#define _pfm_h_

typedef signed int PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>


using namespace std;


//custom type
struct FreeSpace{
	unsigned pageNum;
	unsigned freeSpace;
};

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     			// Access to the _pf_manager instance

    RC createFile    (const string &fileName);                         	// Create a new file
    RC destroyFile   (const string &fileName);                         	// Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle); 	// Open a file
    RC closeFile     (FileHandle &fileHandle);                         	// Close a file

protected:
    PagedFileManager();                                   				// Constructor
    ~PagedFileManager();                                  				// Destructor

private:
    static PagedFileManager *_pf_manager;
};


class FileHandle
{

public:
    // variables to keep the counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	
    FileHandle();                                                    	// Default constructor
    ~FileHandle();                                                   	// Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    void setNumberOfPages(unsigned numberToSet);
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    void setFileStream(void *file_stream_arg);
    fstream* getFileStream();

private:
    fstream *file_stream;
    unsigned numberOfPages;
}; 

#endif
