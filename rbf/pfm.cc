#include "pfm.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    const char* fileName_char = fileName.c_str();
	fstream file_to_create;
	file_to_create.open(fileName_char, fstream::in);
	if(file_to_create.is_open())
	{
		file_to_create.close();
	    return -1; //A file already exists
	}

	file_to_create.open(fileName_char, fstream::out | fstream:: binary); //Create a file (do not use in when creating a file)
	if(file_to_create.is_open())
	{
		file_to_create.close();
		return 0;
	}

    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    const char* fileName_char = fileName.c_str();
    RC success = remove(fileName_char); //delete file
	if(success == 0)
	{
	    return 0; //successful
	}

    return -1; //A file still exists
}





RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	fstream *file_to_open = fileHandle.getFileStream();
	if(file_to_open != NULL) //already opened
		return -1;

    const char* fileName_char = fileName.c_str();
	file_to_open = new fstream;

	file_to_open->open(fileName_char, fstream::in | fstream:: out |fstream::binary); //Using both in | out do not allow creation.
	if(!file_to_open->is_open())
	{

	    return -1; //A file opened
	}


	unsigned current_position = file_to_open->tellg();
	file_to_open->seekg(0,file_to_open->end); //move to the end of the file
	unsigned length = file_to_open->tellg(); //position at the end of the file
	file_to_open->seekg(current_position); //return back to position


	fileHandle.setFileStream(file_to_open);
	fileHandle.setNumberOfPages(ceil(length / PAGE_SIZE)); //initial number of pages

    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    fstream *file_to_close = fileHandle.getFileStream();
    if(file_to_close->is_open())
    {
    	file_to_close->close();
    	return 0;
    }

    return -1;
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	file_stream = NULL;
	numberOfPages = 0;

}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(!file_stream->is_open())
		return -1; //File not opened

	file_stream->seekg(pageNum*PAGE_SIZE, file_stream->beg); //move to the page read

	file_stream->read((char*)data,PAGE_SIZE);
    readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(!file_stream->is_open())
		return -1; //File not opened

	file_stream->seekp(pageNum*PAGE_SIZE, file_stream->beg); //move to the page written

    file_stream->write((const char*)data,PAGE_SIZE);

    writePageCounter++;

    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if(!file_stream->is_open())
		return -1; //File not opened

    file_stream->seekp(0,file_stream->end);

    file_stream->write((const char*)data,PAGE_SIZE);

    unsigned numberOfPages = getNumberOfPages();
    numberOfPages++;
    setNumberOfPages(numberOfPages);

    appendPageCounter++;

    return 0;
}


void FileHandle::setNumberOfPages(unsigned numberToSet)
{
	numberOfPages = numberToSet;
}

unsigned FileHandle::getNumberOfPages()
{

	return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}


void FileHandle::setFileStream(void *file_stream_arg)
{
	file_stream = (fstream*)file_stream_arg;
}

fstream* FileHandle::getFileStream()
{
	return file_stream;
}
