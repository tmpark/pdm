
#include "ix.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library

IndexManager* IndexManager::_index_manager = 0;

SlotOffset IndexManager::getFreeSpaceOffset(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((SlotOffset*)containerPtr);
}

RC IndexManager::setFreeSpaceOffset(void* pageToProcess, SlotOffset freeSpaceOffset)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((SlotOffset*)containerPtr) = freeSpaceOffset; //new free space
	return 0;
}

NumOfEnt IndexManager::getNumOfEnt(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((NumOfEnt*)containerPtr);
}

RC IndexManager::setNumOfEnt(void* pageToProcess, NumOfEnt numOfEnt)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((NumOfEnt*)containerPtr) = numOfEnt;
	return 0;
}

PageNum IndexManager::getTombstone(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((PageNum*)containerPtr);
}

RC IndexManager::setTombstone(void* pageToProcess, PageNum tombstone)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((PageNum*)containerPtr) = tombstone;
	return 0;
}

NodeType IndexManager::getNodeType(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((NodeType*)containerPtr);
}

RC IndexManager::setNodeType(void* pageToProcess, NodeType nodeType)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((NodeType*)containerPtr) = nodeType;
	return 0;
}

PageNum IndexManager::getParentPageNum(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((PageNum*)containerPtr);
}

RC IndexManager::setParentPageNum(void* pageToProcess, PageNum parentPageNum)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((PageNum*)containerPtr) = parentPageNum;
	return 0;
}

PageNum IndexManager::getLeftSiblingPageNum(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((PageNum*)containerPtr);
}

RC IndexManager::setLeftSiblingPageNum(void* pageToProcess, PageNum leftSiblingPageNum)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum)- sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((PageNum*)containerPtr) = leftSiblingPageNum;
	return 0;
}

PageNum IndexManager::getRightSiblingPageNum(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((PageNum*)containerPtr);
}

RC IndexManager::setRightSiblingPageNum(void* pageToProcess, PageNum rightSiblingPageNum)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum)- sizeof(PageNum)- sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((PageNum*)containerPtr) = rightSiblingPageNum;
	return 0;
}

PageNum IndexManager::getLeftMostChildPageNum(const void* pageToProcess)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((PageNum*)containerPtr);
}

RC IndexManager::setLeftMostChildPageNum(void* pageToProcess, PageNum leftChildPageNum)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum)- sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((PageNum*)containerPtr) = leftChildPageNum;
	return 0;
}


SlotOffset IndexManager::getEntryOffset(const void* pageToProcess,unsigned int entryNum)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum) - sizeof(SlotOffset)*(1+entryNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	return *((SlotOffset*)containerPtr);
}

RC IndexManager::setEntryOffset(void* pageToProcess, unsigned int entryNum, SlotOffset entryOffset)
{
	SlotOffset containerOffset = PAGE_SIZE - sizeof(SlotOffset) - sizeof(NumOfEnt) - sizeof(PageNum) - sizeof(NodeType) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum) - sizeof(PageNum)- sizeof(SlotOffset)*(1+entryNum);
	char *containerPtr = (char*)pageToProcess + containerOffset;
	*((SlotOffset*)containerPtr) = entryOffset; //new free space
	return 0;
}

template <typename T>
T IndexManager::getKeyOfEntry(const void* entryToProcess, AttrType type)
{
	if (type == TypeInt)
	{
		return *((int*)entryToProcess);
	}
	else if (type == TypeReal)
	{
		return *((float*)entryToProcess);
	}
	else if (type == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        char *varChar = (char*)entryToProcess + sizeof(int);
        return string(varChar,sizeOfVarChar);
	}

}

template <typename T>
RC IndexManager::setKeyOfIntermediateEntry(void* entryToProcess, AttrType type, T value)
{
	if (type == TypeInt)
	{
		*((int*)entryToProcess) = value;
	}
	else if (type == TypeReal)
	{
		*((float*)entryToProcess) = value;
	}
	else if (type == TypeVarChar)
	{
		int sizeOfVarChar = value.size();
		*((int*)entryToProcess) = sizeOfVarChar;
		memset((char*)entryToProcess + sizeof(int), value.c_str(), sizeOfVarChar);
	}
	return 0;
}

template <typename T>
PageNum IndexManager::getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType)
{
	char *childPtr = NULL;
	if (keyType == TypeInt)
	{
		childPtr = entryToProcess + sizeof(int);
	}
	else if (keyType == TypeReal)
	{
		childPtr = entryToProcess + sizeof(float);
	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        childPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar;
	}
	return *((PageNum*)childPtr);
}

template <typename T>
RC IndexManager::setChildOfIntermediateEntry(void* entryToProcess, AttrType keyType, PageNum childPageNum)
{
	char *childPtr = NULL;
	if (keyType == TypeInt)
	{
		childPtr = entryToProcess + sizeof(int);
	}
	else if (keyType == TypeReal)
	{
		childPtr = entryToProcess + sizeof(float);
	}
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)entryToProcess);
		childPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar;

	}
	*((PageNum*)childPtr) = childPageNum;
	return 0;
}

NumOfEnt IndexManager::getNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType)
{
	char *numOfRidsPtr = NULL;
	if (keyType == TypeInt)
	{
		numOfRidsPtr = (char*)entryToProcess + sizeof(int);

	}
	else if (keyType == TypeReal)
	{
		numOfRidsPtr = (char*)entryToProcess + sizeof(float);
	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        numOfRidsPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar;
	}
	return *((NumOfEnt*)numOfRidsPtr);
}

RC IndexManager::setNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType, NumOfEnt numOfRids)
{
	char *numOfRidsPtr = NULL;
	if (keyType == TypeInt)
	{
		numOfRidsPtr = (char*)entryToProcess + sizeof(int);
		*((NumOfEnt*)numOfRidsPtr) = numOfRids;
	}
	else if (keyType == TypeReal)
	{
	    numOfRidsPtr = (char*)entryToProcess + sizeof(float);
		*((NumOfEnt*)numOfRidsPtr) = numOfRids;
	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        numOfRidsPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar;

	}
	*((NumOfEnt*)numOfRidsPtr) = numOfRids;
	return 0;
}


RC IndexManager::getEntryInLeaf(const void* entryToProcess, AttrType keyType,unsigned entryNum, RID &rid)
{
	char *ridsPtr = NULL;
	if (keyType == TypeInt)
	{
		ridsPtr = (char*)entryToProcess + sizeof(int) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));
	}
	else if (keyType == TypeReal)
	{
		ridsPtr = (char*)entryToProcess + sizeof(float) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));
	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        ridsPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));

	}
    rid.pageNum = *ridsPtr;
    rid.slotNum = *(ridsPtr+sizeof(PageNum));

	return 0;
}

RC IndexManager::setEntryInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid)
{
	char *ridsPtr = NULL;
	if (keyType == TypeInt)
	{
		ridsPtr = (char*)entryToProcess + sizeof(int) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));

	}
	else if (keyType == TypeReal)
	{
		ridsPtr = (char*)entryToProcess + sizeof(float) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));

	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        ridsPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset));
	}

	*((unsigned*)ridsPtr) = rid.pageNum;
	ridsPtr = ridsPtr + sizeof(PageNum);
	*((unsigned*)ridsPtr) = rid.slotNum;
	return 0;
}

RC IndexManager::extractVarChar(const void* data)
{
    int sizeOfVarChar = *((int*)data);
    char *varChar = (char*)data + sizeof(int);
    return string(varChar,sizeOfVarChar);
}



IndexManager* IndexManager::instance()
{
	if(!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
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

RC IndexManager::destroyFile(const string &fileName)
{
	const char* fileName_char = fileName.c_str();
	RC success = remove(fileName_char); //delete file
	if(success == 0)
	{
		return 0; //successful
	}

	return -1; //A file still exists
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	fstream *file_to_open = ixfileHandle.getFileStream();
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


	ixfileHandle.setFileStream(file_to_open);
	ixfileHandle.setNumberOfPages(ceil(length / PAGE_SIZE)); //initial number of pages

	return 0;

}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	fstream *file_to_close = ixfileHandle.getFileStream();
	if(file_to_close->is_open())
	{
		file_to_close->close();
		return 0;
	}

	return -1;

}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc = -1;
	void *pageToProcess = tempPage0;
	//RC rc = ixfileHandle.fileHandle.readPage(rid.pageNum,pageToProcess);//Read Page
	if(rc != 0)
		return rc;


    NodeType nodeType = getNodeType(pageToProcess);
    if (nodeType == ROOT_NODE || nodeType == INTER_NODE)
    {


    }
    else if(nodeType == LEAF_NODE)
    {


    }

	//if it is non leaf node
	//RC = binary search to find location to insert(-1, leftmost child is candidate)

	//if it is leaf Node
	//offset of entry = binarysearch(pageToProcess,key) // binary search to find location to insert
	//check whether there is an entry with the same key
	//verifyFreeSpace(pageToProcess, size of inserted data)
	//if there is free space -> put the data
	//if not :
	//         split
	//

	return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool			lowKeyInclusive,
		bool        	highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}


IXFileHandle::IXFileHandle()
{
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
	numberOfPages = 0;
	file_stream = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}

void IXFileHandle::setNumberOfPages(unsigned numberToSet)
{
	numberOfPages = numberToSet;
}

unsigned IXFileHandle::getNumberOfPages()
{

	return numberOfPages;
}


void IXFileHandle::setFileStream(void *file_stream_arg)
{
	file_stream = (fstream*)file_stream_arg;
}

fstream* IXFileHandle::getFileStream()
{
	return file_stream;
}

