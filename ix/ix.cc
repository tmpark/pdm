
#include "ix.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library
#include <cstring>

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
RC IndexManager::getKeyOfEntry(const void* entryToProcess, T &value)
{
	value = *((T*)entryToProcess);
	return 0;
}

RC IndexManager::getKeyOfEntry(const void* entryToProcess, string &value)
{
    int sizeOfVarChar = *((int*)entryToProcess);
    char *varChar = (char*)entryToProcess + sizeof(int);
    value = string(varChar,sizeOfVarChar);
	return 0;
}

template <typename T>
RC IndexManager::setKeyOfEntry(void* entryToProcess, T value)
{
	*((T*)entryToProcess) = value;
	return 0;
}

RC IndexManager::setKeyOfEntry(void* entryToProcess, string value)
{
	int sizeOfVarChar = value.size();
	*((int*)entryToProcess) = sizeOfVarChar;
	memcpy((char*)entryToProcess + sizeof(int), value.c_str(), sizeOfVarChar);
	return 0;
}

PageNum IndexManager::getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType)
{
	char *childPtr = NULL;
	if (keyType == TypeInt)
	{
		childPtr = (char*)entryToProcess + sizeof(int);
	}
	else if (keyType == TypeReal)
	{
		childPtr = (char*)entryToProcess + sizeof(float);
	}
	else if (keyType == TypeVarChar)
	{
        int sizeOfVarChar = *((int*)entryToProcess);
        childPtr = (char*)entryToProcess + sizeof(int) + sizeOfVarChar;
	}
	return *((PageNum*)childPtr);
}

RC IndexManager::setChildOfIntermediateEntry(void* entryToProcess, AttrType keyType, PageNum childPageNum)
{
	char *childPtr = NULL;
	if (keyType == TypeInt)
	{
		childPtr = (char*)entryToProcess + sizeof(int);
	}
	else if (keyType == TypeReal)
	{
		childPtr = (char*)entryToProcess + sizeof(float);
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



unsigned IndexManager::getSizeOfEntryInLeaf(const void* entryToProcess, AttrType keyType)
{
	unsigned entrySize = 0;

	if (keyType == TypeInt)
	{
		entrySize = entrySize + sizeof(int);
		char *numOfRidsPtr = (char*)entryToProcess + entrySize;
		NumOfEnt numOfRids = *((NumOfEnt*)numOfRidsPtr);
		entrySize = entrySize + sizeof(NumOfEnt);
		entrySize = entrySize + numOfRids*(sizeof(PageNum) + sizeof(SlotOffset));
	}
	else if (keyType == TypeReal)
	{
		entrySize = entrySize + sizeof(float);
		char *numOfRidsPtr = (char*)entryToProcess + entrySize;
		NumOfEnt numOfRids = *((NumOfEnt*)numOfRidsPtr);
		entrySize = entrySize + sizeof(NumOfEnt);
		entrySize = entrySize + numOfRids*(sizeof(PageNum) + sizeof(SlotOffset));

	}
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)entryToProcess);
		entrySize = entrySize + sizeof(int);
		entrySize = entrySize + sizeOfVarChar;
		char *numOfRidsPtr = (char*)entryToProcess + entrySize;
		NumOfEnt numOfRids = *((NumOfEnt*)numOfRidsPtr);
		entrySize = entrySize + sizeof(NumOfEnt);
		entrySize = entrySize + numOfRids*(sizeof(PageNum) + sizeof(SlotOffset));
	}
	return entrySize;
}

unsigned IndexManager::getSizeOfEntryInIntermediate(const void* entryToProcess, AttrType keyType)
{
	unsigned entrySize = 0;

	if (keyType == TypeInt)
	{
		entrySize = entrySize + sizeof(int);

		entrySize = entrySize + sizeof(PageNum);
	}
	else if (keyType == TypeReal)
	{
		entrySize = entrySize + sizeof(float);

		entrySize = entrySize + sizeof(PageNum);
	}
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)entryToProcess);
		entrySize = entrySize + sizeof(int);
		entrySize = entrySize + sizeOfVarChar;

		entrySize = entrySize + sizeof(PageNum);
	}
	return entrySize;
}


SlotOffset IndexManager::findEntryOffsetToProcess(void *pageToProcess,AttrType attrType, const void *key)
{
	RC rc;
	unsigned numOfEntry = getNumOfEnt(pageToProcess);
	NodeType nodeType = getNodeType(pageToProcess);

	SlotOffset lastBigestEntryOffset = -1;

	SlotOffset currentEntryOffset = 0;


	for(unsigned i = 0 ; i < numOfEntry ; i++)
	{
		char* entryToProcess = (char*)pageToProcess + currentEntryOffset;
		if (attrType == TypeInt)
		{
			int candidateValue;
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
	        int objectiveValue = *((int*)key);
	        if (candidateValue <= objectiveValue)
	        {

	        	lastBigestEntryOffset = currentEntryOffset;
	        }

		}
		else if (attrType == TypeReal)
		{
			float candidateValue;
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
	        float objectiveValue = *((float*)key);
	        if (candidateValue <= objectiveValue)
	        {

	        	lastBigestEntryOffset = currentEntryOffset;
	        }

		}
		else if (attrType == TypeVarChar)
		{
			string candidateValue;
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
	        string objectiveValue = extractVarChar(key);
	        if (candidateValue <= objectiveValue)
	        {

	        	lastBigestEntryOffset = currentEntryOffset;
	        }

		}

		if(lastBigestEntryOffset != currentEntryOffset)
		{
			break;
		}

		//next entry
		if(nodeType == LEAF_NODE)
		    currentEntryOffset = currentEntryOffset + getSizeOfEntryInLeaf(entryToProcess, attrType);
		else if (nodeType == ROOT_NODE || nodeType == INTER_NODE)
			currentEntryOffset = currentEntryOffset + getSizeOfEntryInIntermediate(entryToProcess, attrType);
	}


	return lastBigestEntryOffset;
}


string IndexManager::extractVarChar(const void* data)
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

	RC success = PagedFileManager::instance()->createFile(fileName);
	return success;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC success = PagedFileManager::instance()->destroyFile(fileName);
		return success;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	RC success = PagedFileManager::instance()->openFile(fileName,ixfileHandle.fileHandle);
	return success;

}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC success = PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
	return success;

}

RC IndexManager::_insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc = -1;
	void *pageToProcess = tempPage0;
    rc = ixfileHandle.fileHandle.readPage(rid.pageNum,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,key);

    NodeType nodeType = getNodeType(pageToProcess);

    if (nodeType == ROOT_NODE || nodeType == INTER_NODE)
    {


    }
    else if(nodeType == LEAF_NODE)
    {


    }
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{


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
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return fileHandle.collectCounterValues(readPageCount,writePageCount, appendPageCount);
}


