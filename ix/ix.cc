
#include "ix.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library
#include <cstring>
#include <cstdlib>

IndexManager* IndexManager::_index_manager = 0;

SlotOffset IndexManager::getFreeSpaceOffset(const void* pageToProcess) const
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

NumOfEnt IndexManager::getNumOfEnt(const void* pageToProcess) const
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

PageNum IndexManager::getTombstone(const void* pageToProcess) const
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

NodeType IndexManager::getNodeType(const void* pageToProcess) const
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

PageNum IndexManager::getParentPageNum(const void* pageToProcess) const
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

PageNum IndexManager::getLeftSiblingPageNum(const void* pageToProcess) const
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

PageNum IndexManager::getRightSiblingPageNum(const void* pageToProcess) const
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

PageNum IndexManager::getLeftMostChildPageNum(const void* pageToProcess) const
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

unsigned IndexManager::getFreeSpaceSize(void* pageToProcess) const
{
	SlotOffset freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	return PAGE_SIZE - (PAGE_DIC_SIZE + freeSpaceOffset);
}

unsigned IndexManager::getFreeSpaceSizeForOverflowPage(void* pageToProcess) const
{
	SlotOffset freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	return PAGE_SIZE - (OVERFLOW_PAGE_DIC_SIZE + freeSpaceOffset);
}






template <typename T>
RC IndexManager::getKeyOfEntry(const void* entryToProcess, T &value) const
{
	value = *((T*)entryToProcess);
	return 0;
}

RC IndexManager::getKeyOfEntry(const void* entryToProcess, string &value) const
{
	int sizeOfVarChar = *((int*)entryToProcess);
	char *varChar = (char*)entryToProcess + sizeof(int);
	value = string(varChar,sizeOfVarChar);
	return 0;
}

template <typename T>
RC IndexManager::setKeyOfEntry(void* entryToProcess, T value)
{
	(*((T*)entryToProcess)) = value;
	return 0;
}

RC IndexManager::setKeyOfEntry(void* entryToProcess, string value)
{
	int sizeOfVarChar = value.size();
	*((int*)entryToProcess) = sizeOfVarChar;
	memcpy((char*)entryToProcess + sizeof(int), value.c_str(), sizeOfVarChar);
	return 0;
}

RC IndexManager::copyKeyOfEntry(void *to, const void *from, AttrType keyType)
{
	//Extract key value
	if(keyType == TypeInt)
	{
		int value;
		getKeyOfEntry(from,value);
		setKeyOfEntry(to,value);

	}
	else if(keyType == TypeReal)
	{
		float value;
		getKeyOfEntry(from,value);
		setKeyOfEntry(to,value);
	}
	else if(keyType == TypeVarChar)
	{
		string value;
		getKeyOfEntry(from,value);
		setKeyOfEntry(to,value);
	}
	return 0;
}


PageNum IndexManager::getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType) const
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

NumOfEnt IndexManager::getNumOfRIDsInLeafEntry(const void* entryToProcess, AttrType keyType) const
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


RC IndexManager::getRIDInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid) const
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

	rid.pageNum = *((PageNum*)ridsPtr);
	ridsPtr = ridsPtr + sizeof(PageNum);
	rid.slotNum = *((SlotOffset*)ridsPtr);
	return 0;
}




RC IndexManager::setRIDInLeaf(void* entryToProcess, AttrType keyType, unsigned entryNum, const RID &rid)
{
	char *ridsPtr = NULL;
	PageNum pageNum = rid.pageNum;
	SlotOffset slotNum = rid.slotNum;

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
	*((PageNum*)ridsPtr) = pageNum;
	ridsPtr = ridsPtr + sizeof(PageNum);
	*((SlotOffset*)ridsPtr) = slotNum;

	return 0;
}


SlotOffset IndexManager::getRIDOffsetInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum) const
{
	if (keyType == TypeInt)
	{
		return (sizeof(int) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset)));

	}
	else if (keyType == TypeReal)
	{
		return (sizeof(float) + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset)));

	}
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)entryToProcess);
		return (sizeof(int) + sizeOfVarChar + sizeof(NumOfEnt) + entryNum*(sizeof(PageNum) + sizeof(SlotOffset)));
	}

	return -1;
}



RC IndexManager::getRIDInOverFlowPage(const void* pageToProcess, unsigned entryNum, RID &rid) const
{
	SlotOffset ridOffset = (sizeof(PageNum) + sizeof(SlotOffset))*entryNum;
	char *ridsPtr = (char*)pageToProcess + ridOffset;
	rid.pageNum = *((PageNum*)ridsPtr);
	ridsPtr = ridsPtr + sizeof(PageNum);
	rid.slotNum = *((SlotOffset*)ridsPtr);
	return 0;
}

RC IndexManager::setRIDInOverFlowPage(const void* pageToProcess, unsigned entryNum, const RID &rid)
{
	PageNum pageNum = rid.pageNum;
	SlotOffset slotNum = rid.slotNum;

	SlotOffset ridOffset = (sizeof(PageNum) + sizeof(SlotOffset))*entryNum;

	char *ridsPtr = (char*)pageToProcess + ridOffset;
	*((PageNum*)ridsPtr) = pageNum;
	ridsPtr = ridsPtr + sizeof(PageNum);
	*((SlotOffset*)ridsPtr) = slotNum;
	return 0;
}

unsigned IndexManager::calNewLeafEntrySize(const void* key, AttrType keyType) const
{
	unsigned entrySize = 0;

	if (keyType == TypeInt)
		entrySize = entrySize + sizeof(int);
	else if (keyType == TypeReal)
		entrySize = entrySize + sizeof(float);
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)key);
		entrySize = entrySize + sizeof(int);
		entrySize = entrySize + sizeOfVarChar;
	}
	entrySize = entrySize + sizeof(NumOfEnt);
	entrySize = entrySize + sizeof(PageNum);
	entrySize = entrySize + sizeof(SlotOffset);
	return entrySize;
}

unsigned IndexManager::calNewInterEntrySize(const void* key, AttrType keyType) const
{
	unsigned entrySize = 0;

	if (keyType == TypeInt)
		entrySize = entrySize + sizeof(int);
	else if (keyType == TypeReal)
		entrySize = entrySize + sizeof(float);
	else if (keyType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)key);
		entrySize = entrySize + sizeof(int);
		entrySize = entrySize + sizeOfVarChar;
	}
	entrySize = entrySize + sizeof(PageNum);
	return entrySize;
}



unsigned IndexManager::getSizeOfEntryInLeaf(const void* entryToProcess, AttrType keyType) const
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

unsigned IndexManager::getSizeOfEntryInIntermediate(const void* entryToProcess, AttrType keyType) const
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

bool IndexManager::compareKeys(const void *key1,CompOp op, const void *key2,  AttrType keyType) const
{
	if(key1 == NULL || key2 == NULL)
		return false;

	if (keyType == TypeInt)
	{
		int value1;
		int value2;
		getKeyOfEntry(key1,value1);
		getKeyOfEntry(key2,value2);
		if(op == EQ_OP)
			return (value1 == value2);
		if(op == LT_OP)
			return (value1 < value2);
		if(op == LE_OP)
			return (value1 <= value2);
		if(op == GT_OP)
			return (value1 > value2);
		if(op == GE_OP)
			return (value1 >= value2);
		if(op == NE_OP)
			return (value1 != value2);
	}
	else if (keyType == TypeReal)
	{
		float value1;
		float value2;
		getKeyOfEntry(key1,value1);
		getKeyOfEntry(key2,value2);
		if(op == EQ_OP)
			return (value1 == value2);
		if(op == LT_OP)
			return (value1 < value2);
		if(op == LE_OP)
			return (value1 <= value2);
		if(op == GT_OP)
			return (value1 > value2);
		if(op == GE_OP)
			return (value1 >= value2);
		if(op == NE_OP)
			return (value1 != value2);
	}
	else if (keyType == TypeVarChar)
	{
		string value1;
		string value2;
		getKeyOfEntry(key1,value1);
		getKeyOfEntry(key2,value2);

		//cout << value1 <<endl;
		//cout << value2 <<endl;

		if(op == EQ_OP)
			return (value1 == value2);
		if(op == LT_OP)
			return (value1 < value2);
		if(op == LE_OP)
			return (value1 <= value2);
		if(op == GT_OP)
			return (value1 > value2);
		if(op == GE_OP)
			return (value1 >= value2);
		if(op == NE_OP)
			return (value1 != value2);
	}
	return false;
}


SlotOffset IndexManager::findEntryOffsetToProcess(void *pageToProcess,AttrType attrType, const void *key)
{
	if(key == NULL)
		return -1;

	unsigned numOfEntry = getNumOfEnt(pageToProcess);
	NodeType nodeType = getNodeType(pageToProcess);

	SlotOffset currentEntryOffset = 0;
	SlotOffset candidateEntryOffset = -1;


	for(unsigned i = 0 ; i < numOfEntry ; i++)
	{
		char* entryToProcess = (char*)pageToProcess + currentEntryOffset;
		if (attrType == TypeInt)
		{
			int candidateValue;
			getKeyOfEntry((const void*)entryToProcess, candidateValue);
			int objectiveValue = *((int*)key);
			if (candidateValue <= objectiveValue)
				candidateEntryOffset = currentEntryOffset;

		}
		else if (attrType == TypeReal)
		{
			float candidateValue;
			getKeyOfEntry((const void*)entryToProcess, candidateValue);
			float objectiveValue = *((float*)key);
			if (candidateValue <= objectiveValue)
				candidateEntryOffset = currentEntryOffset;
		}
		else if (attrType == TypeVarChar)
		{
			string candidateValue;
			getKeyOfEntry((const void*)entryToProcess, candidateValue);
			string objectiveValue = extractVarChar(key);

			//cout << candidateValue <<endl;
			//cout << objectiveValue <<endl;

			if (candidateValue <= objectiveValue)
				candidateEntryOffset = currentEntryOffset;
		}


		if(candidateEntryOffset != currentEntryOffset)
			return candidateEntryOffset;


		//next entry
		if(nodeType == LEAF_NODE)
			currentEntryOffset = currentEntryOffset + getSizeOfEntryInLeaf(entryToProcess, attrType);
		else if (nodeType == ROOT_NODE || nodeType == INTER_NODE)
			currentEntryOffset = currentEntryOffset + getSizeOfEntryInIntermediate(entryToProcess, attrType);
	}

	return candidateEntryOffset;
}


string IndexManager::extractVarChar(const void* data)
{
	int sizeOfVarChar = *((int*)data);
	char *varChar = (char*)data + sizeof(int);
	return string(varChar,sizeOfVarChar);
}

RC IndexManager::moveEntries(void *pageToProcess,SlotOffset from,unsigned amountToMove, MoveDirection direction)
{
	unsigned freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	unsigned amountOfData = freeSpaceOffset - from;

	char buf[amountOfData];
	memcpy(buf,(char*)pageToProcess+from,amountOfData);
	if(direction == MoveForward)
	{
		memcpy((char*)pageToProcess+from+amountToMove,buf,amountOfData);
		setFreeSpaceOffset(pageToProcess,freeSpaceOffset + amountToMove);
	}
	else if(direction == MoveBackward)
	{
		memcpy((char*)pageToProcess+from-amountToMove,buf,amountOfData);
		setFreeSpaceOffset(pageToProcess,freeSpaceOffset - amountToMove);
	}


	return 0;
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
	if(success != 0)
		return success;

	IXFileHandle ixfileHandle;
	success = PagedFileManager::instance()->openFile(fileName,ixfileHandle.fileHandle);
	if(success != 0)
		return success;


	//Root Page
	setFreeSpaceOffset(tempPage, 0);
	setNumOfEnt(tempPage, 0);
	setTombstone(tempPage, -1);
	setNodeType(tempPage, LEAF_NODE);
	setParentPageNum(tempPage, -1);
	setLeftSiblingPageNum(tempPage, -1);
	setRightSiblingPageNum(tempPage, -1);
	setLeftMostChildPageNum(tempPage, -1);

	success = ixfileHandle.fileHandle.appendPage(tempPage);
	if(success != 0)
		return success;

	success = PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);

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


RC IndexManager::putEntryInItermediate(void *entryToProcess, AttrType attrType, const void *key, PageNum pageNum)
{
	if (attrType == TypeInt)
	{
		memcpy(entryToProcess, key, sizeof(int));
		memcpy((char*)entryToProcess + sizeof(int),(char*)&pageNum,sizeof(PageNum));

	}
	else if (attrType == TypeReal)
	{
		memcpy(entryToProcess, key, sizeof(float));
		memcpy((char*)entryToProcess + sizeof(float),(char*)&pageNum,sizeof(PageNum));
	}
	else if (attrType == TypeVarChar)
	{
		int sizeOfVarChar = *((int*)key);
		memcpy((char*)entryToProcess, key, sizeof(int));
		memcpy((char*)entryToProcess + sizeof(int), (char*)key + sizeof(int), sizeOfVarChar);
		memcpy((char*)entryToProcess + sizeof(int) + sizeOfVarChar,(char*)&pageNum,sizeof(PageNum));
	}
	return 0;

}

RC IndexManager::putEntryInLeaf(void *entryToProcess, AttrType attrType, const void *key, RID rid, bool existing)
{

	if(existing)
	{
		NumOfEnt numOfEntry = getNumOfRIDsInLeafEntry(entryToProcess,attrType);
		setRIDInLeaf(entryToProcess,attrType,numOfEntry,rid);
		setNumOfRIDsInLeaf(entryToProcess, attrType,numOfEntry + 1);
	}
	else
	{
		if (attrType == TypeInt)
		{
			memcpy(entryToProcess, key, sizeof(int));
			setRIDInLeaf(entryToProcess,attrType,0,rid);
			setNumOfRIDsInLeaf(entryToProcess,attrType,1);
		}
		else if (attrType == TypeReal)
		{
			memcpy(entryToProcess, key, sizeof(float));
			setRIDInLeaf(entryToProcess,attrType,0,rid);
			setNumOfRIDsInLeaf(entryToProcess,attrType,1);

		}
		else if (attrType == TypeVarChar)
		{
			int sizeOfVarChar = *((int*)key);
			memcpy((char*)entryToProcess, key, sizeof(int));
			memcpy((char*)entryToProcess + sizeof(int) , (char*)key + sizeof(int), sizeOfVarChar);
			setRIDInLeaf(entryToProcess,attrType,0,rid);
			setNumOfRIDsInLeaf(entryToProcess,attrType,1);
		}
	}
	return 0;

}

RC IndexManager::insertEntryInOverflowPage(IXFileHandle &ixfileHandle,PageNum currentNodePage,void *pageToProcess,const RID &rid, AttrType attrType)
{
	RC rc = -1;
	PageNum tombstone = getTombstone(pageToProcess);
	NodeType nodeType = getNodeType(pageToProcess);

	if(nodeType == OVER_NODE)
	{
		unsigned numOfRids = getNumOfEnt(pageToProcess);
		for(unsigned i = 0 ; i < numOfRids ; i++)
		{
			RID extractedRID;
			getRIDInOverFlowPage(pageToProcess,i,extractedRID);
			if(extractedRID.pageNum == rid.pageNum && extractedRID.slotNum == rid.slotNum)
			{
				return -1;
			}
		}

	}

	//go to another overflow page
	if(tombstone != -1)
	{
		char overFlowPageToProcess[PAGE_SIZE];
		//it is changed when new child node is created in its child
		rc = ixfileHandle.fileHandle.readPage(tombstone,overFlowPageToProcess);//Read Page
		if(rc != 0)
			return rc;
		rc = insertEntryInOverflowPage(ixfileHandle,tombstone,overFlowPageToProcess,rid, attrType);
		if(rc == -1)
			return rc;
	}
	//insert here
	else
	{
		unsigned entrySize = sizeof(PageNum) + sizeof(SlotOffset);
		unsigned freeSpace = 0;
		if(nodeType == OVER_NODE)
			freeSpace = getFreeSpaceSizeForOverflowPage(pageToProcess);
		else if(nodeType == LEAF_NODE)
			freeSpace = getFreeSpaceSize(pageToProcess);

		if(freeSpace >= entrySize)
		{
			unsigned numOfEnt = getNumOfEnt(pageToProcess);
			unsigned freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
			setRIDInOverFlowPage(pageToProcess,numOfEnt,rid);
			setFreeSpaceOffset(pageToProcess,freeSpaceOffset + entrySize);
			setNumOfEnt(pageToProcess,numOfEnt + 1);

		}
		//New Overflow Page
		else
		{
			//initialize overflow page
			char overFlowPageToProcess[PAGE_SIZE];
			setFreeSpaceOffset(overFlowPageToProcess,0);
			setNumOfEnt(overFlowPageToProcess, 0);
			setTombstone(overFlowPageToProcess,-1);
			setNodeType(overFlowPageToProcess,OVER_NODE);
			PageNum anotherOverFlowPage = ixfileHandle.fileHandle.getNumberOfPages();
			rc = ixfileHandle.fileHandle.appendPage(overFlowPageToProcess);
			if(rc != 0)
				return rc;
			rc = insertEntryInOverflowPage(ixfileHandle,anotherOverFlowPage,overFlowPageToProcess, rid, attrType);

			rc =setTombstone(pageToProcess,anotherOverFlowPage);
		}
		//writePage to update tombstone
		rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
		if(rc != 0)
			return rc;


	}
	return rc;

}

RC IndexManager::deleteEntryInOverflowPage(IXFileHandle &ixfileHandle,PageNum currentNodePage,void *pageToProcess,const RID &rid)
{
	RC rc = -1;
	SlotOffset freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	NumOfEnt numOfEntry = getNumOfEnt(pageToProcess);
	PageNum tombStone = getTombstone(pageToProcess);
	NodeType nodeType = getNodeType(pageToProcess);


	//Searching for Rid in overflow page
	if(nodeType == OVER_NODE)
	{
		int targetRidNum = -1;
		for (unsigned i = 0 ; i < numOfEntry ; i++)
		{
			RID extractedRid;
			getRIDInOverFlowPage(pageToProcess,i,extractedRid);
			if(rid.pageNum == extractedRid.pageNum && rid.slotNum == extractedRid.slotNum)
			{
				targetRidNum = i;
				break;
			}
		}
		//found rid to delete: delete / write / return ok
		if(targetRidNum != -1)
		{
			unsigned sizeOfEntry = sizeof(PageNum) + sizeof(SlotOffset);
			//Overwrite target Rid
			unsigned offsetToCompact = sizeOfEntry*(targetRidNum+1);
			moveEntries(pageToProcess,offsetToCompact,sizeOfEntry,MoveBackward);
			setFreeSpaceOffset(pageToProcess,freeSpaceOffset - sizeOfEntry);
			setNumOfEnt(pageToProcess,numOfEntry - 1);
			rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
			if(rc != 0)
				return rc;
			return 0;
		}
	}

	//No more tombstone : There is no rid matching
	if(tombStone == -1)
		return -1;

	char overFlowPageToProcess[PAGE_SIZE];
	//it is changed when new child node is created in its child
	rc = ixfileHandle.fileHandle.readPage(tombStone,overFlowPageToProcess);//Read Page
	if(rc != 0)
		return rc;
	rc = deleteEntryInOverflowPage(ixfileHandle,tombStone,overFlowPageToProcess,rid) ;

	return rc;

}

RC IndexManager::_insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
		PageNum currentNodePage, void *newChildNodeKey, PageNum &newChildNodePage)
{
	/*
	string debug;
	getKeyOfEntry(newChildNodeKey,debug);
	cout << debug << endl;
	 */
	RC rc = -1;
	char pageToProcess[PAGE_SIZE];

	//it is changed when new child node is created in its child
	rc = ixfileHandle.fileHandle.readPage(currentNodePage,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	NodeType nodeType = getNodeType(pageToProcess);
	SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,key);

	char *entryToProcess = NULL;


	if (nodeType == INTER_NODE)
	{

		PageNum childNodePage = -1;
		//left most child pointer
		if(entryOffset == -1)
			childNodePage = getLeftMostChildPageNum(pageToProcess);
		else //general entry to point child
		{
			entryToProcess = pageToProcess + entryOffset;
			childNodePage = getChildOfIntermediateEntry(entryToProcess,attribute.type);
		}

		rc = _insertEntry(ixfileHandle,attribute,key,rid,
				childNodePage,newChildNodeKey,newChildNodePage);

		if(newChildNodePage == -1)
		{
			newChildNodeKey = NULL;
			newChildNodePage = -1;
		}
		else //there was new child
		{
			//check whether there is enough space
			unsigned entrySize = calNewInterEntrySize(newChildNodeKey,attribute.type);
			unsigned freeSpaceSize = getFreeSpaceSize(pageToProcess);
			SlotOffset offsetToInsert = 0;

			//-1 means first position
			if(entryOffset == -1)
				offsetToInsert = 0;
			//position to insert should be next to the found entry
			else
			{
				char *entryTemp = pageToProcess + entryOffset;
				offsetToInsert = entryOffset + getSizeOfEntryInIntermediate(entryTemp,attribute.type);
			}
			if(freeSpaceSize >= entrySize)
			{

				//push as much as entrySize
				rc = moveEntries(pageToProcess,offsetToInsert,entrySize,MoveForward);
				//insert
				char *newEntryPtr = (char*)pageToProcess + offsetToInsert;
				rc = putEntryInItermediate(newEntryPtr,attribute.type,newChildNodeKey,newChildNodePage);



				NumOfEnt numOfEntry = getNumOfEnt(pageToProcess);
				setNumOfEnt(pageToProcess, numOfEntry + 1);

				//writePage
				rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
				if(rc != 0)
					return rc;

				newChildNodeKey = NULL;
				newChildNodePage = -1;


			}
			//Not enough space : intermediate node split
			else
			{
				newChildNodePage = ixfileHandle.fileHandle.getNumberOfPages();
				char returnedEntry[PAGE_SIZE];
				char newChildPageToProcess[PAGE_SIZE];
				if(currentNodePage == 0)
				{
					char newRootPageToProcess[PAGE_SIZE];
					splitIntermediate(pageToProcess, newChildPageToProcess, newRootPageToProcess,
							returnedEntry, newChildNodeKey, attribute.type, newChildNodePage,
							newChildNodePage + 1, offsetToInsert);
					//Copy returned entry to newChildNodekey
					unsigned sizeOfKey = getSizeOfEntryInIntermediate(returnedEntry,attribute.type);
					memcpy(newChildNodeKey,returnedEntry,sizeOfKey);
					//WritePage(Current & newPage)
					rc = ixfileHandle.fileHandle.writePage(currentNodePage,newRootPageToProcess);//Write Page
					if(rc != 0)
						return rc;
					rc = ixfileHandle.fileHandle.appendPage(pageToProcess);
					if(rc != 0)
						return rc;
					rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
					if(rc != 0)
						return rc;
				}
				else
				{
					splitIntermediate(pageToProcess, newChildPageToProcess, NULL,
							returnedEntry, newChildNodeKey, attribute.type, currentNodePage,
							newChildNodePage, offsetToInsert);

					//Copy returned entry to newChildNodekey
					unsigned sizeOfKey = getSizeOfEntryInIntermediate(returnedEntry,attribute.type);
					memcpy(newChildNodeKey,returnedEntry,sizeOfKey);
					//WritePage(Current & newPage)
					rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
					if(rc != 0)
						return rc;
					rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
					if(rc != 0)
						return rc;
					/*
					unsigned numOfEnt = getNumOfEnt(pageToProcess);
					char *entryToProcessTemp = pageToProcess;
					for(unsigned i = 0; i <  numOfEnt; i++)
					{
						string kkk;
						getKeyOfEntry(entryToProcessTemp, kkk);
						cout << kkk << getChildOfIntermediateEntry(entryToProcessTemp,attribute.type) <<endl;
						entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInIntermediate(entryToProcessTemp,attribute.type);
					}

					printf("************************************child****************************************\n");

					numOfEnt = getNumOfEnt(newChildPageToProcess);
					entryToProcessTemp = newChildPageToProcess;
					for(unsigned i = 0; i <  numOfEnt; i++)
					{
						string kkk;
						getKeyOfEntry(entryToProcessTemp, kkk);
						cout << kkk << getChildOfIntermediateEntry(entryToProcessTemp,attribute.type) <<endl;
						entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInIntermediate(entryToProcessTemp,attribute.type);
					}


					printf("sibal\n");
					string sibal;
					getKeyOfEntry(newChildNodeKey,sibal);
					cout << sibal << getChildOfIntermediateEntry(newChildNodeKey,attribute.type) <<endl;
					cout << sibal << "\t" << endl;


					 */

				}


			}
		}

	}
	else if(nodeType == LEAF_NODE)
	{
		char *ptrToInsert = NULL;
		SlotOffset offsetToInsert = 0;
		SlotOffset offsetToPush = 0;
		bool sameKey = false;

		//-1 means first position
		if(entryOffset == -1)
		{
			offsetToInsert = 0;
			offsetToPush = 0;
			entryToProcess = pageToProcess;

		}
		else
		{
			entryToProcess = pageToProcess + entryOffset;

			/*
			string ext;
			getKeyOfEntry(entryToProcess,ext);
			cout << ext << endl;

			RID extrid;
			getRIDInLeaf(entryToProcess,attribute.type,0,extrid);

			getKeyOfEntry(key,ext);
			cout << ext << endl;
			 */

			sameKey = compareKeys(key,EQ_OP,entryToProcess,attribute.type);
			if(sameKey)
				offsetToInsert = entryOffset;
			else
				offsetToInsert = entryOffset + getSizeOfEntryInLeaf(entryToProcess,attribute.type);

			offsetToPush = entryOffset + getSizeOfEntryInLeaf(entryToProcess,attribute.type);
		}

		//There is already same rid in same key : fail!
		if(sameKey)
		{
			unsigned numOfRids = getNumOfRIDsInLeafEntry(entryToProcess,attribute.type);
			for(unsigned i = 0 ; i < numOfRids ; i++)
			{
				RID extractedRID;
				getRIDInLeaf(entryToProcess,attribute.type,i,extractedRID);
				if(extractedRID.pageNum == rid.pageNum && extractedRID.slotNum == rid.slotNum)
				{
					return -1;
				}
			}

		}


		ptrToInsert = pageToProcess + offsetToInsert;

		PageNum tombstone = getTombstone(pageToProcess);

		//Overflowed Node: go inside overflow page and return null
		if((tombstone != -1) && sameKey)
		{
			rc = insertEntryInOverflowPage(ixfileHandle,currentNodePage,pageToProcess,rid, attribute.type);
			newChildNodeKey = NULL;
			newChildNodePage = -1;
			return rc;
		}

		unsigned freeSpaceSize = getFreeSpaceSize(pageToProcess);

		unsigned entrySize = 0;

		//Entry size is different in terms of new or not
		if(sameKey)
			entrySize = sizeof(PageNum) + sizeof(SlotOffset);
		else
			entrySize = calNewLeafEntrySize(key, attribute.type);


		if(freeSpaceSize >= entrySize)
		{

			//push as much as entrySize (Push check whether there needs push)
			rc = moveEntries(pageToProcess,offsetToPush,entrySize,MoveForward);
			rc = putEntryInLeaf(ptrToInsert,attribute.type,key,rid,sameKey);

			if(!sameKey)//slot increases
			{
				NumOfEnt numOfEntry = getNumOfEnt(pageToProcess);
				setNumOfEnt(pageToProcess, numOfEntry + 1);
			}



			//writePage
			rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
			if(rc != 0)
				return rc;

			newChildNodeKey = NULL;
			newChildNodePage = -1;



		}
		//Same key insertion and num of entry == 1 and no space means it requires overflow page.
		else if(sameKey && (getNumOfEnt(pageToProcess) == 1))
		{
			rc = insertEntryInOverflowPage(ixfileHandle,currentNodePage, pageToProcess, rid, attribute.type);
			newChildNodeKey = NULL;
			newChildNodePage = -1;
			return rc;
		}
		//split
		else
		{
			/*
			unsigned numOfEnt = getNumOfEnt(pageToProcess);
			char *entryToProcessTemp = pageToProcess;
			for(unsigned i = 0; i <  numOfEnt; i++)
			{
				string key;
				getKeyOfEntry(entryToProcessTemp,key);
				cout << key << endl;
				unsigned numOfRid = getNumOfRIDsInLeafEntry(entryToProcessTemp,attribute.type) ;
				for(unsigned j = 0 ; j < numOfRid; j++)
				{
					RID extracted;
					getRIDInLeaf(entryToProcessTemp,attribute.type,j,extracted);
					cout << extracted.pageNum << '\t' << extracted.slotNum << endl << flush;
				}
				entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInLeaf(entryToProcessTemp,attribute.type);
			}
			 */
			//cout << getFreeSpaceOffset(pageToProcess)<<"\t"<<getNumOfEnt(pageToProcess)<<"\t"<<getTombstone(pageToProcess)<<"\t"<<getNodeType(pageToProcess)<<"\t"<<getParentPageNum(pageToProcess)<<"\t"<<getLeftSiblingPageNum(pageToProcess)<<"\t"<<getRightSiblingPageNum(pageToProcess)<<"\t"<<getLeftMostChildPageNum(pageToProcess)<<endl<<endl<<endl<<endl<<endl;

			/*
			if(getFreeSpaceOffset(pageToProcess) == 3869)
			{
				printf("sibal\n");
			}


			unsigned numOfEnt = getNumOfEnt(pageToProcess);
			char *entryToProcessTemp = pageToProcess;
			for(unsigned i = 0; i <  numOfEnt; i++)
			{
				int key;
				getKeyOfEntry(entryToProcessTemp,key);
				cout << key << endl;
				unsigned numOfRid = getNumOfRIDsInLeafEntry(entryToProcessTemp,attribute.type) ;
				for(unsigned j = 0 ; j < numOfRid; j++)
				{
					RID extracted;
					getRIDInLeaf(entryToProcessTemp,attribute.type,j,extracted);
					cout << extracted.pageNum << '\t' << extracted.slotNum << endl << flush;
				}
				entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInLeaf(entryToProcessTemp,attribute.type);
			}
			 */


			char newChildPageToProcess[PAGE_SIZE];
			newChildNodePage = ixfileHandle.fileHandle.getNumberOfPages();


			//splitLeaf(get child node key)
			splitLeaf(pageToProcess, newChildPageToProcess, newChildNodeKey,
					currentNodePage, newChildNodePage,offsetToInsert, attribute, key, rid, !sameKey);

			if(currentNodePage == 0)
			{
				rc = ixfileHandle.fileHandle.writePage(currentNodePage,newChildNodeKey);//Write Page
				if(rc != 0)
					return rc;
				rc = ixfileHandle.fileHandle.appendPage(pageToProcess);
				if(rc != 0)
					return rc;
				rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
				if(rc != 0)
					return rc;
			}
			else
			{
				rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
				if(rc != 0)
					return rc;

				rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
				if(rc != 0)
					return rc;
			}
			/*
			string sibal;
			getKeyOfEntry(newChildNodeKey,sibal);
			cout << sibal << "\t" << endl;
			getKeyOfEntry(key,sibal);
			cout << sibal << endl;
			 */
			//cout << getFreeSpaceOffset(newChildNodeKey)<<"\t"<<getNumOfEnt(newChildNodeKey)<<"\t"<<getTombstone(newChildNodeKey)<<"\t"<<getNodeType(newChildNodeKey)<<"\t"<<getParentPageNum(newChildNodeKey)<<"\t"<<getLeftSiblingPageNum(newChildNodeKey)<<"\t"<<getRightSiblingPageNum(newChildNodeKey)<<"\t"<<getLeftMostChildPageNum(newChildNodeKey)<<endl;
			//cout << getFreeSpaceOffset(pageToProcess)<<"\t"<<getNumOfEnt(pageToProcess)<<"\t"<<getTombstone(pageToProcess)<<"\t"<<getNodeType(pageToProcess)<<"\t"<<getParentPageNum(pageToProcess)<<"\t"<<getLeftSiblingPageNum(pageToProcess)<<"\t"<<getRightSiblingPageNum(pageToProcess)<<"\t"<<getLeftMostChildPageNum(pageToProcess)<<endl;
			//cout << getFreeSpaceOffset(newChildPageToProcess)<<"\t"<<getNumOfEnt(newChildPageToProcess)<<"\t"<<getTombstone(newChildPageToProcess)<<"\t"<<getNodeType(newChildPageToProcess)<<"\t"<<getParentPageNum(newChildPageToProcess)<<"\t"<<getLeftSiblingPageNum(newChildPageToProcess)<<"\t"<<getRightSiblingPageNum(newChildPageToProcess)<<"\t"<<getLeftMostChildPageNum(newChildPageToProcess)<<endl;


			/*
			numOfEnt = getNumOfEnt(pageToProcess);
		    entryToProcessTemp = pageToProcess;
			for(unsigned i = 0; i <  numOfEnt; i++)
			{
				string key;
				getKeyOfEntry(entryToProcessTemp,key);
				cout << key << endl;
				unsigned numOfRid = getNumOfRIDsInLeafEntry(entryToProcessTemp,attribute.type) ;
				for(unsigned j = 0 ; j < numOfRid; j++)
				{
					RID extracted;
					getRIDInLeaf(entryToProcessTemp,attribute.type,j,extracted);
					cout << extracted.pageNum << '\t' << extracted.slotNum << endl << flush;
				}
				entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInLeaf(entryToProcessTemp,attribute.type);
			}

			printf("************************************child****************************************\n");

			numOfEnt = getNumOfEnt(newChildPageToProcess);
			entryToProcessTemp = newChildPageToProcess;
			for(unsigned i = 0; i <  numOfEnt; i++)
			{
				string key;
				getKeyOfEntry(entryToProcessTemp,key);
				cout << key << endl;
				unsigned numOfRid = getNumOfRIDsInLeafEntry(entryToProcessTemp,attribute.type) ;
				for(unsigned j = 0 ; j < numOfRid; j++)
				{
					RID extracted;
					getRIDInLeaf(entryToProcessTemp,attribute.type,j,extracted);
					cout << extracted.pageNum << '\t' << extracted.slotNum << endl << flush;
				}
				entryToProcessTemp = entryToProcessTemp + getSizeOfEntryInLeaf(entryToProcessTemp,attribute.type);
			}
			 */
			/*
			printf("sibal\n");
			string sibal;
			getKeyOfEntry(newChildNodeKey,sibal);
			cout << sibal << "\t" << endl;
			 */
		}
	}

	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	fstream *fileStream = ixfileHandle.fileHandle.getFileStream();
	if(fileStream == NULL)
		return -1;
	RC rc;
	char newChildNodeKey[PAGE_SIZE];
	PageNum newChildNodePage = -1;

	rc = _insertEntry(ixfileHandle,attribute,key,rid,0,newChildNodeKey,newChildNodePage);

	return rc;
}

RC IndexManager::splitIntermediate(void *interNode, void *newInterNode, void *newRootNode,
		void *newChildEntry, void *entry, const AttrType entryType, PageNum interNodePN,
		PageNum newInterNodePN, int offset)
{


	int secondPartOffset = 0;
	int firstPartOffset = 0;
	int interNodeOffset = 0;
	char fPart[WHOLE_SIZE_FOR_ENTRIES];
	char *firstPart = fPart;
	char *secondPart = (char *) newInterNode;

	int entSize = 0;
	int newEntrySize = getSizeOfEntryInIntermediate(entry, entryType);
	int numOfEntriesF = 0;
	bool inserted = false;
	while(interNodeOffset < WHOLE_SIZE_FOR_ENTRIES/2)
	{
		numOfEntriesF++;
		if(interNodeOffset == offset && !inserted)
		{
			memcpy(firstPart + firstPartOffset, entry, newEntrySize);
			firstPartOffset += newEntrySize;
			inserted = true;
			continue;
		}
		entSize = getSizeOfEntryInIntermediate(((char *)interNode) + interNodeOffset, entryType);
		memcpy(firstPart + firstPartOffset, ((char *)interNode) + interNodeOffset, entSize);
		firstPartOffset += entSize;
		interNodeOffset += entSize;
	}


	//Create newChildNode and setLeftMostChild of newInterNode
	int numOfEntriesS = 0;
	int freeSpaceOffset = getFreeSpaceOffset(interNode);
	int newChildEntrySize = 0;

	bool secondHasNothing = false;
	bool fuck = false;
	char *newCEntryBuff = (char *)newChildEntry;
	if(interNodeOffset != freeSpaceOffset)
	{
		if(interNodeOffset == offset)
		{
			putEntryInItermediate(newChildEntry, entryType, entry, newInterNodePN);
			setLeftMostChildPageNum(secondPart,	getChildOfIntermediateEntry(entry, entryType));
			fuck = true;
		}
		else
		{
			putEntryInItermediate(newChildEntry, entryType, (((char *)interNode) + interNodeOffset), newInterNodePN);
			setLeftMostChildPageNum(secondPart,	getChildOfIntermediateEntry(((char *)interNode) + interNodeOffset, entryType));
			interNodeOffset += getSizeOfEntryInIntermediate(((char *)interNode) + interNodeOffset, entryType);
		}//if(interNodeOffset == freeSpaceOffset) secondHasNothing = true;
	}
	else
	{
		secondHasNothing = true;
		putEntryInItermediate(newChildEntry, entryType, entry, newInterNodePN);
		//memcpy(newChildEntry, entry, newEntrySize);
		setLeftMostChildPageNum(secondPart,	getChildOfIntermediateEntry(entry, entryType));
	}
	newChildEntrySize = getSizeOfEntryInIntermediate(newChildEntry, entryType);

	//	//	if(interNodeOffset != freeSpaceOffset)
	//	//	{
	//	char *newCEntryBuff = (char *)newChildEntry;
	//
	//	if(entryType == TypeInt)
	//	{
	//		setKeyOfEntry(newChildEntry,*(int *)(((char *)interNode) + interNodeOffset));
	//		memcpy(newCEntryBuff + sizeof(int), &newInterNodePN, sizeof(PageNum));
	//		//setLeftChild
	//		int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(int));
	//		setLeftMostChildPageNum(secondPart, *pageNum);
	//		newChildEntrySize = sizeof(int) + sizeof(PageNum);
	//	}
	//	else if(entryType == TypeReal)
	//	{
	//		setKeyOfEntry(newChildEntry,*(float *)(((char *)interNode) + interNodeOffset));
	//		memcpy(newCEntryBuff + sizeof(float), &newInterNodePN, sizeof(PageNum));
	//		int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(float));
	//		setLeftMostChildPageNum(secondPart, *pageNum);
	//		newChildEntrySize = sizeof(float) + sizeof(PageNum);
	//	}
	//	else
	//	{
	//		string k = extractVarChar(((char *)interNode) + interNodeOffset);
	//		int kLength = k.length();
	//		char *newCEntryBuff = (char *)newChildEntry;
	//		memcpy(newCEntryBuff, &kLength, 4);
	//		memcpy(newCEntryBuff + 4, k.c_str(), kLength);
	//		memcpy(newCEntryBuff + 4 + kLength, &newInterNodePN, sizeof(PageNum));
	//		int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(int) + kLength);
	//		newChildEntrySize = sizeof(int) + kLength + sizeof(PageNum);
	//		setLeftMostChildPageNum(secondPart, *pageNum);
	//	}
	//	//	}
	//	//	else
	//	//	{
	//	//		cout << "ERROR splitINTER" << endl;
	//	//	}

	//Create new inter node
	inserted = false;
	while(interNodeOffset != freeSpaceOffset)
	{
		numOfEntriesS++;
		if(interNodeOffset == offset && !inserted && !fuck)
		{
			memcpy(secondPart + secondPartOffset, entry, newEntrySize);
			secondPartOffset += newEntrySize;
			inserted = true;
			continue;
		}
		entSize = getSizeOfEntryInIntermediate(((char *)interNode) + interNodeOffset, entryType);
		memcpy(secondPart + secondPartOffset, ((char *)interNode) + interNodeOffset, entSize);
		secondPartOffset += entSize;
		interNodeOffset += entSize;
	}

	if(freeSpaceOffset == offset && !secondHasNothing)
	{
		numOfEntriesS++;
		memcpy(secondPart + secondPartOffset, entry, newEntrySize);
		secondPartOffset += newEntrySize;
	}
	//FIXME: If intermediate node splits, it means parent page number for child nodes changes
	//FIXME:(cont) we dont have any method to correct it yet. We may need it in the future
	//update second part of Page DIC
	setRightSiblingPageNum(newInterNode, getRightSiblingPageNum(interNode));
	setLeftSiblingPageNum(newInterNode, interNodePN);
	setParentPageNum(newInterNode, getParentPageNum(interNode));
	setNodeType(newInterNode, INTER_NODE);
	setTombstone(newInterNode, -1);
	setNumOfEnt(newInterNode, (NumOfEnt)numOfEntriesS);
	setFreeSpaceOffset(newInterNode,(SlotOffset) secondPartOffset);

	//Write first part to existing interNode and update Page DIC
	memcpy(interNode, firstPart, firstPartOffset);
	setRightSiblingPageNum(interNode, newInterNodePN);
	setNumOfEnt(interNode, (NumOfEnt)numOfEntriesF);
	setFreeSpaceOffset(interNode,(SlotOffset) firstPartOffset);


	if(newRootNode != NULL)
	{
		setLeftMostChildPageNum(newRootNode, interNodePN);
		memcpy(newRootNode, newChildEntry, newChildEntrySize);
		//FIXME: should I set newChildEntry to NULL???
		setParentPageNum(newInterNode, 0);
		setParentPageNum(interNode, 0);
		setNodeType(newRootNode, INTER_NODE);
		setTombstone(newRootNode, -1);
		setNumOfEnt(newRootNode, 1);
		setFreeSpaceOffset(newRootNode, newChildEntrySize);

	}
	//if we will add rid to existing entry check current size + PageNum + SlotOffset < total free space in a PAGE
	//Or simply check numofentries =? 1. HAha it is more smarter way.
	//Depending on the result: newInterNode = overflow page or newInterNode = leaf page

	//if newInterNode == leaf page
	//

	return 0;
}



RC IndexManager::splitLeaf(void *leafNode, void *newLeafNode, void *newChildEntry,
		PageNum LeafNodePN, PageNum newLeafNodePN,
		int offset, const Attribute &Attribute, const void *key, const RID &rid, bool newEntryNeeded)
{

	//check whether we will add rid to existing entry or add new entry to node

	//FIXME
	if(getNumOfEnt(leafNode) == 1)
	{
		putEntryInLeaf(newLeafNode,Attribute.type, key, rid, false);
		putEntryInItermediate(newChildEntry, Attribute.type, key, newLeafNodePN + 1);

		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		if(LeafNodePN == 0)
		{
			setLeftSiblingPageNum(newLeafNode, newLeafNodePN);
			setParentPageNum(newLeafNode, 0);
		}
		else
		{
			setLeftSiblingPageNum(newLeafNode, LeafNodePN);
			setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		}
		setNodeType(newLeafNode, LEAF_NODE);
		setTombstone(newLeafNode, -1);
		setNumOfEnt(newLeafNode, 1);
		setFreeSpaceOffset(newLeafNode,getSizeOfEntryInLeaf(newLeafNode, Attribute.type));
		setLeftMostChildPageNum(newLeafNode, -1);

		//Write first part to existing leafNode and update Page DIC
		if(LeafNodePN == 0)
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN + 1);
			setParentPageNum(leafNode, 0);

			setLeftMostChildPageNum(newChildEntry, newLeafNodePN);
			setRightSiblingPageNum(newChildEntry, -1);
			setLeftSiblingPageNum(newChildEntry, -1);
			setParentPageNum(newChildEntry, -1);
			//
			setNodeType(newChildEntry, INTER_NODE);
			setTombstone(newChildEntry, -1);
			setNumOfEnt(newChildEntry, 1);
			setFreeSpaceOffset(newChildEntry,
					getSizeOfEntryInIntermediate(newChildEntry, Attribute.type));

		}
		else
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN);
		}

		//		//update second part of Page DIC
		//		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		//		setLeftSiblingPageNum(newLeafNode, LeafNodePN);
		//		setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		//		setNodeType(newLeafNode, LEAF_NODE);
		//		setTombstone(newLeafNode, -1);
		//		setNumOfEnt(newLeafNode, 1);
		//		setFreeSpaceOffset(newLeafNode,
		//				getSizeOfEntryInLeaf(newLeafNode, Attribute.type));
		//		setLeftMostChildPageNum(newLeafNode, -1);
		//
		//		//Write first part to existing leafNode and update Page DIC
		//		setRightSiblingPageNum(leafNode, newLeafNodePN);
		return 0;
	}



	if(newEntryNeeded)//create new entry and shift entries to new leaf node
	{
		char newEnt[WHOLE_SIZE_FOR_ENTRIES];
		char *newEntry = newEnt;
		char *newEntryStartAddr = newEntry;
		NumOfEnt numOfRIDs = 1;
		if(Attribute.type == TypeInt)
		{
			memcpy(newEntry, key, sizeof(int));//copying key
			newEntry += sizeof(int);
		}
		else if(Attribute.type == TypeReal)
		{
			memcpy(newEntry, key, sizeof(float));//copying key
			newEntry += sizeof(float);
		}
		else
		{
			int keyLength = *(int *)key;
			memcpy(newEntry, key, keyLength + 4);//copying key
			newEntry += keyLength + 4;
		}
		memcpy(newEntry , &numOfRIDs, sizeof(NumOfEnt));
		newEntry += sizeof(NumOfEnt);
		memcpy(newEntry, &(rid.pageNum), sizeof(PageNum));
		newEntry += sizeof(PageNum);
		memcpy(newEntry, &(rid.slotNum), sizeof(SlotOffset));
		newEntry += sizeof(SlotOffset);
		int newEntrySize = newEntry - newEntryStartAddr;

		int secondPartOffset = 0;
		int firstPartOffset = 0;
		int leafNodeOffset = 0;
		char fPart[WHOLE_SIZE_FOR_ENTRIES];
		char *firstPart = fPart;
		char *secondPart = (char *) newLeafNode;

		int entSize = 0;
		int numOfEntriesF = 0;
		bool inserted = false;
		while(leafNodeOffset < WHOLE_SIZE_FOR_ENTRIES/2)
		{
			numOfEntriesF++;
			if(leafNodeOffset == offset && !inserted)
			{
				memcpy(firstPart + firstPartOffset, newEntryStartAddr, newEntrySize);
				firstPartOffset += newEntrySize;
				inserted = true;
				continue;
			}
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			if(firstPartOffset + entSize > WHOLE_SIZE_FOR_ENTRIES)
			{
				numOfEntriesF--;
				break; //Nice and easy solution to special case where last entry to
				//this node is very big so that it causes overflow in the node and
				//writes to page dic.
			}
			//			if(firstPartOffset + entSize > WHOLE_SIZE_FOR_ENTRIES)
			//			{
			//				numOfEntriesF--;
			//				break;
			//			}
			memcpy(firstPart + firstPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			firstPartOffset += entSize;
			leafNodeOffset += entSize;
		}


		//Create newChildNode
		int numOfEntriesS = 0;
		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		bool secondHasNothing = false;

		if(leafNodeOffset != freeSpaceOffset)
		{
			if(LeafNodePN == 0)
			{
				if(offset == leafNodeOffset)
					putEntryInItermediate(newChildEntry, Attribute.type, key, newLeafNodePN + 1);
				else
					putEntryInItermediate(newChildEntry, Attribute.type,
							(((char *)leafNode) + leafNodeOffset), newLeafNodePN + 1);
			}
			else
			{
				if(offset == leafNodeOffset)
					putEntryInItermediate(newChildEntry, Attribute.type, key, newLeafNodePN);
				else
					putEntryInItermediate(newChildEntry, Attribute.type,
							(((char *)leafNode) + leafNodeOffset), newLeafNodePN);
			}
		}
		else
		{
			if(LeafNodePN == 0)
				putEntryInItermediate(newChildEntry, Attribute.type, key, newLeafNodePN + 1);
			else
				putEntryInItermediate(newChildEntry, Attribute.type, key, newLeafNodePN);
		}

		//Create new leaf node
		inserted = false;
		while(leafNodeOffset != freeSpaceOffset)
		{
			if(leafNodeOffset == offset && !inserted)
			{
				numOfEntriesS++;
				memcpy(secondPart + secondPartOffset, newEntryStartAddr, newEntrySize);
				secondPartOffset += newEntrySize;
				inserted = true;
				continue;
			}
			numOfEntriesS++;
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			memcpy(secondPart + secondPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			secondPartOffset += entSize;
			leafNodeOffset += entSize;
		}

		if(freeSpaceOffset == offset)
		{
			numOfEntriesS++;
			memcpy(secondPart + secondPartOffset, newEntryStartAddr, newEntrySize);
			secondPartOffset += newEntrySize;
		}

		//update second part of Page DIC
		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		if(LeafNodePN == 0)
		{
			setLeftSiblingPageNum(newLeafNode, newLeafNodePN);
			setParentPageNum(newLeafNode, 0);
		}
		else
		{
			setLeftSiblingPageNum(newLeafNode, LeafNodePN);
			setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		}
		setNodeType(newLeafNode, LEAF_NODE);
		setTombstone(newLeafNode, -1);
		setNumOfEnt(newLeafNode, (NumOfEnt)numOfEntriesS);
		setFreeSpaceOffset(newLeafNode,(SlotOffset) secondPartOffset);
		setLeftMostChildPageNum(newLeafNode, -1);

		//Write first part to existing leafNode and update Page DIC
		memcpy(leafNode, firstPart, firstPartOffset);
		if(LeafNodePN == 0)
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN + 1);
			setParentPageNum(leafNode, 0);

			setLeftMostChildPageNum(newChildEntry, newLeafNodePN);
			setRightSiblingPageNum(newChildEntry, -1);
			setLeftSiblingPageNum(newChildEntry, -1);
			setParentPageNum(newChildEntry, -1);
			//FIXME: should I set newChildEntry to NULL???
			setNodeType(newChildEntry, INTER_NODE);
			setTombstone(newChildEntry, -1);
			setNumOfEnt(newChildEntry, 1);
			setFreeSpaceOffset(newChildEntry,
					getSizeOfEntryInIntermediate(newChildEntry, Attribute.type));

		}
		else
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN);
		}
		setNumOfEnt(leafNode, (NumOfEnt)numOfEntriesF);
		setFreeSpaceOffset(leafNode,(SlotOffset) firstPartOffset);
	}
	else//newEntry is not needed but we need a new leaf node and we need to
		//add rid to ridlist and also shift some entries to new node
	{
		int secondPartOffset = 0;
		int firstPartOffset = 0;
		int leafNodeOffset = 0;
		char fPart[WHOLE_SIZE_FOR_ENTRIES];
		char *firstPart = fPart;
		char *secondPart = (char *) newLeafNode;
		int entSize = 0;
		int numOfEntriesF = 0;

		while(leafNodeOffset < WHOLE_SIZE_FOR_ENTRIES/2)
		{
			numOfEntriesF++;
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			if(firstPartOffset + entSize + sizeof(PageNum)
					+ sizeof(SlotOffset) > WHOLE_SIZE_FOR_ENTRIES)
			{
				numOfEntriesF--;
				break; //Nice and easy solution to special case where last entry to
				//this node is very big so that it causes overflow in the node and
				//writes to page dic.
			}

			memcpy(firstPart + firstPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			firstPartOffset += entSize;
			if(leafNodeOffset == offset)
			{
				setNumOfRIDsInLeaf(firstPart + firstPartOffset - entSize, Attribute.type,
						getNumOfRIDsInLeafEntry(firstPart+ firstPartOffset - entSize,Attribute.type) + 1);
				memcpy(firstPart + firstPartOffset, &(rid.pageNum), sizeof(PageNum));
				firstPartOffset += sizeof(PageNum);
				memcpy(firstPart + firstPartOffset, &(rid.slotNum), sizeof(SlotOffset));
				firstPartOffset += sizeof(SlotOffset);
				leafNodeOffset += entSize;
				continue;
			}
			leafNodeOffset += entSize;

		}


		//Create newChildNode
		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		if(LeafNodePN == 0)
			putEntryInItermediate(newChildEntry, Attribute.type,
					(((char *)leafNode) + leafNodeOffset), newLeafNodePN + 1);
		else
			putEntryInItermediate(newChildEntry, Attribute.type,
					(((char *)leafNode) + leafNodeOffset), newLeafNodePN);



		//		//Create newChildNode
		//		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		//		//		if(leafNodeOffset != freeSpaceOffset)
		//		//		{
		//		char *newCEntryBuff = (char *)newChildEntry;
		//
		//		if(Attribute.type == TypeInt)
		//		{
		//			setKeyOfEntry(newChildEntry,*(int *)(((char *)leafNode) + leafNodeOffset));
		//			memcpy(newCEntryBuff + sizeof(int), &newLeafNodePN, sizeof(PageNum));
		//		}
		//		else if(Attribute.type == TypeReal)
		//		{
		//			setKeyOfEntry(newChildEntry,*(float *)(((char *)leafNode) + leafNodeOffset));
		//			memcpy(newCEntryBuff + sizeof(float), &newLeafNodePN, sizeof(PageNum));
		//		}
		//		else
		//		{
		//			string k = extractVarChar(((char *)leafNode) + leafNodeOffset);
		//			int kLength = k.length();
		//			char *newCEntryBuff = (char *)newChildEntry;
		//			memcpy(newCEntryBuff, &kLength, 4);
		//			memcpy(newCEntryBuff + 4, k.c_str(), kLength);
		//			memcpy(newCEntryBuff + 4 + kLength, &newLeafNodePN, sizeof(PageNum));
		//		}
		//		}
		//		else
		//		{
		//			cout << "ERROR: else" << endl;
		//			return -1; //means that we did not split and probably it will cause an error.
		//		}
		//Create new leaf node
		int numOfEntriesS = 0;
		while(leafNodeOffset != freeSpaceOffset)
		{
			numOfEntriesS++;
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			memcpy(secondPart + secondPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			secondPartOffset += entSize;
			if(leafNodeOffset == offset)
			{
				setNumOfRIDsInLeaf(secondPart + secondPartOffset - entSize, Attribute.type,
						getNumOfRIDsInLeafEntry(secondPart + secondPartOffset - entSize, Attribute.type) + 1);
				memcpy(secondPart + secondPartOffset, &(rid.pageNum), sizeof(PageNum));
				secondPartOffset += sizeof(PageNum);
				memcpy(secondPart + secondPartOffset, &(rid.slotNum), sizeof(SlotOffset));
				secondPartOffset += sizeof(SlotOffset);
				leafNodeOffset += entSize;
				continue;
			}
			leafNodeOffset += entSize;
		}

		//update second part of Page DIC
		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		if(LeafNodePN == 0)
		{
			setLeftSiblingPageNum(newLeafNode, newLeafNodePN);
			setParentPageNum(newLeafNode, 0);
		}
		else
		{
			setLeftSiblingPageNum(newLeafNode, LeafNodePN);
			setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		}
		setNodeType(newLeafNode, LEAF_NODE);
		setTombstone(newLeafNode, -1);
		setNumOfEnt(newLeafNode, (NumOfEnt)numOfEntriesS);
		setFreeSpaceOffset(newLeafNode,(SlotOffset) secondPartOffset);
		setLeftMostChildPageNum(newLeafNode, -1);

		//Write first part to existing leafNode and update Page DIC
		memcpy(leafNode, firstPart, firstPartOffset);
		if(LeafNodePN == 0)
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN + 1);
			setParentPageNum(leafNode, 0);

			setLeftMostChildPageNum(newChildEntry, newLeafNodePN);
			setRightSiblingPageNum(newChildEntry, -1);
			setLeftSiblingPageNum(newChildEntry, -1);
			setParentPageNum(newChildEntry, -1);
			//FIXME: should I set newChildEntry to NULL???
			setNodeType(newChildEntry, INTER_NODE);
			setTombstone(newChildEntry, -1);
			setNumOfEnt(newChildEntry, 1);
			setFreeSpaceOffset(newChildEntry,
					getSizeOfEntryInIntermediate(newChildEntry, Attribute.type));

		}
		else
		{
			setRightSiblingPageNum(leafNode, newLeafNodePN);
		}
		setNumOfEnt(leafNode, (NumOfEnt)numOfEntriesF);
		setFreeSpaceOffset(leafNode,(SlotOffset) firstPartOffset);

	}
	//if we will add rid to existing entry check current size + PageNum + SlotOffset < total free space in a PAGE
	//Or simply check numofentries =? 1. HAha it is more smarter way.
	//Depending on the result: newLeafNode = overflow page or newLeafNode = leaf page

	//if newLeafNode == leaf page
	//

	return 0;
}



RC IndexManager::_deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
		PageNum currentNodePage, void *newChildNodeKey, PageNum &newChildNodePage)
{
	RC rc = -1;
	newChildNodeKey = NULL;
	newChildNodePage = -1;
	char pageToProcess[PAGE_SIZE];

	//it is changed when new child node is created in its child
	rc = ixfileHandle.fileHandle.readPage(currentNodePage,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	NodeType nodeType = getNodeType(pageToProcess);
	SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,key);

	char *entryToProcess = NULL;


	if (nodeType == INTER_NODE)
	{

		PageNum childNodePage = -1;
		//left most child pointer
		if(entryOffset == -1)
			childNodePage = getLeftMostChildPageNum(pageToProcess);
		else //general entry to point child
		{
			entryToProcess = pageToProcess + entryOffset;
			childNodePage = getChildOfIntermediateEntry(entryToProcess,attribute.type);
		}

		rc = _deleteEntry(ixfileHandle,attribute,key,rid,
				childNodePage,newChildNodeKey,newChildNodePage);

		if(newChildNodePage == -1)
		{
			newChildNodeKey = NULL;
			newChildNodePage = -1;
		}
		else //there was merge
		{
		}
	}

	else if(nodeType == LEAF_NODE)
	{

		PageNum tombstone = getTombstone(pageToProcess);
		SlotOffset offsetToDelete = 0;
		SlotOffset offsetToCompact = 0;
		bool sameKey = false;
		NumOfEnt numOfEnt = getNumOfEnt(pageToProcess);

		/*****************************************key matching*************************************/
		//-1 means first position
		if(entryOffset == -1 && tombstone == -1)
		{
			//No key match
			return -1;
		}
		else if (entryOffset == -1 && tombstone != -1)
		{
			rc = deleteEntryInOverflowPage(ixfileHandle,currentNodePage,pageToProcess,rid);
			return rc;
		}


		entryToProcess = pageToProcess + entryOffset;


		sameKey = compareKeys(key,EQ_OP,entryToProcess,attribute.type);

		if(!sameKey)
			//No key to match
			return -1;

		offsetToDelete = entryOffset;

		/****************************************rid Matching*************************************/
		int targetRidNum = -1;
		unsigned numOfRid = getNumOfRIDsInLeafEntry(entryToProcess,attribute.type);
		for (unsigned i = 0 ; i <  numOfRid; i++)
		{
			RID extractedRid;
			getRIDInLeaf(entryToProcess,attribute.type,i,extractedRid);
			if(rid.pageNum == extractedRid.pageNum && rid.slotNum == extractedRid.slotNum)
			{
				targetRidNum = i;
				break;
			}
		}

		//No rid here
		if(targetRidNum == -1)
		{
			//No tombstone
			if(tombstone == -1)
				return -1;//No matching rid
			rc = deleteEntryInOverflowPage(ixfileHandle,currentNodePage,pageToProcess,rid);
			return rc;
		}

		//There is matching rid here

		//Delete Entry


		unsigned entrySize = 0;
		if(numOfRid == 1)
		{
			entrySize = getSizeOfEntryInLeaf(entryToProcess,attribute.type);
			offsetToCompact = offsetToDelete + entrySize;
			moveEntries(pageToProcess,offsetToCompact,entrySize,MoveBackward); //This function includes adjust freespacesize;
			setNumOfEnt(pageToProcess,numOfEnt-1);
		}
		//Delete Rid in entry
		else
		{
			entrySize = sizeof(PageNum) + sizeof(SlotOffset);
			SlotOffset targetRidOffset = getRIDOffsetInLeaf(entryToProcess,attribute.type,targetRidNum);
			offsetToCompact = offsetToDelete + targetRidOffset + entrySize;
			moveEntries(pageToProcess,offsetToCompact,entrySize,MoveBackward);
			setNumOfRIDsInLeaf(entryToProcess,attribute.type,numOfRid - 1);
		}
		//write

		rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
		if(rc != 0)
			return rc;
	}
	return rc;
}



RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	fstream *fileStream = ixfileHandle.fileHandle.getFileStream();
	if(fileStream == NULL)
		return -1;

	RC rc;
	char newChildNodeKey[PAGE_SIZE];
	PageNum newChildNodePage = -1;

	rc = _deleteEntry(ixfileHandle,attribute,key,rid,0,newChildNodeKey,newChildNodePage);

	return rc;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void      *lowKey,
		const void      *highKey,
		bool			lowKeyInclusive,
		bool        	highKeyInclusive,
		IX_ScanIterator &ix_ScanIterator)
{
	fstream *fileStream = ixfileHandle.fileHandle.getFileStream();
	if(fileStream == NULL)
		return -1;

	RC rc = -1;


	ix_ScanIterator.tempPage = malloc(PAGE_SIZE); //tempPage;
	ix_ScanIterator.tempOverFlowPage = malloc(PAGE_SIZE); //tempOverFlowPage;
	void *pageToProcess = ix_ScanIterator.tempPage;
	ix_ScanIterator.fileHandle = &ixfileHandle.fileHandle;
	ix_ScanIterator.until = highKey;
	if(highKeyInclusive)
		ix_ScanIterator.op = LE_OP;
	else
		ix_ScanIterator.op = LT_OP;
	ix_ScanIterator.keyType = attribute.type;

	//Load root
	rc = ixfileHandle.fileHandle.readPage(0,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	//finding Leaf Node to process
	while(getNodeType(pageToProcess) == INTER_NODE)
	{
		SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,lowKey);
		PageNum childNode = -1;
		if(entryOffset == -1)
			childNode = getLeftMostChildPageNum(pageToProcess);
		else
		{
			char *entryToProcess = (char*)pageToProcess + entryOffset;
			childNode = getChildOfIntermediateEntry(entryToProcess,attribute.type);
		}
		rc = ixfileHandle.fileHandle.readPage(childNode,pageToProcess);//Read Page
		if(rc != 0)
			return rc;
	}
	//After while statement, target leaf is loaded in ix_ScanIterator.tempPage

	ix_ScanIterator.entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,lowKey);


	//-1 means first position
	if(ix_ScanIterator.entryOffset == -1)
		ix_ScanIterator.entryOffset = 0;

	char *entryToProcess = (char*)pageToProcess + ix_ScanIterator.entryOffset;


	ix_ScanIterator.currentSlot = 0;
	ix_ScanIterator.tombStone = getTombstone(pageToProcess);


	if(ix_ScanIterator.tombStone != -1)
	{
		//Load Overflow Page
		void *overflowPage = ix_ScanIterator.tempOverFlowPage;
		rc = ixfileHandle.fileHandle.readPage(ix_ScanIterator.tombStone,overflowPage);//Read Page
		if(rc != 0)
			return rc;
		ix_ScanIterator.numOfRidsInOverflow = getNumOfEnt(overflowPage);
		ix_ScanIterator.currentOverFlowSlot = 0;
		ix_ScanIterator.tombStoneInOverflow = getTombstone(overflowPage);
	}

	//Do not allow same key : skip
	if(lowKey != NULL && compareKeys(lowKey,EQ_OP, entryToProcess,attribute.type) && !lowKeyInclusive)
	{
		ix_ScanIterator.entryOffset = ix_ScanIterator.entryOffset  + getSizeOfEntryInLeaf(entryToProcess,attribute.type);
		ix_ScanIterator.tombStone = -1;//Also skip the overflow Page for this key since Overflow page only has one key
	}

	//cout << getFreeSpaceOffset(ix_ScanIterator.tempPage) << endl << flush;


	return 0;
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {

	char node[PAGE_SIZE];
	ixfileHandle.fileHandle.readPage(0, node);
	if(getNodeType(node) == LEAF_NODE)
	{
		_printLeafNode(ixfileHandle, attribute, 0, node, 0, true);
	}
	else
	{
		_printBtree(ixfileHandle, attribute, 0, node, 0, true);
	}
}


void IndexManager::tab(int numOfTabs) const
{
	for(int i = 0; i < numOfTabs; i++)
	{
		cout << "\t";
	}
}

void IndexManager::_printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute,
		PageNum pageNum, void *page1, int numOfTabs, bool last) const
{
	tab(numOfTabs);
	cout << "{";
	if(pageNum == 0)	cout << endl;
	cout << "\"keys\":[";

	char *node = (char *)page1;
	int offset = 0;
	int freeSpaceOffset = getFreeSpaceOffset(node);
	int entrySize = 0;
	while(offset != freeSpaceOffset)
	{
		entrySize = getSizeOfEntryInIntermediate(node + offset, attribute.type);
		cout << "\"";
		if(attribute.type == TypeInt)
		{
			int key = 0;
			getKeyOfEntry(node + offset, key);
			cout << key;
		}
		else if(attribute.type == TypeReal)
		{
			float key = 0;
			getKeyOfEntry(node + offset, key);
			cout << key;
		}
		else
		{
			string key;
			getKeyOfEntry(node + offset, key);
			cout << key;
		}
		cout << "\"";
		offset += entrySize;
		if(offset != freeSpaceOffset) cout << ",";
	}

	cout << "]," << endl;
	tab(numOfTabs);
	cout << "\"children\":[" << endl;

	offset = 0;
	bool lastChild = false;
	bool leftMostProcessed = false;
	entrySize = 0;
	while(offset != freeSpaceOffset || !leftMostProcessed)
	{
		int *page;
		int lmcp = 0;
		if(!leftMostProcessed)
		{
			lmcp = getLeftMostChildPageNum(node);
			page = &lmcp;
			leftMostProcessed = true;
		}
		else
		{
			entrySize = getSizeOfEntryInIntermediate(node + offset, attribute.type);
			page = (int *)(node + offset + entrySize - sizeof(PageNum));
		}

		char *childNode[PAGE_SIZE];
		ixfileHandle.fileHandle.readPage(*page, childNode);
		NodeType nodeType = getNodeType(childNode);

		if((offset + entrySize) == freeSpaceOffset)
		{
			lastChild = true;
		}

		if(nodeType == INTER_NODE)
		{
			_printBtree(ixfileHandle, attribute, *page, childNode, numOfTabs + 1, lastChild);
		}
		else if(nodeType == LEAF_NODE && getNumOfEnt(childNode) > 0)
		{
			_printLeafNode(ixfileHandle,attribute, *page, childNode, numOfTabs + 1, lastChild);
		}
		else
		{
			//cout << "ERRORRORORORORORROROOROROR" << endl;
		}
		offset += entrySize;
	}

	tab(numOfTabs);
	cout << "]";
	if(pageNum == 0)	cout << endl;
	cout << "}";
	if(!last) cout << ",";
	cout << endl;
}

void IndexManager::_printLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute,
		PageNum pageNum, void *page, int numOfTabs, bool last) const
{
	tab(numOfTabs);
	cout << "{";
	cout << "\"keys\":[";

	char *node = (char *)page;
	int offset = 0;
	int freeSpaceOffset = getFreeSpaceOffset(node);
	int entrySize = 0;

	while(offset != freeSpaceOffset)
	{
		entrySize = getSizeOfEntryInLeaf(node + offset, attribute.type);
		cout << "\"";
		if(attribute.type == TypeInt)
		{
			int key = 0;
			getKeyOfEntry(node + offset, key);
			cout << key << ":[";
			int ridOffset = sizeof(int) + sizeof(NumOfEnt);
			while(ridOffset != entrySize)
			{
				cout << "(";
				PageNum p = *(PageNum *)(node + offset + ridOffset);
				SlotOffset s = *(SlotOffset *)(node + offset + ridOffset + sizeof(PageNum));
				cout << p << "," << s << ")";
				ridOffset += sizeof(PageNum) + sizeof(SlotOffset);
				if(ridOffset != entrySize) cout << ",";
			}
		}
		else if(attribute.type == TypeReal)
		{
			float key = 0;
			getKeyOfEntry(node + offset, key);
			cout << key << ":[";
			int ridOffset = sizeof(float) + sizeof(NumOfEnt);
			while(ridOffset != entrySize)
			{
				cout << "(";
				PageNum p = *(PageNum *)(node + offset + ridOffset);
				SlotOffset s = *(SlotOffset *)(node + offset + ridOffset + sizeof(PageNum));
				cout << p << "," << s << ")";
				ridOffset += sizeof(PageNum) + sizeof(SlotOffset);
				if(ridOffset != entrySize) cout << ",";
			}
		}
		else
		{
			string key;
			getKeyOfEntry(node + offset, key);
			cout << key << ":[";
			int ridOffset = key.length() + sizeof(NumOfEnt) + sizeof(int);
			while(ridOffset != entrySize)
			{
				cout << "(";
				PageNum p = *(PageNum *)(node + offset + ridOffset);
				SlotOffset s = *(SlotOffset *)(node + offset + ridOffset + sizeof(PageNum));
				cout << p << "," << s << ")";
				ridOffset += sizeof(PageNum) + sizeof(SlotOffset);
				if(ridOffset != entrySize) cout << ",";
			}
		}

		PageNum overflowPageN = getTombstone(node);
		while(overflowPageN != -1)
		{
			cout << ",";
			char overflowPage[PAGE_SIZE];
			ixfileHandle.fileHandle.readPage(overflowPageN, overflowPage);
			int overflowOffset = 0;
			int oflowFreeSpaceOffs = getFreeSpaceOffset(overflowPage);
			while(overflowOffset != oflowFreeSpaceOffs)
			{
				cout << "(";
				PageNum p = *(PageNum *)(overflowPage + overflowOffset);
				SlotOffset s = *(SlotOffset *)(overflowPage + overflowOffset + sizeof(PageNum));
				cout << p << "," << s << ")";
				overflowOffset += sizeof(PageNum) + sizeof(SlotOffset);
				if(overflowOffset != oflowFreeSpaceOffs) cout << ",";
			}
			overflowPageN = getTombstone(overflowPage);
		}
		cout << "]";
		cout << "\"";
		offset += entrySize;
		if(offset != freeSpaceOffset) cout << ",";
	}
	cout<< "]}";
	if(!last) cout << ",";
	cout << endl;
}

IX_ScanIterator::IX_ScanIterator()
{
	indexManager = IndexManager::instance();
	op = EQ_OP;
	currentSlot = 0;
	fileHandle = NULL;
	tombStone = -1;
	entryOffset = 0;
	tombStoneInOverflow = -1;
	numOfRidsInOverflow = 0;
	keyType = TypeInt;
	until = NULL;
	currentOverFlowSlot = 0;
	tempPage = NULL;
	tempOverFlowPage = NULL;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::_getNextEntryFromOverflow(void *pageToProcess, RID &rid)
{
	RC rc = -1;

	//end of overflow and no other overflow
	if(currentOverFlowSlot  ==  numOfRidsInOverflow && tombStoneInOverflow == -1)
		return 1;//go ahead next entry!
	//end of rid and there is another overflow page : move to another overflow page
	else if(currentOverFlowSlot == numOfRidsInOverflow && tombStoneInOverflow != -1)
	{
		void *overflowPageToProcess = tempOverFlowPage;
		rc = fileHandle->readPage(tombStoneInOverflow,overflowPageToProcess);//Read Page
		if(rc != 0)
			return rc;
		numOfRidsInOverflow = indexManager->getNumOfEnt(overflowPageToProcess);
		tombStoneInOverflow = indexManager->getTombstone(overflowPageToProcess);
		currentOverFlowSlot = 0;
	}

	indexManager->getRIDInOverFlowPage(pageToProcess,currentOverFlowSlot,rid);

	currentOverFlowSlot++;

	return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	RC rc = -1;
	void *pageToProcess = tempPage;
	void *overflowPageToProcess = tempOverFlowPage;

	SlotOffset freeSpaceOffset = indexManager->getFreeSpaceOffset(pageToProcess);
	void *entryToProcess = (char*)pageToProcess + entryOffset;

	//approach to the end of the node: next Node
	if(entryOffset == freeSpaceOffset)
	{
		//------------------overflow page dealing

		if(tombStone != -1)
		{
			rc = _getNextEntryFromOverflow(overflowPageToProcess,rid);
			if(rc == 0)
			{
				indexManager->copyKeyOfEntry(key,pageToProcess,keyType);
				return rc;
			}
		}
		//-------------------------------------------------------------------------

		bool noEntry = true;
		while(noEntry)
		{
			PageNum nextNodePage = indexManager->getRightSiblingPageNum(pageToProcess);

			//end Of leaf Node
			if(nextNodePage == -1)
				return IX_EOF;
			rc = fileHandle->readPage(nextNodePage,pageToProcess);//Read Page
			if(rc != 0)
				return rc;

			unsigned numOfEntry = indexManager->getNumOfEnt(pageToProcess);
			tombStone = indexManager->getTombstone(pageToProcess);
			entryOffset = 0;
			currentSlot = 0;
			entryToProcess = pageToProcess;

			if(tombStone != -1)
			{
				//Load Overflow Page
				rc = fileHandle->readPage(tombStone,overflowPageToProcess);//Read Page
				if(rc != 0)
					return rc;
				numOfRidsInOverflow = indexManager->getNumOfEnt(overflowPageToProcess);
				tombStoneInOverflow = indexManager->getTombstone(overflowPageToProcess);
				currentOverFlowSlot = 0;
			}


		    if(numOfEntry == 0)
		    {
				if(tombStone != -1)
				{
					rc = _getNextEntryFromOverflow(overflowPageToProcess,rid);
					if(rc == 0)
					{
						indexManager->copyKeyOfEntry(key,pageToProcess,keyType);
						return rc;
					}
				}

		    	continue;
		    }
		    else
		    	noEntry = false;

		}
		//-------------------------------------------------------------------------
	}


	if(until != NULL && !indexManager->compareKeys(entryToProcess,op,until,keyType))
		return IX_EOF;

	NumOfEnt numOfRids = indexManager->getNumOfRIDsInLeafEntry(entryToProcess,keyType);
	indexManager->copyKeyOfEntry(key,entryToProcess,keyType);
	indexManager->getRIDInLeaf(entryToProcess, keyType,currentSlot, rid);
	currentSlot++;

	//end of rid: next entry
	if(currentSlot == numOfRids)
	{
		entryOffset = entryOffset + indexManager->getSizeOfEntryInLeaf(entryToProcess,keyType);
		currentSlot = 0;
	}
	return 0;
}

RC IX_ScanIterator::close()
{


	indexManager = NULL;
	op = EQ_OP;
	currentSlot = 0;
	fileHandle = NULL;
	tombStone = -1;
	entryOffset = 0;
	tombStoneInOverflow = -1;
	numOfRidsInOverflow = 0;
	keyType = TypeInt;
	until = NULL;
	currentOverFlowSlot = 0;
	free(tempPage);
	free(tempOverFlowPage);
	return 0;
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


