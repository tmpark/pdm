
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

unsigned IndexManager::getFreeSpaceSize(void* pageToProcess)
{
	SlotOffset freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	return PAGE_SIZE - (PAGE_DIC_SIZE + freeSpaceOffset);
}

unsigned IndexManager::getFreeSpaceSizeForOverflowPage(void* pageToProcess)
{
	SlotOffset freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	return PAGE_SIZE - (OVERFLOW_PAGE_DIC_SIZE + freeSpaceOffset);
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


RC IndexManager::getRIDInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid)
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


RC IndexManager::setRIDInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid)
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



unsigned IndexManager::calNewLeafEntrySize(const void* key, AttrType keyType)
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

unsigned IndexManager::calNewInterEntrySize(const void* key, AttrType keyType)
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

bool IndexManager::hasSameKey(const void *key, const void *entryToProcess,  AttrType keyType)
{
	if (keyType == TypeInt)
	{
		int value1;
		int value2;
		getKeyOfEntry(key,value1);
		getKeyOfEntry(entryToProcess,value2);
		return (value1 == value2);
	}
	else if (keyType == TypeReal)
	{
		float value1;
		float value2;
		getKeyOfEntry(key,value1);
		getKeyOfEntry(entryToProcess,value2);
		return (value1 == value2);
	}
	else if (keyType == TypeVarChar)
	{
		string value1;
		string value2;
		getKeyOfEntry(key,value1);
		getKeyOfEntry(entryToProcess,value2);
		return (value1 == value2);
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

RC IndexManager::pushEntries(void *pageToProcess,SlotOffset from,unsigned amountToMove)
{
	unsigned freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	unsigned amountOfData = freeSpaceOffset - from;
	char buf[amountOfData];
	memcpy(buf,(char*)pageToProcess+from,amountOfData);
	memcpy((char*)pageToProcess+from+amountToMove,buf,amountOfData);
	setFreeSpaceOffset(pageToProcess,freeSpaceOffset + amountToMove);
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

	char tempPage[PAGE_SIZE];

	//Root Page
	setFreeSpaceOffset(tempPage, 0);
	setNumOfEnt(tempPage, 0);
	setTombstone(tempPage, -1);
	setNodeType(tempPage, INTER_NODE);
	setParentPageNum(tempPage, -1);
	setLeftSiblingPageNum(tempPage, -1);
	setRightSiblingPageNum(tempPage, -1);
	setLeftMostChildPageNum(tempPage, 1);

	success = ixfileHandle.fileHandle.appendPage(tempPage);
	if(success != 0)
		return success;

	setNodeType(tempPage, LEAF_NODE);
	setParentPageNum(tempPage, 0);
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
		memcpy(entryToProcess, key, sizeof(int) + sizeOfVarChar);
		memcpy((char*)entryToProcess + sizeof(int) + sizeOfVarChar,(char*)&pageNum,sizeof(PageNum));
	}
	return 0;

}

RC IndexManager::putEntryInLeaf(void *entryToProcess, AttrType attrType, const void *key, RID rid, bool existing)
{

	if(existing)
	{
		NumOfEnt numOfEntry = getNumOfRIDsInLeaf(entryToProcess,attrType);
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
			memcpy(entryToProcess, key, sizeof(int) + sizeOfVarChar);
			setRIDInLeaf(entryToProcess,attrType,0,rid);
			setNumOfRIDsInLeaf(entryToProcess,attrType,1);
		}
	}
	return 0;

}

RC IndexManager::insertEntryInOverflowPage(IXFileHandle &ixfileHandle,void *pageToProcess,const RID &rid)
{
	RC rc = -1;
	PageNum tombstone = getTombstone(pageToProcess);

	//go to another overflow page
	if(tombstone != -1)
	{
		char overFlowPageToProcess[PAGE_SIZE];
		//it is changed when new child node is created in its child
		rc = ixfileHandle.fileHandle.readPage(tombstone,overFlowPageToProcess);//Read Page
		if(rc != 0)
			return rc;
		insertEntryInOverflowPage(ixfileHandle,overFlowPageToProcess,rid);
	}
	//insert here
	else
	{
		unsigned entrySize = sizeof(PageNum) + sizeof(SlotOffset);
		unsigned freeSpace = getFreeSpaceSizeForOverflowPage(pageToProcess);
		if(freeSpace >= entrySize)
		{

			PageNum pageNum = rid.pageNum;
			SlotOffset slotNum = rid.slotNum;
			unsigned numOfEnt = getNumOfEnt(pageToProcess)*entrySize;
			unsigned freeSpaceOffset = getFreeSpaceOffset(pageToProcess);

			char *insertPtr = (char*)pageToProcess + numOfEnt;
			*((PageNum*)insertPtr) = pageNum;
			insertPtr = insertPtr + sizeof(PageNum);
			*((SlotOffset*)insertPtr) = slotNum;

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

			rc = insertEntryInOverflowPage(ixfileHandle, overFlowPageToProcess, rid);

			PageNum anotherOverFlowPage = ixfileHandle.fileHandle.getNumberOfPages();
			rc = ixfileHandle.fileHandle.appendPage(overFlowPageToProcess);
			if(rc != 0)
				return rc;
			rc =setTombstone(pageToProcess,anotherOverFlowPage);
		}

	}
	return 0;

}

RC IndexManager::_insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
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
			if(freeSpaceSize >= entrySize)
			{
				SlotOffset offsetToInsert = 0;

				//-1 means first position
				if(entryOffset == -1)
					offsetToInsert = 0;
				//position to insert should be next to the found entry
				else
				{
					char *entryTemp = pageToProcess + entryOffset;
					offsetToInsert = entryOffset + getSizeOfEntryInLeaf(entryTemp,attribute.type);
				}
				//push as much as entrySize
				rc = pushEntries(pageToProcess,offsetToInsert,entrySize);
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
			else
			{
				char newChildPageToProcess[PAGE_SIZE];

				//split(get child node key)


				newChildNodePage = ixfileHandle.fileHandle.getNumberOfPages();
				//WritePage(Current & newPage)
				rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
				if(rc != 0)
					return rc;
				rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
				if(rc != 0)
					return rc;
			}
		}

	}
	else if(nodeType == LEAF_NODE)
	{
		SlotOffset offsetToInsert = 0;

		//-1 means first position
		if(entryOffset == -1)
			offsetToInsert = 0;
		//position to insert should be next to the found entry
		else
		{
			char *entryTemp = pageToProcess + entryOffset;
			offsetToInsert = entryOffset + getSizeOfEntryInLeaf(entryTemp,attribute.type);
		}

		entryToProcess = pageToProcess + offsetToInsert;


		PageNum tombstone = getTombstone(pageToProcess);

		//Overflowed Node: go inside overflow page and return null
		if(tombstone != -1)
		{
			rc = insertEntryInOverflowPage(ixfileHandle,pageToProcess,rid);
			newChildNodeKey = NULL;
			newChildNodePage = -1;
			return 0;
		}

		unsigned freeSpaceSize = getFreeSpaceSize(pageToProcess);
		bool sameKey = hasSameKey(key,entryToProcess,attribute.type);
		unsigned entrySize = 0;

		//Entry size is different in terms of new or not
		if(sameKey)
			entrySize = sizeof(PageNum) + sizeof(SlotOffset);
		else
			entrySize = calNewLeafEntrySize(key, attribute.type);


		if(freeSpaceSize >= entrySize)
		{

			//push as much as entrySize (Push check whether there needs push)
			rc = pushEntries(pageToProcess,offsetToInsert,entrySize);

			//insert(check whether it is same key or not)
			if(sameKey)
			{
				entryToProcess = pageToProcess + entryOffset;//if same, insert in the found entry; if not, insert in the next to the found entry
			}

			rc = putEntryInLeaf(entryToProcess,attribute.type,key,rid,sameKey);

			if(!sameKey)//
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
			rc = insertEntryInOverflowPage(ixfileHandle, pageToProcess, rid);
			newChildNodeKey = NULL;
			newChildNodePage = -1;
		}
		//split
		else
		{
			char newChildPageToProcess[PAGE_SIZE];

			//splitLeaf(get child node key)


			newChildNodePage = ixfileHandle.fileHandle.getNumberOfPages();
			//WritePage(Current & newPage)
			rc = ixfileHandle.fileHandle.writePage(currentNodePage,pageToProcess);//Write Page
			if(rc != 0)
				return rc;
			rc = ixfileHandle.fileHandle.appendPage(newChildPageToProcess);
			if(rc != 0)
				return rc;
		}
	}

	return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

	RC rc;
	void *newChildNodeKey = NULL;
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
	while(interNodeOffset < WHOLE_SIZE_FOR_ENTRIES/2)
	{
		numOfEntriesF++;
		if(interNodeOffset == offset)
		{
			memcpy(firstPart + firstPartOffset, entry, newEntrySize);
			firstPartOffset += newEntrySize;
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
	if(interNodeOffset != freeSpaceOffset)
	{
		char *newCEntryBuff = (char *)newChildEntry;

		if(entryType == TypeInt)
		{
			setKeyOfEntry(newChildEntry,*(int *)(((char *)interNode) + interNodeOffset));
			memcpy(newCEntryBuff + sizeof(int), &newInterNodePN, sizeof(PageNum));
			//setLeftChild
			int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(int));
			setLeftMostChildPageNum(secondPart, *pageNum);
			newChildEntrySize = sizeof(int) + sizeof(PageNum);
		}
		else if(entryType == TypeReal)
		{
			setKeyOfEntry(newChildEntry,*(float *)(((char *)interNode) + interNodeOffset));
			memcpy(newCEntryBuff + sizeof(float), &newInterNodePN, sizeof(PageNum));
			int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(float));
			setLeftMostChildPageNum(secondPart, *pageNum);
			newChildEntrySize = sizeof(float) + sizeof(PageNum);
		}
		else
		{
			string k = extractVarChar(((char *)interNode) + interNodeOffset);
			int kLength = k.length();
			char *newCEntryBuff = (char *)newChildEntry;
			memcpy(newCEntryBuff, &kLength, 4);
			memcpy(newCEntryBuff + 4, &k, kLength);
			memcpy(newCEntryBuff + 4 + kLength, &newInterNodePN, sizeof(PageNum));
			int *pageNum = (int *)(((char *)interNode) + interNodeOffset + sizeof(int) + kLength);
			newChildEntrySize = sizeof(int) + kLength + sizeof(PageNum);
			setLeftMostChildPageNum(secondPart, *pageNum);
		}
	}

	//Create new inter node
	while(interNodeOffset != freeSpaceOffset)
	{
		numOfEntriesS++;
		if(interNodeOffset == offset)
		{
			memcpy(secondPart + secondPartOffset, entry, newEntrySize);
			secondPartOffset += newEntrySize;
			continue;
		}
		entSize = getSizeOfEntryInIntermediate(((char *)interNode) + interNodeOffset, entryType);
		memcpy(secondPart + secondPartOffset, ((char *)interNode) + interNodeOffset, entSize);
		secondPartOffset += entSize;
		interNodeOffset += entSize;
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


	if(newRootNode == NULL)
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
		int offset, const Attribute &Attribute, const void *key, const RID &rid)
{

	//check whether we will add rid to existing entry or add new entry to node
	bool newEntryNeeded = true;


	char* entryToProcess = ((char *)leafNode) + offset;
	if(Attribute.type == TypeInt)
	{
		int value;
		RC rc = getKeyOfEntry(entryToProcess,value);
		if(value == *(int *)key)
		{
			newEntryNeeded = false;
		}
	}
	if(Attribute.type == TypeReal)
	{
		float value;
		RC rc = getKeyOfEntry(entryToProcess,value);
		if(value == *(float *)key)
		{
			newEntryNeeded = false;
		}
	}
	if(Attribute.type == TypeVarChar)
	{
		string value;
		RC rc = getKeyOfEntry(entryToProcess,value);
		string s;
		RC rcs = getKeyOfEntry(key,s);
		if(value == s)
		{
			newEntryNeeded = false;
		}
	}

	if(newEntryNeeded)//create new entry and shift entries to new leaf node
	{
		char newEnt[400];
		char *newEntry = newEnt;
		char *newEntryStartAddr = newEntry;
		int keyLength = *(int *)key;
		memcpy(newEntry, key, keyLength + 4);//copying key
		NumOfEnt numOfRIDs = 1;
		newEntry += keyLength + 4;
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
		while(leafNodeOffset < WHOLE_SIZE_FOR_ENTRIES/2)
		{
			numOfEntriesF++;
			if(leafNodeOffset == offset)
			{
				memcpy(firstPart + firstPartOffset, newEntryStartAddr, newEntrySize);
				firstPartOffset += newEntrySize;
				continue;
			}
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			if(leafNodeOffset + entSize > WHOLE_SIZE_FOR_ENTRIES)
			{
				break; //Nice and easy solution to special case where last entry to
				//this node is very big so that it causes overflow in the node and
				//writes to page dic.
			}
			memcpy(firstPart + firstPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			firstPartOffset += entSize;
			leafNodeOffset += entSize;
		}

		//Create newChildNode
		int numOfEntriesS = 0;
		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		if(leafNodeOffset != freeSpaceOffset)
		{
			char *newCEntryBuff = (char *)newChildEntry;

			if(Attribute.type == TypeInt)
			{
				setKeyOfEntry(newChildEntry,*(int *)(((char *)leafNode) + leafNodeOffset));
				memcpy(newCEntryBuff + sizeof(int), &newLeafNodePN, sizeof(PageNum));
			}
			else if(Attribute.type == TypeReal)
			{
				setKeyOfEntry(newChildEntry,*(float *)(((char *)leafNode) + leafNodeOffset));
				memcpy(newCEntryBuff + sizeof(float), &newLeafNodePN, sizeof(PageNum));
			}
			else
			{
				string k = extractVarChar(((char *)leafNode) + leafNodeOffset);
				int kLength = k.length();
				char *newCEntryBuff = (char *)newChildEntry;
				memcpy(newCEntryBuff, &kLength, 4);
				memcpy(newCEntryBuff + 4, &k, kLength);
				memcpy(newCEntryBuff + 4 + kLength, &newLeafNodePN, sizeof(PageNum));
			}
		}
		else
		{
			return -1; //means that we did not split and probably it will cause an error.
		}
		//Create new leaf node
		while(leafNodeOffset != freeSpaceOffset)
		{
			numOfEntriesS++;
			if(leafNodeOffset == offset)
			{
				memcpy(secondPart + secondPartOffset, newEntryStartAddr, newEntrySize);
				secondPartOffset += newEntrySize;
				continue;
			}
			entSize = getSizeOfEntryInLeaf(((char *)leafNode) + leafNodeOffset, Attribute.type);
			memcpy(secondPart + secondPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			secondPartOffset += entSize;
			leafNodeOffset += entSize;
		}

		//update second part of Page DIC
		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		setLeftSiblingPageNum(newLeafNode, LeafNodePN);
		setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		setNodeType(newLeafNode, LEAF_NODE);
		setTombstone(newLeafNode, -1);
		setNumOfEnt(newLeafNode, (NumOfEnt)numOfEntriesS);
		setFreeSpaceOffset(newLeafNode,(SlotOffset) secondPartOffset);

		//Write first part to existing leafNode and update Page DIC
		memcpy(leafNode, firstPart, firstPartOffset);
		setRightSiblingPageNum(leafNode, newLeafNodePN);
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
			if(leafNodeOffset + entSize + sizeof(PageNum)
					+ sizeof(SlotOffset) > WHOLE_SIZE_FOR_ENTRIES)
			{
				break; //Nice and easy solution to special case where last entry to
				//this node is very big so that it causes overflow in the node and
				//writes to page dic.
			}

			memcpy(firstPart + firstPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			firstPartOffset += entSize;
			if(leafNodeOffset == offset)
			{
				memcpy(firstPart + firstPartOffset, &(rid.pageNum), sizeof(PageNum));
				firstPartOffset += sizeof(PageNum);
				memcpy(firstPart + firstPartOffset, &(rid.slotNum), sizeof(SlotOffset));
				firstPartOffset += sizeof(SlotOffset);
				continue;
			}
			leafNodeOffset += entSize;

		}

		//Create newChildNode
		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		if(leafNodeOffset != freeSpaceOffset)
		{
			char *newCEntryBuff = (char *)newChildEntry;

			if(Attribute.type == TypeInt)
			{
				setKeyOfEntry(newChildEntry,*(int *)(((char *)leafNode) + leafNodeOffset));
				memcpy(newCEntryBuff + sizeof(int), &newLeafNodePN, sizeof(PageNum));
			}
			else if(Attribute.type == TypeReal)
			{
				setKeyOfEntry(newChildEntry,*(float *)(((char *)leafNode) + leafNodeOffset));
				memcpy(newCEntryBuff + sizeof(float), &newLeafNodePN, sizeof(PageNum));
			}
			else
			{
				string k = extractVarChar(((char *)leafNode) + leafNodeOffset);
				int kLength = k.length();
				char *newCEntryBuff = (char *)newChildEntry;
				memcpy(newCEntryBuff, &kLength, 4);
				memcpy(newCEntryBuff + 4, &k, kLength);
				memcpy(newCEntryBuff + 4 + kLength, &newLeafNodePN, sizeof(PageNum));
			}
		}
		else
		{
			return -1; //means that we did not split and probably it will cause an error.
		}
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
				memcpy(firstPart + secondPartOffset, &(rid.pageNum), sizeof(PageNum));
				secondPartOffset += sizeof(PageNum);
				memcpy(firstPart + secondPartOffset, &(rid.slotNum), sizeof(SlotOffset));
				secondPartOffset += sizeof(SlotOffset);
				continue;
			}
			leafNodeOffset += entSize;
		}

		//update second part of Page DIC
		setRightSiblingPageNum(newLeafNode, getRightSiblingPageNum(leafNode));
		setLeftSiblingPageNum(newLeafNode, LeafNodePN);
		setParentPageNum(newLeafNode, getParentPageNum(leafNode));
		setNodeType(newLeafNode, LEAF_NODE);
		setTombstone(newLeafNode, -1);
		setNumOfEnt(newLeafNode, (NumOfEnt)numOfEntriesS);
		setFreeSpaceOffset(newLeafNode,(SlotOffset) secondPartOffset);

		//Write first part to existing leafNode and update Page DIC
		memcpy(leafNode, firstPart, firstPartOffset);
		setRightSiblingPageNum(leafNode, newLeafNodePN);
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
	RC rc = -1;
	void *pageToProcess = ix_ScanIterator.tempPage;
	ix_ScanIterator.fileHandle = &ixfileHandle.fileHandle;
	ix_ScanIterator.until = highKey;
	ix_ScanIterator.untilInclusive = highKeyInclusive;
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
			char *entryToProcess = pageToProcess + entryOffset;
			childNode = getChildOfIntermediateEntry(entryToProcess,attribute.type);
		}
		rc = ixfileHandle.fileHandle.readPage(childNode,pageToProcess);//Read Page
		if(rc != 0)
			return rc;
	}
	//After while statement, target leaf is loaded in ix_ScanIterator.tempPage

	SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,lowKey);

	//-1 means first position
	if(entryOffset == -1)
		entryOffset = 0;

	char *entryToProcess = pageToProcess + entryOffset;

	//Do not allow same key : skip
	if(hasSameKey(lowKey, entryToProcess,attribute.type) && !lowKeyInclusive)
	{
		entryOffset = entryOffset  + getSizeOfEntryInLeaf(entryToProcess,attribute.type);
	}

	ix_ScanIterator.entryOffset = entryOffset;

	return 0;
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
	indexManager = IndexManager::instance();
	currentSlot = 0;
	currentOverFlowSlot = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	RC rc = -1;
	void *pageToProcess = tempPage;
	SlotOffset freeSpaceOffset = indexManager->getFreeSpaceOffset(pageToProcess);

	//approch to the end of the node: next Node
	if(entryOffset == freeSpaceOffset)
	{
		PageNum nextNodePage = indexManager->getRightSiblingPageNum(pageToProcess);

		//end Of leaf Node
		if(nextNodePage == -1)
			return IX_EOF;
		rc = fileHandle->readPage(nextNodePage,pageToProcess);//Read Page
		if(rc != 0)
			return rc;
		entryOffset = 0;
	}



	void *entryToProcess = pageToProcess + entryOffset;


	//Extract key value
	if(keyType == TypeInt)
	{
		int value;
		indexManager->getKeyOfEntry(entryToProcess,value);
		indexManager->setKeyOfEntry(key,value);

	}
	else if(keyType == TypeReal)
	{
		float value;
		indexManager->getKeyOfEntry(entryToProcess,value);
		indexManager->setKeyOfEntry(key,value);
	}
	else if(keyType == TypeVarChar)
	{
		string value;
		indexManager->getKeyOfEntry(entryToProcess,value);
		indexManager->setKeyOfEntry(key,value);
	}

	//searching for a rid in leaf entry
    if(currentSlot < indexManager->getNumOfRIDsInLeaf(entryToProcess,keyType))
    {
    	indexManager->getRIDInLeaf(entryToProcess, keyType,currentSlot, rid);
    	currentSlot++;
    	return 0;
    }



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


