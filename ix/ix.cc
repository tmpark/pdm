
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
	RC rc = -1;
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
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
	        int objectiveValue = *((int*)key);
	        if (candidateValue <= objectiveValue)
	        	candidateEntryOffset = currentEntryOffset;

		}
		else if (attrType == TypeReal)
		{
			float candidateValue;
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
	        float objectiveValue = *((float*)key);
	        if (candidateValue <= objectiveValue)
	        	candidateEntryOffset = currentEntryOffset;
		}
		else if (attrType == TypeVarChar)
		{
			string candidateValue;
			rc = getKeyOfEntry((const void*)entryToProcess, candidateValue);
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

RC IndexManager::_insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
		PageNum &currentNodePage, void *newChildNodeKey, PageNum &newChildNodePage)
{
	RC rc = -1;
	char tempPage0[PAGE_SIZE];
	void *pageToProcess = tempPage0;

    rc = ixfileHandle.fileHandle.readPage(currentNodePage,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	NodeType nodeType = getNodeType(pageToProcess);
	SlotOffset entryOffset = findEntryOffsetToProcess(pageToProcess,attribute.type,key);
	char *entryToProcess = NULL;


    if (nodeType == INTER_NODE)
    {
    	PageNum newChildPage = -1;
    	PageNum childNodePage = -1;
    	//left most child pointer
    	if(entryOffset == -1)
    	{
    		childNodePage = getLeftMostChildPageNum(pageToProcess);
    		newChildPage = _insertEntry(ixfileHandle,attribute,key,rid,
    				childNodePage,newChildNodeKey,newChildNodePage);
    	}
    	else //general entry to point child
    	{
    		entryToProcess = pageToProcess + entryOffset;
    		childNodePage = getChildOfIntermediateEntry(entryToProcess,attribute.type);
    		newChildPage =_insertEntry(ixfileHandle,attribute,key,rid,
    				childNodePage,newChildNodeKey,newChildNodePage);
    	}
//Fixme
    	if(newChildPage == -1)
    		return -1;
    	else //child was splited
    	{
    		//unsigned entrySize = calNewInterEntrySize()


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

    	//Overflowed Node: go inside overflow page and return null




    	unsigned freeSpaceSize = getFreeSpaceSize(pageToProcess);

    	//key is the highest value
    	if(offsetToInsert == getFreeSpaceOffset(pageToProcess))
    	{
    		//there is enough space
    		if(freeSpaceSize > calNewLeafEntrySize(key, attribute.type))
    		{
    			//insert
    			//return -1
    		}
    		else
    		{
    			//split
    			//return new node;
    		}

    	}

    	//new key inserted
    	if(!hasSameKey(key,entryToProcess,attribute.type))
    	{
    		//there is enough space
    		unsigned entrySize = calNewLeafEntrySize(key, attribute.type);
    		if(freeSpaceSize > entrySize)
    		{
    			//push as much as entrySize
    			//insert
    			//return -1
    		}
    		else
    		{
    			//split
    			//return new node;
    		}

    	}

    	//same key
    	unsigned entrySize = sizeof(PageNum) + sizeof(SlotOffset);

		//there is enough space
		if(freeSpaceSize > entrySize)
		{
			//push as much as entrySize
			//insert
			//return -1
		}
		else
		{
			//split
			//return new node;
		}

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
			memcpy(firstPart + firstPartOffset, ((char *)leafNode) + leafNodeOffset, entSize);
			firstPartOffset += entSize;
			leafNodeOffset += entSize;
		}

		//Create newChildNode
		int numOfEntriesS = 0;
		int freeSpaceOffset = getFreeSpaceOffset(leafNode);
		if(leafNodeOffset != freeSpaceOffset)
		{
			string k = extractVarChar(((char *)leafNode) + leafNodeOffset);
			int kLength = k.length();
			char *newCEntryBuff = (char *) malloc(kLength + 4 + sizeof(PageNum));
			memcpy(newCEntryBuff, &kLength, 4);
			memcpy(newCEntryBuff + 4, &k, kLength);
			memcpy(newCEntryBuff + 4 + kLength, &newLeafNodePN, sizeof(PageNum));
			newChildEntry = newCEntryBuff;
			//FIXME: I created a space to set pageNum but I didn`t set it.
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
			string k = extractVarChar(((char *)leafNode) + leafNodeOffset);
			int kLength = k.length();
			char *newCEntryBuff = (char *) malloc(kLength + 4 + sizeof(PageNum));
			memcpy(newCEntryBuff, &kLength, 4);
			memcpy(newCEntryBuff + 4, &k, kLength);
			memcpy(newCEntryBuff + 4 + kLength, &newLeafNodePN, sizeof(PageNum));
			newChildEntry = newCEntryBuff;
			//FIXME: I created a space to set pageNum but I didn`t set it.
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


