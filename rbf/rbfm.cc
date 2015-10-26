#include "rbfm.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library
#include <cstdlib> //added library
#include <cstring> //added library
#include <cstdio> //added library


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();


    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	directInsert = true;
	tempPage = malloc(PAGE_SIZE);
	tempRecord = malloc(PAGE_SIZE);
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	free(tempPage);
	free(tempRecord);
}

RC RecordBasedFileManager::createFile(const string &fileName) {

	RC success = PagedFileManager::instance()->createFile(fileName);
    return success;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {

	RC success = PagedFileManager::instance()->destroyFile(fileName);
    return success;

}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {

	RC success = PagedFileManager::instance()->openFile(fileName,fileHandle);
    return success;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {

	RC success = PagedFileManager::instance()->closeFile(fileHandle);
    return success;
}

RecordDic RecordBasedFileManager::getRecordFieldSize(const void *recordToProcess, unsigned fieldNum)
{
	RecordDic currentFieldOffset = getRecordFieldOffset(recordToProcess,fieldNum);//*((RecordDic*)currentDicSlotPtr);
	RecordDic nextFieldOffset = getRecordFieldOffset(recordToProcess,fieldNum+1);//*((RecordDic*)nextDicSlotPtr);


	if(currentFieldOffset >= 0)
	{
		return abs(nextFieldOffset) - currentFieldOffset;
	}
	else
	{
		return -1;
	}

}

RecordDic RecordBasedFileManager::getRecordFieldOffset(const void *recordToProcess, unsigned fieldNum)
{
	char *interiorRecordDicSlot = (char*)recordToProcess + (1+1)*sizeof(RecordDic); //Tomb + NumOfRecs
	char *DicSlotPtr = interiorRecordDicSlot + fieldNum*sizeof(RecordDic);
	return *((RecordDic*)DicSlotPtr);
}

RecordDic RecordBasedFileManager::getNumOfRecord(void *recordToProcess)
{
	char *interiorRecordDicSlot = (char*)recordToProcess + 1*sizeof(RecordDic);  //Tomb
	return *((RecordDic*)interiorRecordDicSlot);
}

RecordDic RecordBasedFileManager::getTombstone(void *recordToProcess)
{
	char *interiorRecordDicSlot = (char*)recordToProcess;
	return *((RecordDic*)interiorRecordDicSlot);
}

RC RecordBasedFileManager::setRecordFieldOffset(void *recordToProcess, unsigned fieldNum, RecordDic offset)
{
	char *interiorRecordDicSlot = (char*)recordToProcess + (1+1)*sizeof(RecordDic); //Tomb +NumOfRecs
	char *DicSlotPtr = interiorRecordDicSlot + fieldNum*sizeof(RecordDic);
	*((RecordDic*)DicSlotPtr) = offset;
	return 0;
}

RC RecordBasedFileManager::setNumOfRecord(void *recordToProcess, RecordDic numOfRecordFields)
{
	char *interiorRecordDicSlot = (char*)recordToProcess + 1*sizeof(RecordDic); //Tomb
	*((RecordDic*)interiorRecordDicSlot) = numOfRecordFields;
	return 0;
}

RC RecordBasedFileManager::setTombstone(void *recordToProcess, RecordDic type)
{
	char *interiorRecordDicSlot = (char*)recordToProcess;
	*((RecordDic*)interiorRecordDicSlot) = type;
	return 0;
}


RC RecordBasedFileManager::transToExteriorRecord(const vector<Attribute> &recordDescriptor,const void *interiorRecord, void *exteriorRecord)
{

	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	memset(nullsIndicator, 0, numberOfBytesForNullIndicator);

	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;
	int exteriorRecordFieldOffset = 0;

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;

		//RecordDic currentFieldOffset = getRecordFieldOffset(interiorRecord,i);//*((RecordDic*)currentDicSlotPtr);
		//RecordDic nextFieldOffset = getRecordFieldOffset(interiorRecord,i+1);//*((RecordDic*)nextDicSlotPtr);
        int fieldSize = getRecordFieldSize(interiorRecord,i);
        if(fieldSize != -1)
        {
		//if(currentFieldOffset >= 0)
		//{
			//int fieldSize = abs(nextFieldOffset) - currentFieldOffset;

			if(recordDescriptor[i].type == TypeVarChar)
			{
				int stringSize = fieldSize;
			    char *currentExteriorRecordField = exteriorRecordField + exteriorRecordFieldOffset;
			    *((int*)currentExteriorRecordField) = stringSize;
				exteriorRecordFieldOffset = exteriorRecordFieldOffset + sizeof(int);
			}

			memcpy(exteriorRecordField + exteriorRecordFieldOffset, (char*)interiorRecord + getRecordFieldOffset(interiorRecord,i), fieldSize);
			exteriorRecordFieldOffset = exteriorRecordFieldOffset + fieldSize;
		}
		else//Dictionary Null update
		{
			nullsIndicator[positionOfByte] = nullsIndicator[positionOfByte] | (1 << (7 - positionOfNullIndicator));

		}

	}

    return 0;
}


unsigned RecordBasedFileManager::getSizeOfExteriorRecord(const vector<Attribute> &recordDescriptor,const void *interiorRecord)
{

	unsigned numberOfFields = recordDescriptor.size();


	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);

	int exteriorRecordSize = 0;
	exteriorRecordSize= exteriorRecordSize + numberOfBytesForNullIndicator;

    //Calculate exteriorRecord Size
	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{

		//int currentFieldOffset = getRecordFieldOffset(interiorRecord,i);//*((RecordDic*)currentDicSlotPtr);
		//int nextFieldOffset = getRecordFieldOffset(interiorRecord,i);//*((RecordDic*)nextDicSlotPtr);

		//if (currentFieldOffset < 0 ) //minus value: NULL pointer
		//	continue;//Pass

		//int fieldSize = abs(nextFieldOffset) - currentFieldOffset; //ABS for next null value
		int fieldSize = getRecordFieldSize(interiorRecord,i);

		if(fieldSize == -1)
			continue;//Pass

		exteriorRecordSize = exteriorRecordSize + fieldSize;
		if(recordDescriptor[i].type == TypeVarChar)
		{
			exteriorRecordSize = exteriorRecordSize + sizeof(int);
		}

	}
	return exteriorRecordSize;
}


unsigned RecordBasedFileManager::getSizeOfInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exteriorRecord)
{

	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	//interiorRecord related
	int interiorRecordDicSize = sizeof(RecordDic)*(1+1+recordDescriptor.size()+1);//Tomb,NumOfRecs,Slots,LastSlot

	bool nullExist = false;

    unsigned interiorRecordSize = 0;
    interiorRecordSize = interiorRecordSize + interiorRecordDicSize;
    int exteriorRecordOffset = 0;

    //Calculate interiorRecord Size
	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));

		if(!nullExist)//Not Null Value
		{


		    if(recordDescriptor[i].type == TypeVarChar)
			{
		    	char *stringLength = (char*)exteriorRecordField + exteriorRecordOffset;
			    exteriorRecordOffset = exteriorRecordOffset + sizeof(int) + *((int*)stringLength); //length field + string
		        interiorRecordSize = interiorRecordSize + *((int*)stringLength);

		    }
		    else if (recordDescriptor[i].type == TypeInt)
		    {
		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordSize = interiorRecordSize + recordDescriptor[i].length;
		    }
		    else if (recordDescriptor[i].type ==TypeReal)
		    {
		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordSize = interiorRecordSize + recordDescriptor[i].length;
		    }
		}

	}
	return interiorRecordSize;
}

RC RecordBasedFileManager::transToInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exteriorRecord, void *interiorRecord)
{
	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	//char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	bool nullExist = false;

	if(directInsert)
		setTombstone(interiorRecord, Direct_Rec);
	else
		setTombstone(interiorRecord, Indirect_Rec);

	setNumOfRecord(interiorRecord, numberOfFields);


	//*((RecordDic*)interiorRecord) = (RecordDic)numberOfFields;//Start with number of fields
	//char *interiorRecordDicSlot = (char *)interiorRecord + 1*sizeof(RecordDic);
	//char *interiorRecordField = interiorRecordDicSlot + sizeof(RecordDic)*(numberOfFields + 1);

	int exteriorRecordOffset = 0;
	exteriorRecordOffset = exteriorRecordOffset + numberOfBytesForNullIndicator;

	int interiorRecordheaderSize = sizeof(RecordDic)*(1+1+numberOfFields + 1);

	int interiorRecordOffset = 0;
	interiorRecordOffset = interiorRecordOffset + interiorRecordheaderSize;

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));

		if(!nullExist)//Not Null Value
		{

		    if(recordDescriptor[i].type == TypeVarChar)
			{
		    	char *stringLength = (char*)exteriorRecord + exteriorRecordOffset;
		    	int stringLength_int = *((int*)stringLength);

			    exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field

			    memcpy((char*)interiorRecord + interiorRecordOffset, (char*)exteriorRecord + exteriorRecordOffset,stringLength_int);
			    setRecordFieldOffset(interiorRecord,i,interiorRecordOffset);

			    //*((RecordDic*)interiorRecordDicSlot+i) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

			    interiorRecordOffset = interiorRecordOffset + stringLength_int;
			    exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
		    }
		    else if (recordDescriptor[i].type == TypeInt)
		    {

		    	memcpy((char*)interiorRecord + interiorRecordOffset, (char*)exteriorRecord + exteriorRecordOffset,recordDescriptor[i].length);
		    	setRecordFieldOffset(interiorRecord,i,interiorRecordOffset);
		    	//*((RecordDic*)interiorRecordDicSlot+i) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordOffset = interiorRecordOffset + recordDescriptor[i].length;
		    }
		    else if (recordDescriptor[i].type ==TypeReal)
		    {
		    	memcpy((char*)interiorRecord + interiorRecordOffset, (char*)exteriorRecord + exteriorRecordOffset,recordDescriptor[i].length);
		    	setRecordFieldOffset(interiorRecord,i,interiorRecordOffset);

		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordOffset = interiorRecordOffset + recordDescriptor[i].length;
		    }
		}
		else//NULL (dictionary update but no offset increase)
			setRecordFieldOffset(interiorRecord,i,-interiorRecordOffset);
			//*((RecordDic*)interiorRecordDicSlot+i) = -((RecordDic)(headerSize + interiorRecordOffset));//Dictionary update(Null is minus)

	}
	setRecordFieldOffset(interiorRecord,numberOfFields,interiorRecordOffset);
	//*((RecordDic*)interiorRecordDicSlot+numberOfFields) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

    return 0;
}


unsigned RecordBasedFileManager::calculateRecordSize(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data)
{

	unsigned numberOfFields = recordDescriptor.size();
	//char *interiorRecordDicSlot = (char*)data + 1*sizeof(RecordDic);
	unsigned recordSize = sizeof(RecordDic)*(1 + 1 + numberOfFields + 1);//Tombstone,numOfRec,Slots,last slot

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
//        char *currentDicSlotPtr = interiorRecordDicSlot + i*sizeof(RecordDic);
//        char *nextDicSlotPtr = interiorRecordDicSlot + (i+1)*sizeof(RecordDic);
		//int currentFieldOffset = getRecordFieldOffset(data,i);//*((RecordDic*)currentDicSlotPtr);
		//int nextFieldOffset = getRecordFieldOffset(data,i+1);

		//if (currentFieldOffset < 0 ) //minus value: NULL pointer
			//continue;//Pass

		//int fieldSize = abs(nextFieldOffset) - currentFieldOffset; //ABS for next null value
		int fieldSize = getRecordFieldSize(data,i);
		if (fieldSize == -1)
			continue;//Pass

		recordSize = recordSize + fieldSize;
	}
	return recordSize;
}

/*
unsigned RecordBasedFileManager::calculateRecordSize(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data)
{
    int recordSize = 0;
	unsigned numberOfFields = recordDescriptor.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);

	recordSize = recordSize + numberOfBytesForNullIndicator;

	unsigned char *nullsIndicator = (unsigned char *) malloc(numberOfBytesForNullIndicator);
	memcpy(nullsIndicator, data, numberOfBytesForNullIndicator);

	bool nullExist = false;

	for (unsigned i = 0; i < recordDescriptor.size() ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));


		if(!nullExist)//Not Null Value
		{

		    if(recordDescriptor[i].type == TypeVarChar)
		    {
		    	//Read length of string
		    	char *stringLength = (char*)data + recordSize;
			    recordSize = recordSize + sizeof(int) + *((int*)stringLength); //length field + string
		    }
		    else
		    	recordSize = recordSize + recordDescriptor[i].length;
		}
		//Null Value - skip the attribute
	}
	free(nullsIndicator);
	return recordSize;
}

*/

RC RecordBasedFileManager::setRecordOffset(void *pageToProcess,unsigned slot, PageDic offset)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic)*(3+slot);
	char *recordDic = (char*)pageToProcess + recordDicOffset;
	*((PageDic*)recordDic) = offset; //new dictionary to find record(Previous free space offset)

    return 0;
}

RC RecordBasedFileManager::setNumOfRecordSlots(void *pageToProcess, PageDic numOfRecordSlots)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic)*2;
	char *recordDic = (char*)pageToProcess + recordDicOffset;
	*((PageDic*)recordDic) = numOfRecordSlots; //record increase
	return 0;
}
RC RecordBasedFileManager::setFreeSpaceOffset(void *pageToProcess, PageDic offset)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic);
	char *recordDic = (char*)pageToProcess + recordDicOffset;
    *((PageDic*)recordDic) = offset; //new free space
	return 0;
}

PageDic RecordBasedFileManager::getRecordOffset(void *pageToProcess,unsigned slot)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic)*(3+slot);
	char *recordDic = (char*)pageToProcess + recordDicOffset;
	return *((PageDic*)recordDic);
}

PageDic RecordBasedFileManager::getNumOfRecordSlots(void *pageToProcess)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic)*2;
	char *recordDic = (char*)pageToProcess + recordDicOffset;
	return *((PageDic*)recordDic);
}
PageDic RecordBasedFileManager::getFreeSpaceOffset(void *pageToProcess)
{
	PageDic recordDicOffset = PAGE_SIZE - sizeof(PageDic);
	char *recordDic = (char*)pageToProcess + recordDicOffset;
	return *((PageDic*)recordDic);
}


unsigned RecordBasedFileManager::getFreeSpaceSize(void *pageToProcess)
{
	int freeSpaceOffset = getFreeSpaceOffset(pageToProcess);//*((PageDic*)freeSpaceOffsetPtr);
	int numOfRecordSlots = getNumOfRecordSlots(pageToProcess);//(int)*((PageDic*)numOfRecordsPtr);
	int lastRecordDicOffset = PAGE_SIZE - sizeof(PageDic)*(2+numOfRecordSlots);
	return lastRecordDicOffset - freeSpaceOffset;
}

RC RecordBasedFileManager::verifyFreeSpaceForRecord(FileHandle &fileHandle, int pageNum, void *pageToProcess, int recordSize)
{

	int freeSpaceSize = -1;


	if(freeSpaceSize == -1)
	{
	    RC rc = fileHandle.readPage(pageNum,pageToProcess);//Read Page
	    if(rc != 0)
		    return rc;

	    //FreeSpaceSize
	    freeSpaceSize = getFreeSpaceSize(pageToProcess);
    }

    int spaceToNeed = recordSize + sizeof(PageDic);// + 4096;
    if(spaceToNeed > freeSpaceSize)
    {
	    return -1;
    }
    return 0;
}

RC RecordBasedFileManager::insertRecordNewPage(FileHandle &fileHandle, int pageNum, const void *data, int recordSize, RID &rid)
{
	void *pageToProcess = tempPage; //Create a new page

	memcpy(pageToProcess,data,recordSize);//Write record

    //update
	setRecordOffset(pageToProcess,0,0);
    setNumOfRecordSlots(pageToProcess, 1);
	setFreeSpaceOffset(pageToProcess, recordSize);


    //Insert Page
	RC rc = fileHandle.appendPage(pageToProcess);

	//free(pageToProcess);

    //update
	rid.pageNum = pageNum;
	rid.slotNum = 0;


	return rc;
}



RC RecordBasedFileManager::insertRecordExistingPage(FileHandle &fileHandle, int pageNum, void* pageToProcess, const void *data, int recordSize, RID &rid)
{
    int numOfRecordSlots = getNumOfRecordSlots(pageToProcess);//(int)*((PageDic*)numOfRecordsPtr);

	//Free space related variable
	int freeSpaceOffset = getFreeSpaceOffset(pageToProcess);//(int)*((PageDic*)freeSpaceOffsetPtr);
	char* writeLocation = (char*)pageToProcess + freeSpaceOffset;
	memcpy((void*)writeLocation,data,recordSize);//write the record at the start of free space


	int insertedRecordSlotNum = numOfRecordSlots;


	for(int i = 0 ; i < numOfRecordSlots ; i++)
	{
		PageDic insertedOffset = getRecordOffset(pageToProcess,i);
		if(insertedOffset == -1)
		{
			insertedRecordSlotNum = i;
			break;
		}
	}


    //update
	setRecordOffset(pageToProcess,insertedRecordSlotNum,freeSpaceOffset);
	if(insertedRecordSlotNum == numOfRecordSlots)
    	setNumOfRecordSlots(pageToProcess, numOfRecordSlots + 1);
	setFreeSpaceOffset(pageToProcess, freeSpaceOffset + recordSize);

	//WritePage
	RC rc = fileHandle.writePage(pageNum,pageToProcess);

	rid.pageNum = pageNum;
	rid.slotNum = insertedRecordSlotNum;

	return rc;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	//Calculate record Size
	RC rc = -1;
	int recordSize = getSizeOfInteriorRecord(recordDescriptor, data);//calculateRecordSize(fileHandle,recordDescriptor,data);

	if (recordSize > PAGE_SIZE || recordSize == 0)
	{
		//A page is smaller than record(beyond the scope - stop) and not 0
		return -1;
	}


	void *interiorRecord = tempRecord; //malloc(recordSize);
    rc = transToInteriorRecord(recordDescriptor,data,interiorRecord);
    if(rc == -1)
    {
    	//free(interiorRecord);
    	return rc;
    }


	int numOfPages = fileHandle.getNumberOfPages();


	if(numOfPages == 0)
	{
		RC success = insertRecordNewPage(fileHandle,numOfPages, interiorRecord,recordSize,rid);
		//free(interiorRecord);
		return success;
	}


	int lastPageNum = numOfPages - 1; //First candidate: Last page

	void *pageToProcess = tempPage;//malloc(PAGE_SIZE);
	rc = verifyFreeSpaceForRecord(fileHandle, lastPageNum, pageToProcess, recordSize); //verify the last page

	if(rc == 0)//Free space in the last page
	{
		RC success = insertRecordExistingPage(fileHandle,lastPageNum,pageToProcess, interiorRecord, recordSize, rid);
		return success;
	}

	//L--------------------last page do not have enough space-------------------------------

	//search entire pages

	for (int i = 0 ; i < lastPageNum ; i++)
	{

		rc = verifyFreeSpaceForRecord(fileHandle,i,pageToProcess,recordSize);

		if (rc ==0)//Page with enough space exists
		{
			RC success = insertRecordExistingPage(fileHandle,i,pageToProcess, interiorRecord, recordSize, rid);
			return success;
		}

	}


	//Make new page and insert
	rc = insertRecordNewPage(fileHandle,numOfPages, interiorRecord,recordSize,rid);


	return rc;

}

RC RecordBasedFileManager::readIndirectRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	void *page = tempPage;//malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(rid.pageNum,page);
	if(rc != 0)
		return -1;
	//find the offset from dictionary
	PageDic recordOffset = getRecordOffset(page,rid.slotNum);//(int)*((PageDic*)recordOffsetPtr);

	if(recordOffset == -1)
		return -1;//deleted Page

	char *recordToRead = (char*)page + recordOffset;

	//Tombstone
	RecordDic tombstone = getTombstone(recordToRead);

	if(tombstone == Indirect_Rec)//directly inserted record
	{
		int recordSize = getSizeOfExteriorRecord(recordDescriptor,recordToRead);//calculateRecordSize(fileHandle,recordDescriptor,(void*)recordToread);

		if(recordSize == 0)
		{
			return -1; // No record exists
		}

		rc = transToExteriorRecord(recordDescriptor,recordToRead,data);

		return rc;

	}
	return -1;//records other than indirect not allowed
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	void *page = tempPage;//malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(rid.pageNum,page);
	if(rc != 0)
		return -1;
	//find the offset from dictionary
	PageDic recordOffset = getRecordOffset(page,rid.slotNum);//(int)*((PageDic*)recordOffsetPtr);

	if(recordOffset == -1)
		return -1;//deleted Page

	char *recordToRead = (char*)page + recordOffset;

	//Tombstone
	RecordDic tombstone = getTombstone(recordToRead);

	if(tombstone == Direct_Rec)//directly inserted record
	{
		int recordSize = getSizeOfExteriorRecord(recordDescriptor,recordToRead);//calculateRecordSize(fileHandle,recordDescriptor,(void*)recordToread);

		if(recordSize == 0)
		{
			return -1; // No record exists
		}

		rc = transToExteriorRecord(recordDescriptor,recordToRead,data);

		return rc;

	}
	else if(tombstone < 0)//find indirect record
	{
		RID indirectRid;
		indirectRid.pageNum = abs(getTombstone(recordToRead));
		indirectRid.slotNum = abs(getTombstone(recordToRead+1));
		rc = readIndirectRecord(fileHandle, recordDescriptor, indirectRid, data);
		return rc;
	}

    return -1;//Do not allow direct read for indirect record and ...
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int recordSize = 0;
	unsigned numberOfFields = recordDescriptor.size();
	int numberOfBytesForNullIndicator = (int)ceil((float)numberOfFields/8);

	recordSize = recordSize + numberOfBytesForNullIndicator;

	unsigned char *nullsIndicator = (unsigned char *) malloc(numberOfBytesForNullIndicator);
	memcpy(nullsIndicator, data, numberOfBytesForNullIndicator);

	bool nullExist = false;

	for (unsigned i = 0 ;i < recordDescriptor.size() ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));

		if(!nullExist)//Not Null Value
		{


		    if(recordDescriptor[i].type == TypeVarChar)
			{
		    	//Read length of string
		    	char *stringLength = (char*)data + recordSize;
		    	recordSize = recordSize + sizeof(int);//string length
		    	char *stringToPrint_char = (char*)data + recordSize;

		    	string stringToPrint(stringToPrint_char,*((int*)stringLength));
			    recordSize = recordSize + *((int*)stringLength); //string
			    cout <<recordDescriptor[i].name << ": "<< stringToPrint;
		    }
		    else if (recordDescriptor[i].type == TypeInt)
		    {
		    	char *integerToPrint = (char*)data + recordSize;
		    	cout <<recordDescriptor[i].name << ": "<< *((int*)integerToPrint);
		    	recordSize = recordSize + recordDescriptor[i].length;
		    }
		    else if (recordDescriptor[i].type ==TypeReal)
		    {
		    	char *realToPrint = (char*)data + recordSize;
		    	cout <<recordDescriptor[i].name << ": "<< *((float*)realToPrint);
		    	recordSize = recordSize + recordDescriptor[i].length;
		    }

		}
		else//Null Value
		{
			cout <<recordDescriptor[i].name << ": NULL";
		}
		cout << "\t";
	}
	cout << "\n";
	free(nullsIndicator);
    return 0;
}




RC RecordBasedFileManager::compactRecords(char *pageToProcess, PageDic from, PageDic to)
{
	unsigned difference = from - to;
	char *recordsToMove = pageToProcess + from;
	char *destination = pageToProcess + to;
	PageDic freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	int sizeOfMovingRecords = freeSpaceOffset - from;

	char *movingBuffer = (char*)malloc(sizeOfMovingRecords);
	if (movingBuffer == NULL)
	{
		return -1;
	}


	memcpy(movingBuffer,recordsToMove,sizeOfMovingRecords);//get the buffer to move
	memcpy(destination,movingBuffer,sizeOfMovingRecords);//overwrite the buffer to the place of deleted record
	free(movingBuffer);

	//Dictionary Update
	//--free space dic
	setFreeSpaceOffset(pageToProcess,freeSpaceOffset - difference);
	//--search each slot and modify it if necessary
	for(int i = 0 ; i < getNumOfRecordSlots(pageToProcess) ; i++)
	{
		PageDic targetRecordOffset = getRecordOffset(pageToProcess,i);
		if(to < targetRecordOffset)
			setRecordOffset(pageToProcess,i,targetRecordOffset - difference); //other record that moved
		else if(to == targetRecordOffset)
			setRecordOffset(pageToProcess,i,-1); //deleted record
	}

	return 0;
}

RC RecordBasedFileManager::pushRecords(char *pageToProcess, PageDic from, PageDic to)
{
	unsigned difference = to - from;
	char *recordsToMove = pageToProcess + from;
	char *destination = pageToProcess + to;
	PageDic freeSpaceOffset = getFreeSpaceOffset(pageToProcess);
	int sizeOfMovingRecords = freeSpaceOffset - from;

	char *movingBuffer = (char*)malloc(sizeOfMovingRecords);
	if (movingBuffer == NULL)
	{
		return -1;
	}


	memcpy(movingBuffer,recordsToMove,sizeOfMovingRecords);//get the buffer to move
	memcpy(destination,movingBuffer,sizeOfMovingRecords);//overwrite the buffer to the place
	free(movingBuffer);

	//Dictionary Update
	//--free space dic
	setFreeSpaceOffset(pageToProcess,freeSpaceOffset + difference);
	//--search each slot and modify it if necessary
	for(int i = 0 ; i < getNumOfRecordSlots(pageToProcess) ; i++)
	{
		PageDic targetRecordOffset = getRecordOffset(pageToProcess,i);
		if(from < targetRecordOffset)
			setRecordOffset(pageToProcess,i,targetRecordOffset + difference); //other record that moved
	}

	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	//Find Page and Read
	char *pageToProcess = (char*)tempPage;//(char*)malloc(PAGE_SIZE);

	if(pageToProcess == NULL)
		return -1;

	RC rc = fileHandle.readPage(rid.pageNum,pageToProcess);//Read Page
	if(rc != 0)
	{
		return rc;
	}

	//Find Dictionary Slot
	PageDic deletedRecordOffset = getRecordOffset(pageToProcess,rid.slotNum);

	char *recordToProcess = pageToProcess + deletedRecordOffset;

	int recordSize = calculateRecordSize(fileHandle, recordDescriptor, recordToProcess);
	int recordsToMoveOffset = deletedRecordOffset + recordSize;
	//char *recordsToMove = pageToProcess + recordsToMoveOffset;

	rc = compactRecords(pageToProcess, recordsToMoveOffset, deletedRecordOffset);
	if (rc == -1)
	{
		return rc;
	}


	//Write Page
    rc = fileHandle.writePage(rid.pageNum,pageToProcess);


    return rc;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	//Find Page and Read
	char *pageToProcess = (char*)tempPage;//(char*)malloc(PAGE_SIZE);

	if(pageToProcess == NULL)
		return -1;

	RC rc = fileHandle.readPage(rid.pageNum,pageToProcess);//Read Page
	if(rc != 0)
	{
		return rc;
	}

	//Find Dictionary Slot
    PageDic existingRecordOffset = getRecordOffset(pageToProcess,rid.slotNum);
	char *existingRecord = pageToProcess + existingRecordOffset;
	int existingRecordSize = calculateRecordSize(fileHandle, recordDescriptor, existingRecord);
	int newRecordSize = getSizeOfInteriorRecord(recordDescriptor, data);

	if(existingRecordSize == newRecordSize)
	{
		//Overwrite
		memcpy(existingRecord,data,newRecordSize);

	}
	else if(existingRecordSize > newRecordSize)
	{
		//Overwrite
		memcpy(existingRecord,data,newRecordSize);
		//Compact
		rc = compactRecords(pageToProcess,existingRecordOffset+existingRecordSize,existingRecordOffset + newRecordSize);
		if(rc == -1)
		{
			return rc;
		}
	}
	else
	{
		unsigned difference = newRecordSize - existingRecordSize;
		unsigned freeSpace = getFreeSpaceSize(pageToProcess);
		if(freeSpace >= difference)
		{
			rc = pushRecords(pageToProcess,existingRecordOffset+existingRecordSize,existingRecordOffset + newRecordSize);
			if(rc == -1)
			{
				return rc;
			}
			memcpy(existingRecord,data,newRecordSize);
		}
		else
		{
			//Migrate

			RID forwardedRid;
			directInsert = Indirect_Rec;
			rc = insertRecord(fileHandle,recordDescriptor,data,forwardedRid);
			directInsert = Direct_Rec;
			if(rc == -1)
			{
				return rc;
			}
			int sizeOfTombstone = 2 * sizeof(RecordDic);

			//Tombstone created
			RecordDic fowardedPageNum = -forwardedRid.pageNum;
			char *fowardedPageNumPtr = (char*)&fowardedPageNum;
			RecordDic fowardedSlotNum = -forwardedRid.slotNum;
			char *fowardedSlotNumPtr = (char*)&fowardedSlotNum;
			//insert Tombstone
			memcpy(existingRecord,fowardedPageNumPtr,sizeof(RecordDic));
			memcpy(existingRecord+sizeof(RecordDic),fowardedSlotNumPtr,sizeof(RecordDic));

			//compact
			compactRecords(pageToProcess,existingRecordOffset+existingRecordSize,existingRecordOffset + sizeOfTombstone);
		}

	}


    rc = fileHandle.writePage(rid.pageNum,pageToProcess);

    return rc;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	//Find Page and Read
	char *pageToProcess = (char*)tempPage;//(char*)malloc(PAGE_SIZE);

	if(pageToProcess == NULL)
		return -1;

	RC rc = fileHandle.readPage(rid.pageNum,pageToProcess);//Read Page
	if(rc != 0)
	{
		return rc;
	}

	char *recordToProcess = pageToProcess + getRecordOffset(pageToProcess,rid.slotNum);

	for (unsigned i = 0 ; i < recordDescriptor.size() ; i++)
	{
		if(recordDescriptor[i].name.compare(attributeName) == 0)
		{
			int fieldSize = getRecordFieldSize(recordToProcess,i);

			if(fieldSize != -1)
			{
				memset(data,0,1);
				memcpy((char*)data+1,recordToProcess+getRecordFieldOffset(recordToProcess,i),fieldSize);
			}
			else  //NULL
				memset(data,1,1);
		}
		return 0;
	}

	return -1;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator)
{
    return -1;
}

/****************************debug*/
/*
    int interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptorForTweetMessage,record);
    cout<<interiorRecordSize<<"\n"<<flush;
    void *interiorRecord = malloc(interiorRecordSize);
    rbfm->transToInteriorRecord(recordDescriptorForTweetMessage,record,interiorRecord);
    cout<<rbfm->calculateRecordSize(fileHandle,recordDescriptorForTweetMessage,interiorRecord)<<"\n"<<flush;


    int exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptorForTweetMessage,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    void *exteriorRecord = malloc(exteriorRecordSize);
    rbfm->transToExteriorRecord(recordDescriptorForTweetMessage,interiorRecord,exteriorRecord);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptorForTweetMessage,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->transToInteriorRecord(recordDescriptorForTweetMessage,record,interiorRecord);
    cout<<rbfm->calculateRecordSize(fileHandle,recordDescriptorForTweetMessage,interiorRecord)<<"\n"<<flush;

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptorForTweetMessage,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->transToExteriorRecord(recordDescriptorForTweetMessage,interiorRecord,exteriorRecord);


    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptorForTweetMessage,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->transToInteriorRecord(recordDescriptorForTweetMessage,record,interiorRecord);
    cout<<rbfm->calculateRecordSize(fileHandle,recordDescriptorForTweetMessage,interiorRecord)<<"\n"<<flush;

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptorForTweetMessage,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->transToExteriorRecord(recordDescriptorForTweetMessage,interiorRecord,exteriorRecord);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptorForTweetMessage,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->transToInteriorRecord(recordDescriptorForTweetMessage,record,interiorRecord);
    cout<<rbfm->calculateRecordSize(fileHandle,recordDescriptorForTweetMessage,interiorRecord)<<"\n"<<flush;


    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptorForTweetMessage,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->transToExteriorRecord(recordDescriptorForTweetMessage,interiorRecord,exteriorRecord);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptorForTweetMessage,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->transToInteriorRecord(recordDescriptorForTweetMessage,record,interiorRecord);
    cout<<rbfm->calculateRecordSize(fileHandle,recordDescriptorForTweetMessage,interiorRecord)<<"\n"<<flush;

    cout<<"\n-----------------------------------------------------------------\n"<<flush;

    free(interiorRecord);
    free(exteriorRecord);

*/
/****************************debug*/
