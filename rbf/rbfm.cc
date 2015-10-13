#include "rbfm.h"
#include <iostream> //added library
#include <fstream> //added library
#include <cmath> //added library
#include <cstdlib> //added library
#include <cstring> //added library
#include <cstdio> //added library

typedef signed short RecordDic;
typedef unsigned short PageDic;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();


    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
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


RC RecordBasedFileManager::transToExteriorRecord(const vector<Attribute> &recordDescriptor,const void *interiorRecord, void *exteriorRecord, unsigned exteriorRecordSize)
{

	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	memset(nullsIndicator, 0, numberOfBytesForNullIndicator);

	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;
	*((RecordDic*)interiorRecord) = (RecordDic)numberOfFields;//Start with number of fields
	char *interiorRecordDicSlot = (char *)interiorRecord + 1*sizeof(RecordDic);
	char *interiorRecordField = interiorRecordDicSlot + sizeof(RecordDic)*(numberOfFields + 1);
	int headerSize = sizeof(RecordDic)*(numberOfFields + 2);
	int exteriorRecordFieldOffset = 0;

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;

        char *currentDicSlotPtr = interiorRecordDicSlot + i*sizeof(RecordDic);
        char *nextDicSlotPtr = interiorRecordDicSlot + (i+1)*sizeof(RecordDic);
		int currentFieldOffset = *((RecordDic*)currentDicSlotPtr);
		int nextFieldOffset = *((RecordDic*)nextDicSlotPtr);


		if(currentFieldOffset >= 0)
		{
			int fieldSize = abs(nextFieldOffset) - currentFieldOffset;

			if(recordDescriptor[i].type == TypeVarChar)
			{
				int stringSize = fieldSize;
			    char *currentExteriorRecordField = exteriorRecordField + exteriorRecordFieldOffset;
			    *((int*)currentExteriorRecordField) = stringSize;
				exteriorRecordFieldOffset = exteriorRecordFieldOffset + sizeof(int);
			}

			memcpy(exteriorRecordField + exteriorRecordFieldOffset, interiorRecordField + currentFieldOffset- headerSize, fieldSize);
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

	char *interiorRecordDicSlot = (char*)interiorRecord + 1*sizeof(RecordDic);
    //Calculate exteriorRecord Size
	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
        char *currentDicSlotPtr = interiorRecordDicSlot + i*sizeof(RecordDic);
        char *nextDicSlotPtr = interiorRecordDicSlot + (i+1)*sizeof(RecordDic);
		int currentFieldOffset = *((RecordDic*)currentDicSlotPtr);
		int nextFieldOffset = *((RecordDic*)nextDicSlotPtr);

		if (currentFieldOffset < 0 ) //minus value: NULL pointer
			continue;//Pass

		int fieldSize = abs(nextFieldOffset) - currentFieldOffset; //ABS for next null value

		exteriorRecordSize = exteriorRecordSize + fieldSize;
		if(recordDescriptor[i].type == TypeVarChar)
		{
			exteriorRecordSize = exteriorRecordSize + sizeof(int);
		}

	}
	return exteriorRecordSize;
}

/*
unsigned TransRecForRead(const vector<Attribute> &recordDescriptor,const void *data)
{

}*/

unsigned RecordBasedFileManager::getSizeOfInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exteriorRecord)
{

	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	//interiorRecord related
	int interiorRecordDicSize = sizeof(RecordDic)*(2+recordDescriptor.size());//num of fields + dictionary + final point

	bool nullExist = false;

    unsigned interiorRecordSize = interiorRecordDicSize;
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

RC RecordBasedFileManager::transToInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exteriorRecord, void *interiorRecord, unsigned interiorRecordSize)
{
	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	bool nullExist = false;


	*((RecordDic*)interiorRecord) = (RecordDic)numberOfFields;//Start with number of fields
	char *interiorRecordDicSlot = (char *)interiorRecord + 1*sizeof(RecordDic);
	char *interiorRecordField = interiorRecordDicSlot + sizeof(RecordDic)*(numberOfFields + 1);
	int exteriorRecordOffset = 0;
	int interiorRecordOffset = 0;
	int headerSize = sizeof(RecordDic)*(numberOfFields + 2);

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
		    	int stringLength_int = *((int*)stringLength);

			    exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field

			    memcpy(interiorRecordField + interiorRecordOffset, exteriorRecordField + exteriorRecordOffset,stringLength_int);
			    *((RecordDic*)interiorRecordDicSlot+i) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

			    interiorRecordOffset = interiorRecordOffset + stringLength_int;
			    exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
		    }
		    else if (recordDescriptor[i].type == TypeInt)
		    {
		    	char *currentInteriorRecordField = interiorRecordField + interiorRecordOffset;
		    	char *currentExteriorRecordField = exteriorRecordField + exteriorRecordOffset;
		    	*((int*)currentInteriorRecordField) = *((int*)currentExteriorRecordField);
		    	*((RecordDic*)interiorRecordDicSlot+i) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordOffset = interiorRecordOffset + recordDescriptor[i].length;
		    }
		    else if (recordDescriptor[i].type ==TypeReal)
		    {
		    	char *currentInteriorRecordField = interiorRecordField + interiorRecordOffset;
		    	char *currentExteriorRecordField = exteriorRecordField + exteriorRecordOffset;
		    	*((int*)currentInteriorRecordField) = *((int*)currentExteriorRecordField);
		    	*((RecordDic*)interiorRecordDicSlot+i) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

		    	exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
		    	interiorRecordOffset = interiorRecordOffset + recordDescriptor[i].length;
		    }
		}
		else//NULL (dictionary update but no offset increase)
			*((RecordDic*)interiorRecordDicSlot+i) = -((RecordDic)(headerSize + interiorRecordOffset));//Dictionary update(Null is minus)

	}
	*((RecordDic*)interiorRecordDicSlot+numberOfFields) = (RecordDic)(headerSize + interiorRecordOffset);//Dictionary update

    return 0;
}


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

RC RecordBasedFileManager::verifyFreeSpaceForRecord(FileHandle &fileHandle, int pageNum, void *pageToProcess, int recordSize)
{
	if(pageNum < 0)//No page to read in a file
		return -1;

	RC rc = fileHandle.readPage(pageNum,pageToProcess);//Read Page
	if(rc != 0)
		return rc;

	//Free space related variable
	char *freeSpaceOffsetPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic);
	int freeSpaceOffset = *((PageDic*)freeSpaceOffsetPtr);

    //Record related Variable
	char *numOfRecordsPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic)*2;
	int numOfRecords = (int)*((PageDic*)numOfRecordsPtr);
	int lastRecordDicOffset = PAGE_SIZE - sizeof(PageDic)*(2+numOfRecords);

	//FreeSpaceSize
	int freeSpaceSize = lastRecordDicOffset - freeSpaceOffset;


	int spaceToNeed = recordSize + sizeof(PageDic);// + 4096;

	if(spaceToNeed > freeSpaceSize)
	{
		return -1;
	}
	return 0;
}

RC insertRecordNewPage(FileHandle &fileHandle, int pageNum, const void *data, int recordSize, RID &rid)
{
	void *pageToProcess = malloc(PAGE_SIZE); //Create a new page

	memcpy(pageToProcess,data,recordSize);//Write record
	char *freeSpaceOffsetPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic); //free space offset pointer
	char *numOfRecordsPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic)*2; //number of records pointer
	int numOfRecords = 0;
	char *lastRecordOffsetPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic)*(2 + numOfRecords);
    char *insertedRecordOffsetPtr = lastRecordOffsetPtr - sizeof(PageDic);

    //update
	*((PageDic*)insertedRecordOffsetPtr) = (PageDic)0; //new dictionary to find record(Previous free space offset)
	*((PageDic*)numOfRecordsPtr) = (PageDic)1; //number of records
    *((PageDic*)freeSpaceOffsetPtr) = (PageDic)recordSize; //new free space


    //Insert Page
	RC rc = fileHandle.appendPage(pageToProcess);

	free(pageToProcess);


	rid.pageNum = pageNum;
	rid.slotNum = 0;



	return rc;
}



RC insertRecordExistingPage(FileHandle &fileHandle, int pageNum, void* pageToProcess, const void *data, int recordSize, RID &rid)
{
	char *freeSpaceOffsetPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic); //free space offset pointer
	char *numOfRecordsPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic)*2; //number of records pointer
    int numOfRecords = (int)*((PageDic*)numOfRecordsPtr);

	//Free space related variable
	int freeSpaceOffset = (int)*((PageDic*)freeSpaceOffsetPtr);
	char* writeLocation = (char*)pageToProcess + freeSpaceOffset;
	memcpy((void*)writeLocation,data,recordSize);//write the record at the start of free space

	char *lastRecordOffsetPtr = (char*)pageToProcess + PAGE_SIZE - sizeof(PageDic)*(2+numOfRecords);
    char *insertedRecordOffsetPtr = lastRecordOffsetPtr - sizeof(PageDic);
    //update
	*((PageDic*)insertedRecordOffsetPtr) = (PageDic)freeSpaceOffset; //new dictionary to find record(Previous free space offset)
	*((PageDic*)numOfRecordsPtr) = *((PageDic*)numOfRecordsPtr) + 1; //record increase
    *((PageDic*)freeSpaceOffsetPtr) = (PageDic)(freeSpaceOffset + recordSize); //new free space


	//WritePage
	RC rc = fileHandle.writePage(pageNum,pageToProcess);
	rid.pageNum = pageNum;
	rid.slotNum = numOfRecords;

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




	void *interiorRecord = malloc(recordSize);
    rc = transToInteriorRecord(recordDescriptor,data,interiorRecord,recordSize);
    if(rc == -1)
    {
    	free(interiorRecord);
    	return rc;
    }


	int numOfPages = fileHandle.getNumberOfPages();


	if(numOfPages == 0)
	{
		RC success = insertRecordNewPage(fileHandle,numOfPages, interiorRecord,recordSize,rid);
		free(interiorRecord);
		return success;
	}


	int lastPageNum = numOfPages - 1; //First candidate: Last page

	void *pageToProcess = malloc(PAGE_SIZE);
	rc = verifyFreeSpaceForRecord(fileHandle, lastPageNum, pageToProcess, recordSize); //verify the last page

	if(rc == 0)//Free space in the last page
	{
		RC success = insertRecordExistingPage(fileHandle,lastPageNum,pageToProcess, interiorRecord, recordSize, rid);
		free(pageToProcess);
		free(interiorRecord);
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
			free(pageToProcess);
			free(interiorRecord);
			return success;
		}

	}


	//Make new page and insert
	rc = insertRecordNewPage(fileHandle,numOfPages, interiorRecord,recordSize,rid);

	free(pageToProcess);
	free(interiorRecord);

	return rc;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	void *page = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(rid.pageNum,page);
	if(rc != 0)
		return -1;
	//find the offset from dictionary
	char *recordOffsetPtr= (char*)page + PAGE_SIZE - sizeof(PageDic)*(3 + (int)rid.slotNum);
	int recordOffset = (int)*((PageDic*)recordOffsetPtr);
	char *recordToread = (char*)page + recordOffset;

	int recordSize = getSizeOfExteriorRecord(recordDescriptor,recordToread);//calculateRecordSize(fileHandle,recordDescriptor,(void*)recordToread);

	if(recordSize == 0)
	{
		free(page);
		return -1; // No record exists
	}

	rc = transToExteriorRecord(recordDescriptor,recordToread,data,recordSize);

	if(rc == -1)
	{
		free(page);
		return rc;
	}

	free(page);
    return 0;
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



/****************************debug*/
/*
    int interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,record);
    cout<<interiorRecordSize<<"\n"<<flush;
    void *interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    int exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    void *exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);

    interiorRecordSize = rbfm->getSizeOfInteriorRecord(recordDescriptor,exteriorRecord);
    cout<<interiorRecordSize<<"\n"<<flush;
    free(interiorRecord);
    interiorRecord = malloc(interiorRecordSize);
    rbfm->TransToInteriorRecord(recordDescriptor,record,interiorRecord,interiorRecordSize);

    exteriorRecordSize = rbfm->getSizeOfExteriorRecord(recordDescriptor,interiorRecord);
    cout<<exteriorRecordSize<<"\n"<<flush;
    free(exteriorRecord);
    exteriorRecord = malloc(exteriorRecordSize);
    rbfm->TransToExteriorRecord(recordDescriptor,interiorRecord,exteriorRecord,exteriorRecordSize);


    cout<<"\n-----------------------------------------------------------------\n"<<flush;

    free(interiorRecord);
    free(exteriorRecord);
*/
/****************************debug*/


