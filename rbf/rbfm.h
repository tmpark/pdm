#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar, TypeDeleted} AttrType;

typedef unsigned AttrLength;


struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

struct ExtractedAttr {
    unsigned fieldNum;
    AttrType type;     // attribute type
};


// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP	   // no condition
} CompOp;



//custom type
typedef signed short RecordDic;
typedef signed short PageDic;

typedef enum {
	Direct_Rec = -1,
	Indirect_Rec = -2
}RecordType;


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator();
  ~RBFM_ScanIterator();

  template <typename T>
  int compareValues(T const valueExtracted, T const valueCompared, int compOp);
  RC projectData(vector<ExtractedAttr> &extractedDataDescriptor, char *recordToRead, void *data);
  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);// { return RBFM_EOF; };
  RC close();// { return -1; };

  vector<ExtractedAttr> extractedDataDescriptor;

  int conditionAttrFieldNum;
  int conditionAttrFieldType;
  unsigned currentPageNum;
  char *currentPage;
  unsigned currentRecordNum;
  char *currentRecord;
  unsigned numOfPages;
  unsigned numOfRecords;
  FileHandle *fileHandle;
  char *tempPage;
  char *tempPage1;
  const void *value;
  CompOp compOp;


};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);


  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);


  //-----------------custom functions--------------------------------------------------------------------------
  bool isNullField(const void *data, unsigned fieldNum);

  RecordDic getRecordFieldOffset(const void *recordToProcess, unsigned fieldNum);
  RecordDic getRecordFieldSize(const void *recordToProcess, unsigned fieldNum);
  RecordDic getNumOfRecord(const void *recordToProcess);
  RecordDic getTombstone(void *recordToProcess);
  RC setRecordFieldOffset(void *recordToProcess, unsigned fieldNum, RecordDic offset);
  RC setNumOfRecord(void *recordToProcess, RecordDic numOfRecordFields);
  RC setTombstone(void *recordToProcess, RecordDic type);
  RC setRecordOffset(void *pageToProcess,unsigned slot, PageDic offset);
  RC setNumOfRecordSlots(void *pageToProcess, PageDic numOfRecordSlots);
  RC setFreeSpaceOffset(void *pageToProcess, PageDic offset);
  PageDic getRecordOffset(void *pageToProcess,unsigned slot);
  PageDic getNumOfRecordSlots(void *pageToProcess);
  PageDic getFreeSpaceOffset(void *pageToProcess);
  unsigned getFreeSpaceSize(void *pageToProcess);





  RC insertRecordExistingPage(FileHandle &fileHandle, int pageNum, void* pageToProcess, const void *data, int recordSize, RID &rid);
  RC insertRecordNewPage(FileHandle &fileHandle, int pageNum, void* pageToProcess, const void *data, int recordSize, RID &rid);

  unsigned calculateRecordSize(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data);
  RC verifyFreeSpaceForRecord(FileHandle &fileHandle, int pageNum, void *pageToProcess, int recordSize);


  unsigned getSizeOfInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exteriorRecord);
  RC transToInteriorRecord(const vector<Attribute> &recordDescriptor,const void *exterioRecord, void *interiorRecord);

  unsigned getSizeOfExteriorRecord(const vector<Attribute> &recordDescriptor,const void *interiorRecord);
  RC transToExteriorRecord(const vector<Attribute> &recordDescriptor,const void *interiorRecord, void *exteriorRecord);

  RC compactRecords(char *pageToProcess, PageDic from, PageDic to);
  RC pushRecords(char *pageToProcess, PageDic from, PageDic to);
  RC readIndirectRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  RC indirectUpdateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  bool indirectRef;
  char tempPage[PAGE_SIZE];
  char tempPage1[PAGE_SIZE];
  char tempRecord[PAGE_SIZE];

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

#endif
