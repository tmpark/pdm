
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define TABLES_TABLE_NAME "Tables"
#define COLUMNS_TABLE_NAME "Columns"
#define IX_INDICATOR "_ix_"



// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {catalogFile = false;};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data){return rbfm_scanIterator.getNextRecord(rid,data);};
  RC close(){
	  RC rc = -1;
	  rc = rbfm_scanIterator.close();
	  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	  if(!catalogFile)
		  rbfm->closeFile(fileHandle);

	  return rc;
  };

  RBFM_ScanIterator rbfm_scanIterator;
  FileHandle fileHandle;
  bool catalogFile;
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key) {return ix_scanIterator.getNextEntry(rid,key);};  	// Get next matching entry
  RC close() {
	  RC rc = -1;
      rc = ix_scanIterator.close();
      IndexManager *ixm = IndexManager :: instance();
      ixm->closeFile(ixFileHandle);
      return rc;
  };  // Terminate index scan

  IX_ScanIterator ix_scanIterator;
  IXFileHandle ixFileHandle;

};



// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);


  int getTableIDWithRid(const string &tableName, RID &rid);
  RC getAttributeList(unsigned tableID,vector<Attribute> &attrs);
  RC getTTAttributeList(vector<Attribute> &attrs);
  RC getCTAttributeList(vector<Attribute> &attrs);
  RC makeTTTuple(const string &tableName, void *tuple);
  RC makeCTTuple(Attribute attr, unsigned attrNum, void *tuple);



// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
    int nextTableID;
    bool admin;
    bool comingFromCreateTable;
    FileHandle tabFileHandle;
    FileHandle colFileHandle;
};

#endif

