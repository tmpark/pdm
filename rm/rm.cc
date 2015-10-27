#include <iostream>
#include <bitset>
#include <cmath>
#include "rm.h"
#include "string.h"
#include "../rbf/rbfm.h"


RelationManager* RelationManager::_rm = 0;
int RelationManager:: tableID = 1;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{

    admin = true;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

/*
    if(!(rbfm->createFile(TABLES_TABLE_NAME)))
    {
        cerr << "Couldnt create table file." << endl;
        return 1;
    }
*/

    if((rbfm->createFile(COLUMNS_TABLE_NAME)))
    {
        cerr << "Couldnt create column file." << endl;
        return 2;
    }

/*
    FileHandle tabFileHandle;
    if(!(rbfm->openFile(TABLES_TABLE_NAME,tabFileHandle)))
    {
        cerr << "Couldnt open table file." << endl;
        return 3;
    }
*/


    //Create table's table info here

    /*
   *
     TABLE's TABLE FILE
     ----------------------------------------------------------------------
     |flag|version|table id|table name|file name
     ----------------------------------------------------------------------
   */

    Attribute attr;
    vector<Attribute> attrs;

    attr.name = "flag";
    attr.type = TypeVarChar;
    attr.length = 1;
    attrs.push_back(attr);

    attr.name = "version";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    attrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    attrs.push_back(attr);



    //Write table info here.
    createTable(TABLES_TABLE_NAME, attrs);

/*
    FileHandle catFileHandle;
    if(!(rbfm->openFile(COLUMNS_TABLE_NAME,tabFileHandle)))
    {
        cerr << "Couldnt open column file." << endl;
        return 4;
    }
*/
    //Create catalog's table info here
    Attribute attr1;
    vector<Attribute> attrs1;

    attr1.name = "version";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);

 /*   attr1.name = "version_deleted";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);*/

    attr1.name = "table-id";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);


    attr1.name = "column-name";
    attr1.type = TypeVarChar;
    attr1.length = 50;
    attrs1.push_back(attr1);
    //attrs1.push_back(attr);

    attr1.name = "column-type";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);

    attr1.name = "column-length";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);

    attr1.name = "column-position";
    attr1.type = TypeInt;
    attr1.length = 4;
    attrs1.push_back(attr1);
    //Write table info here.
    createTable(COLUMNS_TABLE_NAME,attrs1);

    /***DELETE THINGS***/

    //delete rbfm;
    admin = false;

    return 0;
}

RC RelationManager::deleteCatalog()
{

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    return rbfm->destroyFile(TABLES_TABLE_NAME) || rbfm->destroyFile(COLUMNS_TABLE_NAME);
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    /*
  *
    TABLE's TABLE FILE
    ----------------------------------------------------------------------
    |flag|version|table id|table name|file name
    ----------------------------------------------------------------------
  */

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if(tableName.compare(COLUMNS_TABLE_NAME) != 0) {
        if ((rbfm->createFile(tableName))) {
            cerr << "Couldnt create file." << endl;
            return 1;
        }
    }

    FileHandle tabFileHandle;
    if((rbfm->openFile(TABLES_TABLE_NAME,tabFileHandle)))
    {
        cerr << "Couldnt open table file." << endl;
        return 2;
    }

    FileHandle colFileHandle;
    if((rbfm->openFile(COLUMNS_TABLE_NAME,colFileHandle)))
    {

        cerr << "Couldnt open column file." << endl;
        return 3;
    }

    /*****START OF PREPARING TABLE BUFFER******/

    char *buffer = (char *) malloc(5*50);
    char *buffer1 = buffer;

    memset(buffer,0,1);
    buffer += 1;

    int length = 1;
    memcpy(buffer, &length, 4);
    buffer += 4;
    char flag = 'U';   //means user
    flag = admin ? 'S' : 'U';
    memcpy(buffer, &flag, 1);
    buffer += 1;

    int version = 0;   //first version
    memcpy(buffer, &version, 4);
    buffer += 4;

    memcpy(buffer,&tableID,4);
    buffer += 4;

    int nameSize = tableName.length();
    cout<<nameSize<<endl;

    memcpy(buffer, &nameSize, 4);
    buffer += 4;
    memcpy(buffer, tableName.c_str(), nameSize);
    buffer += nameSize;

    memcpy(buffer, &nameSize, 4);
    buffer += 4;
    memcpy(buffer, tableName.c_str(), nameSize);
    buffer += nameSize;


    /*****END OF PREPARING BUFFER******/


    /****TESTING****/
    //rbfm->printRecord(attrs,buffer1);


    //FIXME
    //RID rid;
    //insertTuple(TABLES_TABLE_NAME,buffer, rid);




    for(int i = 0; i < attrs.size(); i++)
    {

        /*****START OF PREPARING COLUMN BUFFER******/

        buffer = buffer1;
        memset(buffer,0,5*50);

        memset(buffer, 0, 1);
        buffer += 1;
        //version_added
        memcpy(buffer, &version, 4);
        buffer += 4;
        //version_deleted
      /*  version = -1;
        memcpy(buffer, &version, 4);
        version = 0;
        buffer += 4;*/

        //tableid
        memcpy(buffer,&tableID,4);
        buffer += 4;

        Attribute attr = attrs[i];
        //column-name
        int length = attr.name.length();
        memcpy(buffer,&length,4);
        buffer += 4;

        char name[length];
        strcpy(name,attr.name.c_str());
        memcpy(buffer, name, length);
        buffer += length;

        //FIXME: ask following line
        //type
        memcpy(buffer,&(attr.type), 4);
        buffer += 4;
        //length
        memcpy(buffer,&(attr.length), 4);
        buffer += 4;
        //position
        int position = i + 1;
        memcpy(buffer, &position, 4);
        buffer += 4;



        //FIXME: Do we need to add something else to do column?

        /*****END OF PREPARING BUFFER******/


        //FIXME
        //RID rid;
        //insertTuple(COLUMNS_TABLE_NAME,buffer, rid);


        /****TESTING****/

        Attribute attr1;
        vector<Attribute> attrs1;

        attr1.name = "version_added";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);

       /* attr1.name = "version_deleted";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);*/

        attr1.name = "table-id";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);


        attr1.name = "column-name";
        attr1.type = TypeVarChar;
        attr1.length = attr.name.length();
        attrs1.push_back(attr1);
        //attrs1.push_back(attr);

        attr1.name = "column-type";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);

        attr1.name = "column-length";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);

        attr1.name = "column-position";
        attr1.type = TypeInt;
        attr1.length = 4;
        attrs1.push_back(attr1);

        rbfm->printRecord(attrs1,buffer1);
    }


    tableID++;
    free(buffer1);


    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    //Remove table's table info from table file and also remove table's column info from column file
    //..
    //..
    //..

    RID rid;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr = "table-name";
    string returnattr = "table-id";
    attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    deleteTuple(TABLES_TABLE_NAME, rid);


    RM_ScanIterator rmsiColumn;
    attr = "table-id";
    returnattr = "column-name";
    int* TableIDPtr = (int *) (((char *) returnedData) + 1);
    attributes.clear();
    attributes.push_back(returnattr);
    char rData[50];

    if(scan(COLUMNS_TABLE_NAME, attr, EQ_OP, TableIDPtr, attributes, rmsiColumn))
    {
        cerr << "Error occured while scanning!" << endl;
        return 3;
    }

    //for debugging purpose column names can be printed out.
    int x = 1;
    while(rmsiColumn.getNextTuple(rid,rData) != RM_EOF)
    {
        deleteTuple(COLUMNS_TABLE_NAME, rid);
        x++;
    }
    cout  << x << endl;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if(rbfm->destroyFile(tableName))
    {
        cerr << "Error occured while destroying file!" << endl;
        return 4;
    }

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RID ridt;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr1 = "table-name";
    string returnattr = "table-id";
    attributes.push_back(returnattr);
    // returnattr = "version";
    // attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr1, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(ridt, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    char * tableNameID = (char *) returnedData;
    tableNameID++;

    RID rid;
    RM_ScanIterator rmsiColumn;
    attributes.clear();
    attr1 = "table-id";
    returnattr = "column-name";
    attributes.push_back(returnattr);
    returnattr = "column-type";
    attributes.push_back(returnattr);
    returnattr = "column-length";
    attributes.push_back(returnattr);
    // returnattr = "version";
    // attributes.push_back(returnattr);
    char returnedD[150];

    if(scan(COLUMNS_TABLE_NAME, attr1, EQ_OP, tableNameID, attributes, rmsiColumn))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    while(rmsiTable.getNextTuple(ridt, returnedD) != RM_EOF)
    {
        char *data = (char *) returnedD;
        data++;
        int * nameLength = (int *) data;
        Attribute a;
        data += 4;
        a.name = string(data, *nameLength);
        data += *nameLength;
        a.type = *((AttrType *) data);
        cout << "type: " << a.type << endl;
        data += 4;
        a.length = *((int *) data);
        attrs.push_back(a);

    }

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> attrVector;
    Attribute at;
    at.name = "version";
    at.type = TypeInt;
    at.length = 4;
    attrVector.push_back(at);

    getAttributes(tableName, attrVector);
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle filehandle;

    double size = attrVector.size() - 1;
    int nullInd = ceil(size/8);


    char tempData[attrVector.size()*50];
    char *tempDataPtr = tempData;

    RID ridt;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr = "table-name";
    string returnattr = "version";
    attributes.push_back(returnattr);
    // returnattr = "version";
    // attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    char * rData = returnedData;
    int *versionPtr = (int *)(rData + 1);


    if(attrVector.size()%8 == 0)
    {
        uint8_t c = 0;
        memcpy(tempDataPtr, data, nullInd);
        memcpy(tempDataPtr + nullInd,&c,1);
        long long nullNum = 0;
        memcpy(&nullNum, tempDataPtr, nullInd + 1);
        bitset<64> nullBits<nullNum>;
        nullBits>>=1;
        nullNum = nullBits.to_ullong();
        memcpy(tempDataPtr, &nullNum, nullInd + 1);
        memcpy(tempDataPtr + nullInd + 1, versionPtr, 4);
        memcpy(tempDataPtr + nullInd + 5, data + nullInd, attrVector.size()*50 - nullInd - 1 - 4);
    }
    else
    {
        long long nullNum = 0;
        memcpy(&nullNum, data, nullInd);
        bitset<64> nullBits<nullNum>;
        nullBits >>= 1;
        nullNum = nullBits.to_ullong();
        memcpy(tempDataPtr, &nullNum, nullInd);
        memcpy(tempDataPtr + nullInd, versionPtr, 4);
        memcpy(tempDataPtr + nullInd + 4, data + nullInd, attrVector.size()*50 - nullInd - 4);
    }

    rbfm->insertRecord(filehandle, attrVector, tempData, rid);

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    vector<Attribute> attrVector;
    Attribute at;
    at.name = "version";
    at.type = TypeInt;
    at.length = 4;
    attrVector.push_back(at);

    getAttributes(tableName, attrVector);
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle filehandle;
    rbfm->deleteRecord(filehandle, attrVector, rid);

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    vector<Attribute> attrVector;
    Attribute at;
    at.name = "version";
    at.type = TypeInt;
    at.length = 4;
    attrVector.push_back(at);

    getAttributes(tableName, attrVector);
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle filehandle;

    double size = attrVector.size() - 1;
    int nullInd = ceil(size/8);


    char tempData[attrVector.size()*50];
    char *tempDataPtr = tempData;

    RID ridt;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr = "table-name";
    string returnattr = "version";
    attributes.push_back(returnattr);
    // returnattr = "version";
    // attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(ridt, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    char * rData = returnedData;
    int *versionPtr = (int *)(rData + 1);


    if(attrVector.size()%8 == 0)
    {
        uint8_t c = 0;
        memcpy(tempDataPtr, data, nullInd);
        memcpy(tempDataPtr + nullInd,&c,1);
        long long nullNum = 0;
        memcpy(&nullNum, tempDataPtr, nullInd + 1);
        bitset<64> nullBits<nullNum>;
        nullBits>>=1;
        nullNum = nullBits.to_ullong();
        memcpy(tempDataPtr, &nullNum, nullInd + 1);
        memcpy(tempDataPtr + nullInd + 1, versionPtr, 4);
        memcpy(tempDataPtr + nullInd + 5, data + nullInd, attrVector.size()*50 - nullInd - 1 - 4);
    }
    else
    {
        long long nullNum = 0;
        memcpy(&nullNum, data, nullInd);
        bitset<64> nullBits<nullNum>;
        nullBits >>= 1;
        nullNum = nullBits.to_ullong();
        memcpy(tempDataPtr, &nullNum, nullInd);
        memcpy(tempDataPtr + nullInd, versionPtr, 4);
        memcpy(tempDataPtr + nullInd + 4, data + nullInd, attrVector.size()*50 - nullInd - 4);
    }

    rbfm->updateRecord(filehandle, attrVector, tempData, rid);

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> attrVector;
    Attribute at;
    at.name = "version";
    at.type = TypeInt;
    at.length = 4;
    attrVector.push_back(at);

    getAttributes(tableName, attrVector);
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle  fileHandle;
    void * tempData = malloc(attrVector.size()*50);
    rbfm->readRecord(fileHandle,attrVector, rid, tempData);

    double size = attrVector.size();
    int nullInd = ceil(size/8);
    long long nullNum = 0;
    memcpy(&nullNum, tempData, nullInd);

    bitset<64> bitset(nullNum);
    bitset<<=1;
    long long nullBits = bitset.to_ullong();

    int x = 0;
    if(attrVector.size()%8 == 1)
    {
        x = nullInd - 1;
    }

    //write null byte info
    memcpy(data,tempData, x);
    char *charTempData = (char *) tempData;
    char *charData = (char *) data;
    charTempData += nullInd + 4;
    charData += x;
    memcpy(data, tempData, attrVector.size()*50 - nullInd - 4);

    return -0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> attrVector;
    Attribute at;
    at.name = "version";
    at.type = TypeInt;
    at.length = 4;
    attrVector.push_back(at);

    getAttributes(tableName, attrVector);
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle filehandle;

   return rbfm->readAttribute(filehandle,attrVector,rid, attributeName, data);

}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{


    RID rid;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr = "table-name";
    string returnattr = "table-id";
    attributes.push_back(returnattr);
   // returnattr = "version";
   // attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    char data[5*50];
    readTuple(TABLES_TABLE_NAME, rid,data);
    //Update Version
    int *versionPtr = (int *) (data + 2);
    int prevVersion = *versionPtr;
    (*versionPtr)++;
    updateTuple(TABLES_TABLE_NAME, data,rid);

    RM_ScanIterator rmsiColumn;
    attr = "table-id";
    returnattr = "version";
    int* tableIDPtr = (int *) (((char *) returnedData) + 1);
    //int* tableVersionPtr = tableIDPtr + 1;
    attributes.clear();
    attributes.push_back(returnattr);
    //returnattr = "column-name";
    //attributes.push_back(returnattr);
    //attributes.push_back(attributeName);
    char Data[50];

    if(scan(COLUMNS_TABLE_NAME, attr, EQ_OP, tableIDPtr, attributes, rmsiColumn))
    {
        cerr << "Error occured while scanning!" << endl;
        return 3;
    }


    while(rmsiColumn.getNextTuple(rid,Data) != RM_EOF) {
        int *rData = (int *) (Data + 1);
        if (prevVersion == *rData) {
            char tupl[50 * 5];
            char *tuple = tupl;
            readTuple(COLUMNS_TABLE_NAME, rid, tuple);
            tuple++;//Pass null byte
            int *v = (int *) tuple;//Version number;
            *v = (*v) + 1;//Updated version number
            v++;//Pointing to table-id
            v++;//Pointing to the length of column-name
            int columnSize = (*v) + 1;
            char columnName[columnSize];
            v++;//Pointing to the column name
            char *columnPtr = (char *) v;
            strncpy(columnName, columnPtr, columnSize);

            if (attributeName.compare(columnName) == 0) {
                columnPtr += columnSize - 1;
                int *columnType = (int *) columnPtr;
                *columnType = TypeDeleted;
            }

            insertTuple(COLUMNS_TABLE_NAME, Data, rid);
            cout << "InsertedPage:" << rid.pageNum << "  " << "InsertedSlot:" << rid.slotNum << endl;

        }
    }

/*        int *rData = (int *) (Data + 1);
        int version = *rData;
        int* columnNameLengthPtr = rData + 1;
        char columnName[(*columnNameLengthPtr) + 1];
        char *string = (char *) columnNameLengthPtr;
        strncpy(columnName, (string + 4), *columnNameLengthPtr);

        if(tableName.compare(columnName))
        {
            char buffer[5*50];
            readTuple(COLUMNS_TABLE_NAME, rid, buffer);
            buffer = buffer + 5;  //1 for null and 4 for version_added
            cout << "version_deleted was:" << *((int *) buffer) << endl;
            int *versionD = (int *) buffer;
            *versionD = version;//set to tables version.
            cout << "version_deleted is now:" << version << endl;
            updateTuple(COLUMNS_TABLE_NAME,(buffer - 5),rid);
            return 0;
        }
        x++;
    }
    cout  << x << endl;
*/
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    //Copy the drop attribute code here
    //Use the following code snippet to convert attr to record with null info
    //Then the rest is sikine gore

    RID rid;
    RM_ScanIterator rmsiTable;
    vector<string> attributes;
    string attr1 = "table-name";
    string returnattr = "table-id";
    attributes.push_back(returnattr);
    // returnattr = "version";
    // attributes.push_back(returnattr);
    char returnedData[16];

    if(scan(TABLES_TABLE_NAME, attr1, EQ_OP, &tableName, attributes, rmsiTable))
    {
        cerr << "Error occured while scanning!" << endl;
        return 1;
    }

    if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
    {
        cerr << "Error occured while getting next tuple!" << endl;
        return 2;
    }

    char data[5*50];
    readTuple(TABLES_TABLE_NAME, rid,data);
    //Update Version
    int *versionPtr = (int *) (data + 2);
    int prevVersion = *versionPtr;
    (*versionPtr)++;
    updateTuple(TABLES_TABLE_NAME, data,rid);

    RM_ScanIterator rmsiColumn;
    attr1 = "table-id";
    returnattr = "version";
    int* tableIDPtr = (int *) (((char *) returnedData) + 1);
    //int* tableVersionPtr = tableIDPtr + 1;
    attributes.clear();
    attributes.push_back(returnattr);
    //returnattr = "column-name";
    //attributes.push_back(returnattr);
    //attributes.push_back(attributeName);



    char Data[50];
    if(scan(COLUMNS_TABLE_NAME, attr1, EQ_OP, tableIDPtr, attributes, rmsiColumn))
    {
        cerr << "Error occured while scanning!" << endl;
        return 3;
    }

    int i = 0;
    while(rmsiColumn.getNextTuple(rid,Data) != RM_EOF) {
        int *rData = (int *) (Data + 1);
        if (prevVersion == *rData) {
            char tupl[50 * 5];
            char *tuple = tupl;
            readTuple(COLUMNS_TABLE_NAME, rid, tuple);
            tuple++;//Pass null byte
            int *v = (int *) tuple;//Version number;
            *v = (*v) + 1;//Updated version number
            i++;
            insertTuple(COLUMNS_TABLE_NAME, Data, rid);
            cout << "InsertedPage:" << rid.pageNum << "  " << "InsertedSlot:" << rid.slotNum << endl;
        }
    }

    /*****START OF PREPARING COLUMN BUFFER******/
    char buffer2[5*50];
    char* buffer = buffer2;
    char *buffer1 = buffer;

    memset(buffer, 0, 1);
    buffer += 1;
    //version_added
    memcpy(buffer, versionPtr, 4);
    buffer += 4;
    //version_deleted
    /*  version = -1;
      memcpy(buffer, &version, 4);
      version = 0;
      buffer += 4;*/

    //tableid
    memcpy(buffer,tableIDPtr,4);
    buffer += 4;

    //Attribute attr = attrs[i];
    //column-name

    int length = attr.name.length();
    memcpy(buffer,&length,4);
    buffer += 4;

    char name[length];
    strcpy(name,attr.name.c_str());
    memcpy(buffer, name, length);
    buffer += length;

    //FIXME: ask following line
    //type
    memcpy(buffer,&(attr.type), 4);
    buffer += 4;
    //length
    memcpy(buffer,&(attr.length), 4);
    buffer += 4;
    //position
    int position = i + 1;
    memcpy(buffer, &position, 4);
    buffer += 4;

    insertTuple(COLUMNS_TABLE_NAME, buffer, rid);
    cout << "InsertedPage:" << rid.pageNum << "  " << "InsertedSlot:" << rid.slotNum << endl;

    //FIXME: Do we need to add something else to do column?

    /*****END OF PREPARING BUFFER******/


    return -1;
}



