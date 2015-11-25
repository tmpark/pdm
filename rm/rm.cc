#include <iostream>
#include <bitset>
#include <cmath>
#include <cstdlib>
#include "rm.h"
#include "string.h"
#include "../rbf/rbfm.h"


RelationManager* RelationManager::_rm = 0;


RelationManager* RelationManager::instance()
{
	if(!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager()
{
	admin = false;
	comingFromCreateTable = false;
	nextTableID = 0;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	RC rc = rbfm->openFile(string(TABLES_TABLE_NAME),tabFileHandle);
	rbfm->openFile(string(COLUMNS_TABLE_NAME),colFileHandle);
	if(rc == 0)
	{
		RM_ScanIterator rmsiTable;
		vector<string> attributes;
		string attr = "";
		string returnattr = "table-id";
		attributes.push_back(returnattr);
		RID rid;
		char tableIDArr[5];
		char *tableIDPtr = tableIDArr;

		if(scan(string(TABLES_TABLE_NAME), attr, NO_OP, NULL, attributes, rmsiTable) != 0)
		{
			cerr << "Error occured while scanning!" << endl;
		}
		while(rmsiTable.getNextTuple(rid,tableIDPtr) != RM_EOF)
		{

		}
		tableIDPtr++;
		nextTableID = *((int*)tableIDPtr);
		nextTableID++;

	}

}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{


	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	if((rbfm->createFile(string(COLUMNS_TABLE_NAME))))
	{
		cerr << "Couldnt create column file." << endl;
		return 2;
	}

	if((rbfm->createFile(string(TABLES_TABLE_NAME))))
	{
		cerr << "Couldnt create column file." << endl;
		return 2;
	}

	if(tabFileHandle.getFileStream() == NULL)
	{
		rbfm->openFile(string(TABLES_TABLE_NAME),tabFileHandle);
	}

	if(colFileHandle.getFileStream() == NULL)
	{
		rbfm->openFile(string(COLUMNS_TABLE_NAME),colFileHandle);
	}


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

	attr.name = "flag";
	attr.type = TypeVarChar;
	attr.length = 1;
	attrs.push_back(attr);

	//Write table info here.

	admin = true;
	createTable(string(TABLES_TABLE_NAME), attrs);
	admin = false;


	Attribute attr1;
	vector<Attribute> attrs1;


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
	admin = true;
	createTable(string(COLUMNS_TABLE_NAME),attrs1);
	admin = false;


	/***DELETE THINGS***/

	//delete rbfm;


	return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	tabFileHandle.setFileStream(NULL);
	colFileHandle.setFileStream(NULL);
	admin = true;
	rbfm->destroyFile(string(TABLES_TABLE_NAME));
	rbfm->destroyFile(string(COLUMNS_TABLE_NAME));
	admin = false;
	return 0;
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

	if((tableName.compare(string(COLUMNS_TABLE_NAME)) != 0) && (tableName.compare(string(TABLES_TABLE_NAME)) != 0)) {
		if ((rbfm->createFile(tableName))) {
			cerr << "Couldnt create file." << endl;
			return 1;
		}
	}


	/*****START OF PREPARING TABLE BUFFER******/

	char *buffer1 = (char *) malloc(5*50);
	char *buffer = buffer1;

	memset(buffer,0,1);//Null vector
	buffer += 1;


/*
	int version = 0;   //first version
	memcpy(buffer, &version, 4);
	buffer += 4;
*/
	memcpy(buffer,&nextTableID,4);
	buffer += 4;

	int nameSize = tableName.length();
	//cout<<nameSize<<endl;

	memcpy(buffer, &nameSize, 4);
	buffer += 4;
	memcpy(buffer, tableName.c_str(), nameSize);
	buffer += nameSize;

	memcpy(buffer, &nameSize, 4);
	buffer += 4;
	memcpy(buffer, tableName.c_str(), nameSize);
	buffer += nameSize;

	int length = 1;
	memcpy(buffer, &length, 4); //flag len
	buffer += 4;

	char flag = 'U';   //flag
	flag = admin ? 'S' : 'U';
	memcpy(buffer, &flag, 1);
	buffer += 1;


	/*****END OF PREPARING BUFFER******/


	/****TESTING****/
	//rbfm->printRecord(attrs,buffer1);


	RC rc;
	//FIXME
	RID rid;

	admin = true;
	rc = insertTuple(string(TABLES_TABLE_NAME),buffer1, rid);
	admin = false;


	for(unsigned int i = 0; i < attrs.size(); i++)
	{

		/*****START OF PREPARING COLUMN BUFFER******/

		buffer = buffer1;
		memset(buffer,0,5*50);

		memset(buffer, 0, 1);
		buffer += 1;

		//tableid
		memcpy(buffer,&nextTableID,4);
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
		RID rid;
		admin = true;
		rc = insertTuple(string(COLUMNS_TABLE_NAME),buffer1, rid);
        admin = false;

		/****TESTING****/

		Attribute attr1;
		vector<Attribute> attrs1;


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

		//rbfm->printRecord(attrs1,buffer1);
	}


	nextTableID++;
	free(buffer1);


	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{

	//User access cut
	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}
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

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	int size = tableName.length();
	char s[size + 4];
	char * str = s;
	memcpy(str, &size, 4);
	memcpy(str + 4, tableName.c_str(), size);

	if(scan(string(TABLES_TABLE_NAME), attr, EQ_OP, str, attributes, rmsiTable) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return 1;
	}

	if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
	{
		//cerr << "Error occured while getting next tuple!" << endl;
		return 2;
	}

	int* TableIDPtr = (int *) (((char *) returnedData) + 1);
	rmsiTable.close();

	//User access cut
	if(admin == false && (*TableIDPtr == 0 || *TableIDPtr == 1))
	{
		return -1;
	}


	admin = true;
	deleteTuple(string(TABLES_TABLE_NAME), rid);
    admin = false;

	RM_ScanIterator rmsiColumn;
	attr = "table-id";
	returnattr = "column-name";
	attributes.clear();
	attributes.push_back(returnattr);
	char rData[50];




	if(scan(string(COLUMNS_TABLE_NAME), attr, EQ_OP, TableIDPtr, attributes, rmsiColumn) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return 3;
	}

	//for debugging purpose column names can be printed out.
	int x = 1;
	while(rmsiColumn.getNextTuple(rid,rData) != RM_EOF)
	{
		admin = true;
		deleteTuple(string(COLUMNS_TABLE_NAME), rid);
		admin = false;
		x++;
	}
	//cout  << x << endl;

	rmsiColumn.close();

	admin = true;
	if(rbfm->destroyFile(tableName))
	{
		cerr << "Error occured while destroying file!" << endl;
		return 4;
	}
	admin = false;

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

	int size = tableName.length();
	char s[size + 4];
	char * str = s;
	memcpy(str, &size, 4);
	memcpy(str + 4, tableName.c_str(), size);

	if(scan(string(TABLES_TABLE_NAME), attr1, EQ_OP, str, attributes, rmsiTable) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return 1;
	}

	if(rmsiTable.getNextTuple(ridt, returnedData) == RM_EOF)
	{
		//cerr << "Error occured while getting next tuple!" << endl;
		return 2;
	}


	char * tableNameID = (char *) returnedData; //nullindi + table ID
	tableNameID++;//next to null indicator

	//RID rid;
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

	if(scan(string(COLUMNS_TABLE_NAME), attr1, EQ_OP, tableNameID, attributes, rmsiColumn) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return 1;
	}

	//FileHandle fileHandle;
	while(rmsiColumn.getNextTuple(ridt, returnedD) != RM_EOF)
	{
		char *data = (char *) returnedD;
		data++;
		int * nameLength = (int *) data;
		Attribute a;
		data += 4;
		a.name = string(data, *nameLength);
		data += *nameLength;
		a.type = *((AttrType *) data);
		//cout << "type: " << a.type << endl;
		data += 4;
		a.length = *((int *) data);
		attrs.push_back(a);

	}

	rmsiTable.close();
	rmsiColumn.close();


	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = 0;

	vector<Attribute> attrVector;


	//if(tableName.compare(string(TABLES_TABLE_NAME)) == 0 || tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	//{
	rc = getSystemAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed


	//User access cut
	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}



	double size = attrVector.size() - 1;
	int nullInd = ceil(size/8);




	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{
		rc = rbfm->insertRecord(tabFileHandle, attrVector, data, rid);



	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{
		rc = rbfm->insertRecord(colFileHandle, attrVector, data, rid);
	}
	else
	{
		FileHandle fileHandle;
		rbfm->openFile(tableName,fileHandle);
		rbfm->insertRecord(fileHandle, attrVector, data, rid);
		rbfm->closeFile(fileHandle);
	}


	return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{

	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	vector<Attribute> attrVector;

	RC rc = getSystemAttributes(tableName, attrVector);
			if(rc != 0)
				return -1;//get attribute info failed


    if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
    {
        rbfm->deleteRecord(tabFileHandle, attrVector, rid);

    }
    else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
    {
        rbfm->deleteRecord(colFileHandle, attrVector, rid);
    }
    else
    {
        FileHandle fileHandle;
        rbfm->openFile(tableName,fileHandle);
        rbfm->deleteRecord(fileHandle, attrVector, rid);
        rbfm->closeFile(fileHandle);
    }
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{

	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrVector;

	RC rc =getSystemAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed


	double size = attrVector.size() - 1;
	int nullInd = ceil(size/8);




    if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
    {
        rbfm->updateRecord(tabFileHandle, attrVector, data, rid);

    }
    else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
    {
        rbfm->updateRecord(colFileHandle, attrVector, data, rid);
    }
    else
    {
        FileHandle fileHandle;
        rbfm->openFile(tableName,fileHandle);
        rbfm->updateRecord(fileHandle, attrVector, data, rid);
        rbfm->closeFile(fileHandle);
    }

	return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrVector;
	RC rc = -1;

	//if(tableName.compare(string(TABLES_TABLE_NAME)) == 0 || tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
		//{
	rc = getSystemAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed
		//}



    if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
    {
        rc = rbfm->readRecord(tabFileHandle, attrVector, rid, data);

    }
    else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
    {
        rc = rbfm->readRecord(colFileHandle, attrVector, rid, data);
    }
    else
    {
        FileHandle fileHandle;
        rbfm->openFile(tableName,fileHandle);
        rc = rbfm->readRecord(fileHandle, attrVector, rid, data);
        rbfm->closeFile(fileHandle);
    }


	return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	vector<Attribute> attrVector;

	//if(tableName.compare(string(TABLES_TABLE_NAME)) == 0 || tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
		//{
	RC rc = getSystemAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed


    if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{
         rbfm->readAttribute(tabFileHandle, attrVector,rid, attributeName, data);

	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{
         rbfm->readAttribute(colFileHandle, attrVector,rid, attributeName, data);
    }
	else
	{
	    FileHandle fileHandle;
        rbfm->openFile(tableName,fileHandle);
        rbfm->readAttribute(fileHandle, attrVector,rid, attributeName, data);
        rbfm->closeFile(fileHandle);
	}

    return 0;

}



RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute,
		const CompOp compOp,
		const void *value,
		const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager :: instance();

	RC rc = 0;
	Attribute attr;
	vector<Attribute> attrs;


	rc = getSystemAttributes(tableName, attrs);
	if(rc != 0)
		return -1;//get attribute info failed

	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{
		rc = rbfm->scan(tabFileHandle, attrs, conditionAttribute,
				compOp, value, attributeNames, rm_ScanIterator.rbfm_scanIterator);

	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{
		rc = rbfm->scan(colFileHandle, attrs, conditionAttribute,
				compOp, value, attributeNames, rm_ScanIterator.rbfm_scanIterator);
	}
	else
	{
		rc = rbfm->openFile(tableName,rm_ScanIterator.fileHandle);
		rc = rbfm->scan(rm_ScanIterator.fileHandle, attrs, conditionAttribute,
				compOp, value, attributeNames, rm_ScanIterator.rbfm_scanIterator);
	}
	return rc;
}

RC RelationManager::getSystemAttributes(const string &tableName, vector<Attribute> &attrs)
{
	Attribute attr;
	RC rc = -1;
	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{

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

		attr.name = "flag";
		attr.type = TypeVarChar;
		attr.length = 1;
		attrs.push_back(attr);

		rc = 0;
	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{


		attr.name = "table-id";
		attr.type = TypeInt;
		attr.length = 4;
		attrs.push_back(attr);


		attr.name = "column-name";
		attr.type = TypeVarChar;
		attr.length = 50;
		attrs.push_back(attr);
		//attrs1.push_back(attr);

		attr.name = "column-type";
		attr.type = TypeInt;
		attr.length = 4;
		attrs.push_back(attr);

		attr.name = "column-length";
		attr.type = TypeInt;
		attr.length = 4;
		attrs.push_back(attr);

		attr.name = "column-position";
		attr.type = TypeInt;
		attr.length = 4;
		attrs.push_back(attr);
		rc = 0;
	}
	else
	{
		rc = getAttributes(tableName, attrs);
	}



	return rc;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{

	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{


	return -1;
}



RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	vector<Attribute> attrs;
	Attribute attr;
	attr.length = PAGE_SIZE;
	attr.name = attributeName;
	attr.type = TypeVarChar;
	attrs.push_back(attr);

	RC rc = createTable(tableName, attrs);

	return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc  = deleteTable(tableName);
	return rc;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	IndexManager *ix = IndexManager :: instance();

	RC rc = 0;
	Attribute attr;
	vector<Attribute> attrs;
	rc = getSystemAttributes(tableName, attrs);
	if(rc != 0)
		return -1;//get attribute info failed

	int targetAttr = -1;
	for(unsigned i = 0 ; i < attrs.size() ; i ++)
	{
		if(attrs[i].name == attributeName)
		{
			targetAttr = i;
			break;
		}
	}
	if(targetAttr == -1)
		return -1; //This index is not for the attribute


	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{
		return -1;//Catalog file is not index

	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{
		return -1;//Catalog file is not index
	}
	else
	{
		rc = ix->openFile(tableName,rm_IndexScanIterator.ixFileHandle);
		rc = ix->scan(rm_IndexScanIterator.ixFileHandle, attrs[targetAttr],
				lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ix_scanIterator);
	}
	return rc;
}

