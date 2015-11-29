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
		char tableIDArr[1+sizeof(int)];
		char *tableIDPtr = tableIDArr;

		if(scan(string(TABLES_TABLE_NAME), attr, NO_OP, NULL, attributes, rmsiTable) != 0)
		{
			cerr << "Error occured while scanning!" << endl;
		}

		int currentLastTableID = -1;
		while(rmsiTable.getNextTuple(rid,tableIDPtr) != RM_EOF)
		{
			char *candidateTableIDPtr = tableIDPtr + 1;
			int candidateLastTableID = *((int*)candidateTableIDPtr);

			if(candidateLastTableID > currentLastTableID)
			{
				currentLastTableID = candidateLastTableID;
			}

		}
		rmsiTable.close();
		nextTableID = currentLastTableID + 1;
	}

}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{


	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	if((rbfm->createFile(string(COLUMNS_TABLE_NAME))) != 0)
	{
		cerr << "Couldnt create column file." << endl;
		return -1;
	}

	if((rbfm->createFile(string(TABLES_TABLE_NAME))) != 0)
	{
		cerr << "Couldnt create column file." << endl;
		return -1;
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
     |table id|table name|file name|flag|
     ----------------------------------------------------------------------
	 */

	vector<Attribute> attrs;
	getTTAttributeList(attrs);

	//Write table info here.
	admin = true;
	createTable(string(TABLES_TABLE_NAME), attrs);
	admin = false;

	vector<Attribute> attrs1;
	getCTAttributeList(attrs1);

	//Write table info here.
	admin = true;
	createTable(string(COLUMNS_TABLE_NAME),attrs1);
	admin = false;



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

	nextTableID = 0;
	return 0;
}


RC RelationManager::makeTTTuple(const string &tableName, void *tuple)
{
	char *buffer = (char*)tuple;

	int nameSize = tableName.length();
	int flagSize = 1;

	memset(buffer,0,1);//Null vector
	buffer = buffer + 1;

	memcpy(buffer,&nextTableID,sizeof(int));
	buffer = buffer + sizeof(int);

	memcpy(buffer, &nameSize, sizeof(int));
	buffer = buffer + sizeof(int);
	memcpy(buffer, tableName.c_str(), nameSize);
	buffer = buffer + nameSize;

	memcpy(buffer, &nameSize, sizeof(int));
	buffer = buffer + sizeof(int);
	memcpy(buffer, tableName.c_str(), nameSize);
	buffer = buffer + nameSize;

	memcpy(buffer, &flagSize, sizeof(int)); //flag len
	buffer = buffer + sizeof(int);
	char flag = 'U';   //flag
	flag = admin ? 'S' : 'U';
	memcpy(buffer, &flag, flagSize);
	return 0;
}

RC RelationManager::makeCTTuple(Attribute attr, unsigned attrNum, void *tuple)
{
	char *buffer = (char*)tuple;

	memset(buffer,0,1);//Null vector
	buffer = buffer + 1;

	//tableid
	memcpy(buffer,&nextTableID,sizeof(int));
	buffer = buffer + sizeof(int);

	//column-name
	int length = attr.name.length();
	memcpy(buffer,&length,sizeof(int));
	buffer  = buffer + sizeof(int);
	memcpy(buffer, attr.name.c_str(), length);
	buffer  = buffer + length;

	//type
	memcpy(buffer,&(attr.type), sizeof(AttrType));
	buffer  = buffer + sizeof(AttrType);

	//length
	memcpy(buffer,&(attr.length), sizeof(AttrLength));
	buffer = buffer + sizeof(AttrLength);

	//position
	int position = attrNum + 1; //Why +1?
	memcpy(buffer, &position, sizeof(int));

	return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	/*
	 *
    TABLE's TABLE FILE
    ----------------------------------------------------------------------
    |table id|table name|file name|flag
    ----------------------------------------------------------------------
	 */

	if(admin == false && (  (tableName.compare(string(COLUMNS_TABLE_NAME)) == 0) || (tableName.compare(string(TABLES_TABLE_NAME)) == 0))) {
		return -1; // user access prohibiited
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();


	//First make file for table (In case of table's table and column's table, the files are already made in create catalog
	if((tableName.compare(string(COLUMNS_TABLE_NAME)) != 0) && (tableName.compare(string(TABLES_TABLE_NAME)) != 0)) {
		if ((rbfm->createFile(tableName))) {
			cerr << "Couldnt create file." << endl;
			return 1;
		}
	}

	//Update Catalog for the new table

	/*****START OF PREPARING TABLE BUFFER******/

	char buffer[1000];
	makeTTTuple(tableName,buffer);

	/*****END OF PREPARING BUFFER******/

	RID rid;

	RC rc = -1;

	admin = true;
	rc = insertTuple(string(TABLES_TABLE_NAME),buffer, rid);
	admin = false;
	if(rc == -1)
		return -1;


	for(unsigned int i = 0; i < attrs.size(); i++)
	{

		/*****START OF PREPARING COLUMN BUFFER******/
		Attribute attr = attrs[i];

		makeCTTuple(attr, i, buffer);

		/*****END OF PREPARING BUFFER******/

		RID rid;
		admin = true;
		rc = insertTuple(string(COLUMNS_TABLE_NAME),buffer, rid);
		admin = false;

	}


	nextTableID++;

	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	//User access cut
	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}


	//Remove table's table info from table file and also remove table's column info from column file
	RID rid;
	int tableID = getTableIDWithRid(tableName,rid);
	//User access cut
	if(admin == false && (tableID == 0 || tableID == 1))
	{
		return -1;
	}
	admin = true;
	deleteTuple(string(TABLES_TABLE_NAME), rid);
	admin = false;


	RM_ScanIterator rmsiColumn;
	string attr = "table-id";
	string returnattr = "column-name";
	vector<string> attributes;
	attributes.push_back(returnattr);
	char rData[1000];
	if(scan(string(COLUMNS_TABLE_NAME), attr, EQ_OP, &tableID, attributes, rmsiColumn) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return -1;
	}

	vector<string> attributeNames; //contains attibute name

	while(rmsiColumn.getNextTuple(rid,rData) != RM_EOF)
	{
		char *varCharSizePtr = rData + 1;//Null indicator
		int varCharSize = *((int*)varCharSizePtr);
		char *varCharPtr = varCharSizePtr + sizeof(int);
		attributeNames.push_back(string(varCharPtr,varCharSize));
		admin = true;
		deleteTuple(string(COLUMNS_TABLE_NAME), rid);
		admin = false;
	}
	rmsiColumn.close();

	admin = true;
	if(rbfm->destroyFile(tableName) == -1)
	{
		cerr << "Error occured while destroying file!" << endl;
		return -1;
	}
	admin = false;

	for(unsigned i = 0 ; i < attributeNames.size() ; i++)
	{

		string ix_tableName = IX_INDICATOR + attributeNames[i]+ "_" + tableName;
		if(rbfm->destroyFile(ix_tableName) == 0)
		{
			//Remove table's table info from table file
			RID rid;
			int tableID = getTableIDWithRid(ix_tableName,rid);
			admin = true;
			deleteTuple(string(TABLES_TABLE_NAME), rid);
			admin = false;
		}
	}

	return 0;
}


int RelationManager::getTableIDWithRid(const string &tableName, RID &rid)
{

	RM_ScanIterator rmsiTable;
	vector<string> attributes;
	string attr1 = "table-name";
	string returnattr = "table-id";
	attributes.push_back(returnattr);
	// returnattr = "version";
	// attributes.push_back(returnattr);
	char returnedData[1 + sizeof(int)];

	int size = tableName.length();
	char s[sizeof(int) + size];
	char *str = s;
	memcpy(str, &size, sizeof(int));
	memcpy(str + sizeof(int), tableName.c_str(), size);

	if(scan(string(TABLES_TABLE_NAME), attr1, EQ_OP, str, attributes, rmsiTable) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return -1;
	}

	if(rmsiTable.getNextTuple(rid, returnedData) == RM_EOF)
	{
		//cerr << "Error occured while getting next tuple!" << endl;
		return -1;
	}
	rmsiTable.close();
	char *tableIDPtr = (char *) returnedData + 1; //nullindi + table ID
	return *((int*)tableIDPtr);
}


RC RelationManager::getTTAttributeList(vector<Attribute> &attrs)
{
	Attribute attr;
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
	return 0;

}

RC RelationManager::getCTAttributeList(vector<Attribute> &attrs)
{
	Attribute attr;
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
	return 0;

}


RC RelationManager::getAttributeList(unsigned tableID,vector<Attribute> &attrs)
{

	RID rid;
	RM_ScanIterator rmsiColumn;
	vector<string> attributes;
	string attr1 = "table-id";
	string returnattr = "column-name";
	attributes.push_back(returnattr);
	returnattr = "column-type";
	attributes.push_back(returnattr);
	returnattr = "column-length";
	attributes.push_back(returnattr);

	if(scan(string(COLUMNS_TABLE_NAME), attr1, EQ_OP, &tableID, attributes, rmsiColumn) != 0)
	{
		cerr << "Error occured while scanning!" << endl;
		return -1;
	}

	char returnedD[1000];
	//FileHandle fileHandle;
	while(rmsiColumn.getNextTuple(rid, returnedD) != RM_EOF)
	{
		char *data = (char *) returnedD + 1; //skip null indicator

		Attribute attr;

		int nameLength = *((int *) data);
		data = data + sizeof(int);

		attr.name = string(data, nameLength);
		data = data + nameLength;

		attr.type = *((AttrType*) data);
		data = data + sizeof(AttrType);

		attr.length = *((AttrLength*) data);
		attrs.push_back(attr);
	}
	rmsiColumn.close();
	return 0;
}


RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	Attribute attr;
	RC rc = -1;
	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{

		rc = getTTAttributeList(attrs);
	}
	else if(tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	{

		rc = getCTAttributeList(attrs);
	}
	else
	{
		RID rid;
		int tableID = getTableIDWithRid(tableName,rid);
		if(tableID == -1)
			return -1; // No table with tableName
		getAttributeList(tableID,attrs);
		if(attrs.size() == 0)
			return -1; //No Attribute related
		rc = 0;
	}

	return rc;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	IndexManager *ixm = IndexManager :: instance();
	RC rc = 0;

	vector<Attribute> attrVector;


	//if(tableName.compare(string(TABLES_TABLE_NAME)) == 0 || tableName.compare(string(COLUMNS_TABLE_NAME)) == 0)
	//{
	rc = getAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed


	//User access cut
	if(admin == false && (tableName == string(TABLES_TABLE_NAME) || tableName == string(COLUMNS_TABLE_NAME)))
	{
		return -1;
	}


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


		unsigned numberOfBytesForNullIndicator = ceil((float)attrVector.size()/8);
		unsigned fieldOffset = numberOfBytesForNullIndicator;

		//Index update
		for(unsigned i = 0 ; i < attrVector.size() ; i++)
		{
			if(rbfm->isNullField(data,i))
			{
				continue;
			}
			string ixFileName = IX_INDICATOR + attrVector[i].name + "_" + tableName;
			IXFileHandle ixFileHandle;
			rc = ixm->openFile(ixFileName,ixFileHandle);

			//Index File exist (Should I change it to scan to search the file?)
			if(rc == 0)
			{
				char *key = (char*)data + fieldOffset;
				rc = ixm->insertEntry(ixFileHandle,attrVector[i],key,rid);
				if(rc == -1)
					return -1; //insert entry failed
			}

			//Next field
			if(attrVector[i].type == TypeInt)
				fieldOffset = fieldOffset + sizeof(int);
			else if(attrVector[i].type == TypeReal)
				fieldOffset = fieldOffset + sizeof(float);
			else if(attrVector[i].type == TypeVarChar)
			{
				char *sizePtr = (char*)data + fieldOffset;
				int size = *((int*)sizePtr);
				fieldOffset = fieldOffset + sizeof(int) + size;
			}

		}

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
	IndexManager *ixm = IndexManager :: instance();

	vector<Attribute> attrVector;

	RC rc = getAttributes(tableName, attrVector);
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

	RC rc =getAttributes(tableName, attrVector);
	if(rc != 0)
		return -1;//get attribute info failed


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
	rc = getAttributes(tableName, attrVector);
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
	RC rc = getAttributes(tableName, attrVector);
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

	vector<Attribute> attrs;

	rc = getAttributes(tableName, attrs);
	if(rc != 0)
		return -1;//get attribute info failed

	if(tableName.compare(string(TABLES_TABLE_NAME)) == 0)
	{
		rc = rbfm->scan(tabFileHandle,attrs, conditionAttribute,
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

	RC rc = 0;
	IndexManager *ixm = IndexManager :: instance();
	//preparing for rbfm for index
	RM_ScanIterator rmsi;
	vector<string> projAttr;
	projAttr.push_back(attributeName);
	rc = scan(tableName,"",NO_OP, NULL, projAttr, rmsi);
	if(rc == -1)
		return rc;

	//get information about attribute for index
	Attribute keyAttr;
	keyAttr.type = rmsi.rbfm_scanIterator.extractedDataDescriptor[0].type; // projected attribute is just one
	keyAttr.name = attributeName;
	if(keyAttr.type == TypeInt || keyAttr.type == TypeReal)
		keyAttr.length = sizeof(int);
	else
		keyAttr.length = 1000;


	string ix_tableName = IX_INDICATOR + attributeName + "_" + tableName;
	//Create index file
	if ((ixm->createFile(ix_tableName))) {
		cerr << "Couldnt create file." << endl;
		return -1;
	}

	//Update T_T for the index file
	char buffer[1000];
	makeTTTuple(ix_tableName,buffer);
	RID rid;
	admin = true;
	rc = insertTuple(string(TABLES_TABLE_NAME),buffer, rid);
	admin = false;
	nextTableID++;

	//Update index file
	IXFileHandle ixFileHandle;
	rc = ixm->openFile(ix_tableName,ixFileHandle);
	if(rc == -1)
		return rc;
	char returnedData[PAGE_SIZE];
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		ixm->insertEntry(ixFileHandle,keyAttr,returnedData,rid);
	}

	rmsi.close();
	ixm->closeFile(ixFileHandle);
	return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	IndexManager *ixm = IndexManager :: instance();
	RC rc = -1;
	string ix_tableName = IX_INDICATOR + attributeName + "_" + tableName;

	//Remove table's table info from table file
	RID rid;
	int tableID = -1;
	tableID = getTableIDWithRid(ix_tableName,rid);
	if(tableID == -1)
		return -1;//No index with this table

	admin = true;
	rc = deleteTuple(string(TABLES_TABLE_NAME), rid);
	admin = false;

	if(ixm->destroyFile(ix_tableName) != 0)
	{
		cerr << "Error occured while destroying file!" << endl;
		return -1;
	}

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
	string ix_tableName = IX_INDICATOR + attributeName + "_" + tableName;
	RC rc = 0;
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
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
		rc = ix->openFile(ix_tableName,rm_IndexScanIterator.ixFileHandle);
		rc = ix->scan(rm_IndexScanIterator.ixFileHandle, attrs[targetAttr],
				lowKey,highKey,lowKeyInclusive,highKeyInclusive,rm_IndexScanIterator.ix_scanIterator);
	}
	return rc;
}
