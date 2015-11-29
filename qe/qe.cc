
#include "qe.h"
#include <string>
#include <cmath> //added library
#include <cstdlib> //added library
#include <cstring> //added library
#include <cstdio> //added library
#include <iostream> //added library
#include <fstream> //added library

Filter::Filter(Iterator* input, const Condition &condition)
{
	it = input;
	cond = condition;
	it->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data)
{
	//	char tableName[WHOLE_SIZE_FOR_ENTRIES];
	//	char attrName[WHOLE_SIZE_FOR_ENTRIES];
	string tableName;
	string attrName;
	//	char tuple[WHOLE_SIZE_FOR_ENTRIES];

	split(cond.lhsAttr,tableName, attrName);
	//AttrType type = getType(attrs, attrName);
	AttrType type = cond.rhsValue.type;

	while (it->getNextTuple(data) == 0)
	{
		/*if(type == TypeVarChar)
		{
			string value;
			if(getValueOfAttr(data, attrs, s, value) != 0) return -1;
			if(cond.bRhsIsAttr == true) cout <<  "ERROR: Righthandside is attr" << endl; return -1;
			//FIXME: I think they assumed that they will not provide string in rhside
			if(compareValues(value, cond.rhsValue.data, cond.op)) return 0;
		}
		else*/ if(type == TypeInt)
		{
			int value;
			if(getValueOfAttr(data, attrs, attrName, value) != 0)
			{
				cout << "ERROR" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true) cout <<  "ERROR: Righthandside is attr" << endl; return -1;
			if(compareValues(value, *(int *)cond.rhsValue.data, cond.op)) return 0;
		}
		else if(type == TypeReal)
		{
			float value;
			if(getValueOfAttr(data, attrs, attrName, value) != 0) return -1;
			if(cond.bRhsIsAttr == true) cout <<  "ERROR: Righthandside is attr" << endl; return -1;
			if(compareValues(value, *(float *)cond.rhsValue.data, cond.op)) return 0;
		}

	}
	return 0;
}


//BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
//		TableScan *rightIn,           // TableScan Iterator of input S
//		const Condition &condition,   // Join condition
//		const unsigned numPages       // # of pages that can be loaded into memory,
//		//   i.e., memory block size (decided by the optimizer)
//)
//{
//	leftIt = leftIn;
//	rightIt = rightIn;
//	cond = condition;
//	this->numPages = numPages;
//	totalBufferSize = numPages*PAGE_SIZE;
//	//buffer = new char[totalBufferSize];
//
//	unsigned occupiedSpace = 0;
//	char tuple[WHOLE_SIZE_FOR_ENTRIES];
//	vector<Attribute> attrs;
//	leftIt->getAttributes(attrs);
//	while(occupiedSpace <= totalBufferSize)
//	{
//		if(leftIt->getNextTuple(tuple) != 0)
//		{
//			hasMore = false;
//			//return something or just return;
//		}
//		unsigned tupleSize = getSizeOfTuple(attrs, tuple);
//		//add tuple to buffer
//		char *buffer = new char[tupleSize];
//		memcpy(buffer + occupiedSpace, tuple, tupleSize);
//		bufferV.push_back((void *) buffer);
//		occupiedSpace = occupiedSpace + tupleSize;
//
//
//		string tableName;
//		string attrName;
//		split(cond.lhsAttr,tableName, attrName);
//		AttrType type = getType(attrs, attrName);
//
//		string key;
//		if(type == TypeVarChar)
//		{
//			getValueOfAttr(tuple, attrs, attrName, key);
//		}
//		else if(type == TypeInt)
//		{
//			int value = 0;
//			getValueOfAttr(tuple, attrs, attrName, value);
//			char valueChar[64];
//			itoa(value, valueChar, 10);
//			key = string(valueChar);
//		}
//		else if(type == TypeReal)
//		{
//			float value = 0;
//			getValueOfAttr(tuple, attrs, attrName, value);
//			char valueChar[64];
//			itoa(value, valueChar, 10);
//			key = string(valueChar);
//			//key = std::to_string(value);
//		}
//		else
//		{
//			cout << "ERROR" << endl;
//		}
//
//		auto got = tuplesMap.find(key);
//		if(got == tuplesMap.end())
//		{
//
//		}
//	}
//}
//
//BNLJoin::~BNLJoin()
//{
//	for(int i = 0; i < bufferV.size(); i++)
//	{
//		delete[] bufferV[i];
//	}
//}

////////////////////////////////////////////
///////////////HELPERS//////////////////////
////////////////////////////////////////////

unsigned Iterator::getSizeOfTuple(const vector<Attribute> &recordDescriptor,const void *exteriorRecord)
{
	unsigned numberOfFields = recordDescriptor.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)exteriorRecord;
	char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	bool nullExist = false;
	unsigned exteriorRecordOffset = 0;

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
			}
			else if (recordDescriptor[i].type == TypeInt)
			{
				exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
			}
			else if (recordDescriptor[i].type ==TypeReal)
			{
				exteriorRecordOffset = exteriorRecordOffset + recordDescriptor[i].length;
			}
		}

	}
	return exteriorRecordOffset + numberOfBytesForNullIndicator;
}

RC Iterator::getValueOfAttr(const void* data, vector<Attribute> &attrs, string &attrName, string &value)
{
	unsigned numberOfFields = attrs.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)data;
	//char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	bool nullExist = false;

	int exteriorRecordOffset = 0;
	exteriorRecordOffset = exteriorRecordOffset + numberOfBytesForNullIndicator;


	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));

		if(!nullExist)//Not Null Value
		{

			if(attrs[i].type == TypeVarChar)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field

				if(attrName.compare(attrs.at(i).name) == 0)
				{
					value = string((char *)data + exteriorRecordOffset, stringLength_int);
					return 0;
				}

				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
			}
			else if (attrs[i].type == TypeInt)
			{
				exteriorRecordOffset = exteriorRecordOffset + attrs[i].length;
			}
			else if (attrs[i].type ==TypeReal)
			{
				exteriorRecordOffset = exteriorRecordOffset + attrs[i].length;
			}
		}
		else
		{
			//blablalbalbla
			//return -1;
		}
	}
	return -1;
}

template <typename T>
RC Iterator::getValueOfAttr(const void* data, vector<Attribute> &attrs, string &attrName, T &value)
{
	unsigned numberOfFields = attrs.size();

	//exteriorRecord related
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)data;
	//char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;

	bool nullExist = false;

	int exteriorRecordOffset = 0;
	exteriorRecordOffset = exteriorRecordOffset + numberOfBytesForNullIndicator;


	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;
		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));

		if(!nullExist)//Not Null Value
		{
			if(attrs[i].type == TypeVarChar)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field
				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
			}
			else if (attrs[i].type == TypeInt)
			{
				if(attrName.compare(attrs.at(i).name) == 0)
				{
					value = *((T*) data);
					return 0;
				}
				exteriorRecordOffset = exteriorRecordOffset + attrs[i].length;
			}
			else if (attrs[i].type ==TypeReal)
			{
				if(attrName.compare(attrs.at(i).name) == 0)
				{
					value = *((T*) data);
					return 0;
				}
				exteriorRecordOffset = exteriorRecordOffset + attrs[i].length;
			}
		}
		else
		{
			//blablalbalbla
			//return -1;
		}
	}
	return -1;
}

AttrType Iterator::getType(vector<Attribute> &attrs, string &attrName)
{
	unsigned numberOfFields = attrs.size();

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		if(attrs[i].name.compare(attrName) == 0)
		{
			return attrs[i].type;
		}
	}

	cout << "ERROR" << endl;
	return TypeDeleted;
}

RC Iterator::split(const string &toBeSplitted, string &firstToken, string &secondToken)
{
	char c[WHOLE_SIZE_FOR_ENTRIES];
	memcpy(c, toBeSplitted.c_str(), toBeSplitted.length() + 1);
	firstToken = string(strtok(c, "."));
	secondToken = string(strtok(NULL, "."));
	//	while (p) {
	//	    printf ("Token: %s\n", p);
	//	    p = strtok(NULL, " ");
	//	}
	return 0;
}

template <typename T>
int Iterator::compareValues(T const valueExtracted, T const valueCompared, int compOp)
{
	switch(compOp)
	{
	case EQ_OP :
		return (valueExtracted == valueCompared);
		break;
	case LT_OP :
		return (valueExtracted < valueCompared);

		break;
	case LE_OP :
		return (valueExtracted <= valueCompared);

		break;
	case GT_OP :
		return (valueExtracted > valueCompared);

		break;
	case GE_OP :
		return (valueExtracted >= valueCompared);

		break;
	case NE_OP :
		return (valueExtracted != valueCompared);

		break;
	case NO_OP :
		return true;
		break;
	}
	return -1;
}
// ... the rest of your implementations go here



Project :: Project(Iterator *input,const vector<string> &attrNames)
{

	iter = input;
	vector <Attribute> attrsFromIter;
	iter->getAttributes(attrsFromIter);
	for(vector<string>::const_iterator it = attrNames.begin() ; it != attrNames.end() ; ++it)
	{
		for(unsigned i = 0 ; i < attrsFromIter.size() ; i++)
		{

			string attributeName;
			split(attrsFromIter[i].name, tableName, attributeName);

			//Projected Attribute construction
			if(attributeName.compare(*it) == 0)
			{
				//Projected Attr location info
				ExtractedAttr extractedAttr;
				extractedAttr.fieldNum = i;
				extractedAttr.type = attrsFromIter[i].type;
				extractedDataDescriptor.push_back(extractedAttr);

				//Projected Attr
				Attribute attr;
				attr = attrsFromIter[i];
				attrs.push_back(attr);
			}
		}
	}

}

Project :: ~Project()
{

}

RC Project ::getNextTuple(void *data) {

	char returnedData[PAGE_SIZE];
	RC rc = iter->getNextTuple(returnedData);
	if(rc == QE_EOF)
		return QE_EOF;

	rc = projectData(extractedDataDescriptor,returnedData,data);
	return rc;
}

RC Project::projectData(vector<ExtractedAttr> &extractedDataDescriptor, char *recordToRead, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	unsigned numberOfFields = extractedDataDescriptor.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)data;
	memset(nullsIndicator, 0, numberOfBytesForNullIndicator);

	char *recordField = (char*)data + numberOfBytesForNullIndicator;
	int recordFieldOffset = 0;

	for (unsigned i = 0 ;i < numberOfFields ; i++)
	{
		unsigned positionOfByte = floor((double)i / 8);
		unsigned positionOfNullIndicator = i % 8;

		int fieldSize = rbfm->getRecordFieldSize(recordToRead,extractedDataDescriptor[i].fieldNum);
		if(fieldSize != -1)
		{
			if(extractedDataDescriptor[i].type == TypeVarChar)
			{
				int stringSize = fieldSize;
				char *currentRecordField = recordField + recordFieldOffset;
				*((int*)currentRecordField) = stringSize;
				recordFieldOffset = recordFieldOffset + sizeof(int);
			}

			memcpy(recordField + recordFieldOffset, (char*)recordToRead + rbfm->getRecordFieldOffset(recordToRead,extractedDataDescriptor[i].fieldNum), fieldSize);
			recordFieldOffset = recordFieldOffset + fieldSize;
		}
		else//return value -1 means NULL
		{
			nullsIndicator[positionOfByte] = nullsIndicator[positionOfByte] | (1 << (7 - positionOfNullIndicator));
		}

	}

	return 0;
}

void Project ::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = tableName;
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
}



INLJoin :: INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
	leftIter = leftIn;
	rightIter = rightIn;
	op = condition.op;
	leftAttr = condition.lhsAttr;
	rightAttr = condition.rhsAttr;

	leftIter->getAttributes(lAttrs);
	rightIter->getAttributes(rAttrs);

	for(unsigned i = 0 ; i < lAttrs.size() ; i++)
	{
		if(lAttrs[i].name == leftAttr)
		{
			leftAttrType = lAttrs[i].type;
			break;
		}
	}

	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		if(rAttrs[i].name == rightAttr)
		{
			rightAttrType = rAttrs[i].type;
			break;
		}
	}

}
INLJoin :: ~INLJoin(){

}



bool INLJoin :: joinSatisfied(void *leftTuple,void *rightTuple)
{

	if(leftAttrType == TypeInt && rightAttrType == TypeInt)
	{
		int lValue;
		getValueOfAttr(leftTuple,leftAttri,)
	}
	else if(leftAttrType == TypeReal && rightAttrType == TypeReal)
	{

	}
	else if(leftAttrType == TypeVarChar && rightAttrType == TypeVarChar)
	{

	}

	return false;
}

RC INLJoin :: getNextTuple(void *data){

	char leftTuple[PAGE_SIZE];
	char rightTuple[PAGE_SIZE];

	while(leftIter->getNextTuple(leftTuple) != QE_EOF)
	{
		while(rightIter->getNextTuple(rightTuple) != QE_EOF)
		{
			//attribute extract;
			if(joinSatisfied(leftTuple,rightTuple))
			{
				concaternate(data,leftTuple,rightTuple);
				return 0;
			}


		}
	}

	return QE_EOF;
}

void INLJoin :: getAttributes(vector<Attribute> &attrs) const{

}

