
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

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
		TableScan *rightIn,           // TableScan Iterator of input S
		const Condition &condition,   // Join condition
		const unsigned numPages       // # of pages that can be loaded into memory,
		//   i.e., memory block size (decided by the optimizer)
)
{
	leftIt = leftIn;
	rightIt = rightIn;
	cond = condition;
	this->numPages = numPages;
	totalBufferSize = numPages*PAGE_SIZE;
	buffer = new char[totalBufferSize];

	unsigned occupiedSpace = 0;
	char tuple[WHOLE_SIZE_FOR_ENTRIES];
	vector<Attribute> attrs;
	leftIt->getAttributes(attrs);
	while(occupiedSpace <= totalBufferSize)
	{
		if(leftIt->getNextTuple(tuple) != 0)
		{
			hasMore = false;
			//return something or just return;
		}
		unsigned tupleSize = getSizeOfTuple(attrs, tuple);
		//add tuple to buffer
		memcpy(buffer + occupiedSpace, tuple, tupleSize);
		occupiedSpace = occupiedSpace + tupleSize;


		string tableName;
		string attrName;
		split(cond.lhsAttr,tableName, attrName);
		AttrType type = getType(attrs, attrName);

		string key;
		if(type == TypeVarChar)
		{
			getValueOfAttr(tuple, attrs, attrName, key);
		}
		else if(type == TypeInt)
		{
			int value = 0;
			getValueOfAttr(tuple, attrs, attrName, value);
			key = std::to_string(value);
		}
		else if(type == TypeReal)
		{
			float value = 0;
			getValueOfAttr(tuple, attrs, attrName, value);
			key = std::to_string(value);
		}
		else
		{
			cout << "ERROR" << endl;
		}

		auto got = tuplesMap.find(key);
		if(got == tuplesMap.end())
		{

		}
	}
}

BNLJoin::~BNLJoin()
{
	delete[] buffer;
}

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
