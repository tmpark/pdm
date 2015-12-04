
#include "qe.h"
#include <string>
#include <cmath> //added library
#include <cstdlib> //added library
#include <cstring> //added library
#include <cstdio> //added library
#include <iostream> //added library
#include <fstream> //added library
#include <stdio.h>
#include <stdlib.h>

Filter::Filter(Iterator* input, const Condition &condition)
{
	it = input;
	cond = condition;
	it->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data)
{
	//    char tableName[WHOLE_SIZE_FOR_ENTRIES];
	//    char attrName[WHOLE_SIZE_FOR_ENTRIES];
	//string tableName;
	//string attrName;
	//    char tuple[WHOLE_SIZE_FOR_ENTRIES];

	//split(cond.lhsAttr,tableName, attrName);
	//AttrType type = getType(attrs, attrName);
	AttrType type = cond.rhsValue.type;

	while (it->getNextTuple(data) == 0)
	{
		if(type == TypeVarChar)
		{
			string valueLeft;
			string valueRight;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, valueLeft) != 0)
			{
				cout << "ERRORCHAR" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				if(getValueOfAttr(data, attrs, cond.rhsAttr, valueRight) != 0)
				{
					cout << "ERRORCHAR" << endl;
					return -1;
				}
			}
			else
			{
				int *length = (int *)(cond.rhsValue.data);
				valueRight = string((char *)(length + 1), *length);
			}
			if(compareValues(valueLeft, valueRight, cond.op) == true)
			{
				return 0;
			}
		}
		else if(type == TypeInt)
		{
			int valueL;
			int valueR;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, valueL) != 0)
			{
				cout << "ERRORINT" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				if(getValueOfAttr(data, attrs, cond.rhsAttr, valueR) != 0)
				{
					cout << "ERRORCHAR" << endl;
					return -1;
				}
			}
			else
			{
				valueR = *(int *)((cond.rhsValue).data);
			}
			if(compareValues(valueL, valueR, cond.op) == true)
			{
				return 0;
			}
		}
		else if(type == TypeReal)
		{
			float valueL;
			float valueR;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, valueL) != 0)
			{
				cout << "ERRORINT" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				if(getValueOfAttr(data, attrs, cond.rhsAttr, valueR) != 0)
				{
					cout << "ERRORCHAR" << endl;
					return -1;
				}
			}
			else
			{
				valueR = *(float *)((cond.rhsValue).data);
			}
			if(compareValues(valueL, valueR, cond.op) == true)
			{
				return 0;
			}
		}
	}
	return QE_EOF;
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
	rightIt->getAttributes(rightAttrs);
	leftIt->getAttributes(leftAttrs);
	keyType = getType(leftAttrs, cond.lhsAttr);
	if(cond.op != EQ_OP)
	{
		cout << "BNLJOIN CONSTRUCTOR ERROR" << endl;
	}

	if(numPages > 0)
	{
		hasMore = true;
		readLeftBlock();
	}

	for(unsigned int i = 0; i < leftAttrs.size(); i++)
	{
		attrs.push_back(leftAttrs.at(i));
	}
	for(unsigned int i = 0; i < rightAttrs.size(); i++)
	{
		attrs.push_back(rightAttrs.at(i));
	}
}

BNLJoin::~BNLJoin()
{
	free();
}

RC BNLJoin::getNextTuple(void *data)
{

	if(otherTuples.size() > 0)
	{
		memcpy(data, otherTuples.front().tuple, otherTuples.front().size);
		delete[] otherTuples.front().tuple;
		otherTuples.erase(otherTuples.begin());
		return 0;
	}

	char *rightTuple[WHOLE_SIZE_FOR_ENTRIES];
	while(rightIt->getNextTuple(rightTuple) == 0)
	{
		unsigned rTupleSize = getSizeOfTuple(rightAttrs, rightTuple);

		/**************************______Create Key______****************************/
		string key;
		if(keyType == TypeVarChar)
		{
			getValueOfAttr(rightTuple, rightAttrs, cond.rhsAttr, key);
		}
		else if(keyType == TypeInt)
		{
			int value = 0;
			getValueOfAttr(rightTuple, rightAttrs, cond.rhsAttr, value);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%d", value);
			key = string(valueChar);
		}
		else if(keyType == TypeReal)
		{
			float value = 0;
			getValueOfAttr(rightTuple, rightAttrs, cond.rhsAttr, value);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%f", value);
			key = string(valueChar);
			//key = std::to_string(value);
		}
		else
		{
			cout << "ERROR" << endl;
		}

		//		/**************************______Check Tuple______****************************/
		//		std::map<string, vector<TupleInfo> >::iterator got = tuplesMap.find(key);
		//		if(got != tuplesMap.end())
		//		{
		//			TupleInfo tupleInfo = got->second.at(0);
		//			join(data, tupleInfo, rightTuple, rTupleSize);
		//			//FIXME:Here in the for loop store tuples in something to return later
		//			for(unsigned i = 1; i < got->second.size(); i++)
		//			{
		//				TupleInfo tupleInfo = got->second.at(i);
		//				char *otherTuple = new char[tupleInfo.size + rTupleSize];
		//				join(otherTuple, tupleInfo, rightTuple, rTupleSize);
		//				TupleInfo otherTupleInfo;
		//				otherTupleInfo.tuple = otherTuple;
		//				otherTupleInfo.size = tupleInfo.size + rTupleSize;
		//				otherTuples.push_back(otherTupleInfo);
		//			}
		//			return 0;
		//		}
		//		else
		//		{
		//			//				vector<TupleInfo> v;
		//			//				v.push_back(tupInfo);
		//			//				tuplesMap.insert(std::pair<string, vector<TupleInfo> >(key, v));
		//		}

		/**************************______Check Tuple______****************************/
		std::map<string, vector<TupleInfo> >::iterator got = tuplesMap.find(key);
		if(got != tuplesMap.end())
		{
			TupleInfo tupleInfo = got->second.at(0);
			concatenate(data, tupleInfo.tuple, rightTuple);
			//rightIt->rm.printTuple(attrs, data);
			for(unsigned i = 1; i < got->second.size(); i++)
			{
				TupleInfo tupleInfo = got->second.at(i);
				char *otherTuple = new char[tupleInfo.size + rTupleSize];
				concatenate(otherTuple, tupleInfo.tuple, rightTuple);
				TupleInfo otherTupleInfo;
				otherTupleInfo.tuple = otherTuple;
				otherTupleInfo.size = tupleInfo.size + rTupleSize;
				otherTuples.push_back(otherTupleInfo);
			}
			return 0;
		}

	}

	if(hasMore)
	{
		readLeftBlock();
		rightIt->setIterator();
		return getNextTuple(data);
	}
	return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const
{
	for(unsigned int i = 0; i < this->attrs.size(); i++)
	{
		attrs.push_back(this->attrs.at(i));
	}
}

////////////////////////////////////////////
///////////////HELPERS//////////////////////
////////////////////////////////////////////

void BNLJoin::free()
{
	for(unsigned i = 0; i < bufferV.size(); i++)
	{
		delete[] bufferV[i];
	}
	bufferV.clear();

	tuplesMap.clear();

	for(unsigned i = 0; i < otherTuples.size(); i++)
	{
		delete[] otherTuples[i].tuple;
	}
	otherTuples.clear();
	//cout << bufferV.size() << "-" << tuplesMap.size() << "-" << otherTuples.size() << endl;
}

void BNLJoin::join(void *data, TupleInfo &tupleInfo, void *rightTuple, unsigned rTupleSize)
{
	void *leftTuple = tupleInfo.tuple;
	unsigned lTupleSize = tupleInfo.size;

	unsigned sizeOfNullIndicatorL = ceil((float)leftAttrs.size()/8);
	unsigned sizeOfNullIndicatorR = ceil((float)rightAttrs.size()/8);

	/********************_______Null Info Combination_______*******************/
	if(leftAttrs.size()%8 != 0)
	{
		memcpy(((char *) data), leftTuple, sizeOfNullIndicatorL - 1);
		char *lastNullIndicatorL = ((char *)leftTuple) + sizeOfNullIndicatorL - 1;
		char L = *lastNullIndicatorL;
		L >>= (8 - leftAttrs.size()%8);
		char *combination = new char[sizeOfNullIndicatorR + 1];
		combination[0] = L;
		memcpy(combination + 1, rightTuple, sizeOfNullIndicatorR);
		//FIXME
		long comb = *(long *) combination;
		comb <<= (8 - leftAttrs.size()%8);
		memcpy(((char *) data) + sizeOfNullIndicatorL - 1, &comb, sizeOfNullIndicatorR + 1);
		delete[] combination;
	}
	else
	{
		memcpy(((char *) data), leftTuple, sizeOfNullIndicatorL);
		memcpy(((char *) data) + sizeOfNullIndicatorL, rightTuple, sizeOfNullIndicatorR);
	}
	/********************_______Add Tuples to Rest_______*******************/
	unsigned sizeOfNewNullInfo = sizeOfNullIndicatorL + sizeOfNullIndicatorR;
	memcpy(((char *) data) + sizeOfNewNullInfo,
			((char *)leftTuple) + sizeOfNullIndicatorL, lTupleSize - sizeOfNullIndicatorL);
	memcpy(((char *) data) + sizeOfNullIndicatorR + lTupleSize,
			((char *)rightTuple) + sizeOfNullIndicatorR, rTupleSize - sizeOfNullIndicatorR);

}

RC BNLJoin :: concatenate(void *data,const void *leftTuple,const void *rightTuple)
{
	//Null indicator initialization for concaternate tuple
	char *nullIndicator = (char*)data;
	char *dataFieldPtr = (char*)data + initializeNullIndicator(attrs,nullIndicator);

	//Left tuple field preparation
	unsigned numberOfFields = leftAttrs.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *lNullIndicator = (char*)leftTuple;
	char *lDataFieldPtr = (char*)leftTuple + numberOfBytesForNullIndicator;

	//Right tuple field preparation
	numberOfFields = rightAttrs.size();
	numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *rNullIndicator = (char*)rightTuple;
	char *rDataFieldPtr = (char*)rightTuple + numberOfBytesForNullIndicator;

	//first filled with left
	for(unsigned i = 0 ; i < leftAttrs.size() ; i++)
	{
		if(isNullField(lNullIndicator,i))
			setNull(nullIndicator,i);
		else
		{
			int lSize = getSizeOfField(lDataFieldPtr,leftAttrs[i].type);
			memcpy(dataFieldPtr, lDataFieldPtr, lSize);
			dataFieldPtr = dataFieldPtr + lSize;
			lDataFieldPtr = lDataFieldPtr + lSize;
		}

	}

	//Next filled with right
	for(unsigned i = 0 ; i < rightAttrs.size() ; i++)
	{
		if(isNullField(rNullIndicator,i))
			setNull(nullIndicator,i+leftAttrs.size()); //Nullindicator is extended from left
		else
		{
			int rSize = getSizeOfField(rDataFieldPtr,rightAttrs[i].type);
			memcpy(dataFieldPtr, rDataFieldPtr, rSize);
			dataFieldPtr = dataFieldPtr + rSize;
			rDataFieldPtr = rDataFieldPtr + rSize;
		}

	}
	return 0;
}
void BNLJoin::readLeftBlock()
{

	free();
	unsigned occupiedSpace = 0;
	char tuple[WHOLE_SIZE_FOR_ENTRIES];
	while(occupiedSpace <= totalBufferSize)
	{
		if(leftIt->getNextTuple(tuple) != 0)
		{
			hasMore = false;
			return;
			//return something or just return;
		}
		unsigned tupleSize = getSizeOfTuple(leftAttrs, tuple);
		//add tuple to buffer
		char *buffer = new char[tupleSize];
		memcpy(buffer, tuple, tupleSize);
		bufferV.push_back((void *)buffer);
		occupiedSpace = occupiedSpace + tupleSize;

		TupleInfo tupInfo;
		tupInfo.tuple = buffer;
		tupInfo.size = tupleSize;

		/**************************______Create Key______****************************/
		string key;
		if(keyType == TypeVarChar)
		{
			getValueOfAttr(tuple, leftAttrs, cond.lhsAttr, key);
		}
		else if(keyType == TypeInt)
		{
			int value = 0;
			getValueOfAttr(tuple, leftAttrs, cond.lhsAttr, value);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%d", value);
			key = string(valueChar);
		}
		else if(keyType == TypeReal)
		{
			float value = 0;
			getValueOfAttr(tuple, leftAttrs, cond.lhsAttr, value);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%f", value);
			key = string(valueChar);
			//key = std::to_string(value);
		}
		else
		{
			cout << "ERROR" << endl;
		}
		/**************************______Insert Tuple______****************************/
		std::map<string, vector<TupleInfo> >::iterator got = tuplesMap.find(key);
		if(got != tuplesMap.end())
		{
			got->second.push_back(tupInfo);
		}
		else
		{
			vector<TupleInfo> v;
			v.push_back(tupInfo);
			tuplesMap.insert(std::pair<string, vector<TupleInfo> >(key, v));
		}
	}
}

bool Iterator::isNullField(const void *data, unsigned fieldNum)
{
	unsigned positionOfByte = floor((double)fieldNum / 8);
	unsigned positionOfNullIndicator = fieldNum % 8;
	char *nullPtr = (char*)data;
	return nullPtr[positionOfByte] & (1 << (7 - positionOfNullIndicator));
}

RC Iterator::setNull(void *data, unsigned fieldNum)
{
	unsigned positionOfByte = floor((double)fieldNum / 8);
	unsigned positionOfNullIndicator = fieldNum % 8;
	char *nullIndicator = (char*) data;
	nullIndicator[positionOfByte] = nullIndicator[positionOfByte] | (1 << (7 - positionOfNullIndicator));
	return 0;
}

int Iterator::initializeNullIndicator(const vector<Attribute> &recordDescriptor,void *data)
{
	unsigned numberOfFields = recordDescriptor.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	unsigned char *nullsIndicator = (unsigned char*)data;
	memset(nullsIndicator, 0, numberOfBytesForNullIndicator);
	return numberOfBytesForNullIndicator;
}


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

		if(attrName.compare(attrs.at(i).name) == 0)
		{
			if(nullExist)
				return 1;

			char *stringLength = (char*)data + exteriorRecordOffset;
			int stringLength_int = *((int*)stringLength);
			exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field
			value = string((char *)data + exteriorRecordOffset, stringLength_int);
			return 0;
		}
		else
		{
			if(nullExist)
				continue;

			if(attrs[i].type == TypeInt)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
			else if(attrs[i].type ==TypeReal)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(float);
			else if(attrs[i].type ==TypeVarChar)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field
				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
			}
		}
	}
	return -1;
}

//RC Iterator::getAddressOfAttr(const void* data, vector<Attribute> &attrs, string &attrName, void *address)
//{
//	unsigned numberOfFields = attrs.size();
//
//	//exteriorRecord related
//	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
//	unsigned char *nullsIndicator = (unsigned char*)data;
//	//char *exteriorRecordField = (char*)exteriorRecord + numberOfBytesForNullIndicator;
//
//	bool nullExist = false;
//
//	int exteriorRecordOffset = 0;
//	exteriorRecordOffset = exteriorRecordOffset + numberOfBytesForNullIndicator;
//
//
//	for (unsigned i = 0 ;i < numberOfFields ; i++)
//	{
//		unsigned positionOfByte = floor((double)i / 8);
//		unsigned positionOfNullIndicator = i % 8;
//		nullExist = nullsIndicator[positionOfByte] & (1 << (7 - positionOfNullIndicator));
//
//		if(!nullExist)//Not Null Value
//		{
//
//			if(attrs[i].type == TypeVarChar)
//			{
//				char *stringLength = (char*)data + exteriorRecordOffset;
//				int stringLength_int = *((int*)stringLength);
//
//				if(attrName.compare(attrs.at(i).name) == 0)
//				{
//					address = (void *)(nullsIndicator + exteriorRecordOffset);
//					return 0;
//				}
//				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
//				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
//			}
//			else if (attrs[i].type == TypeInt)
//			{
//				if(attrName.compare(attrs.at(i).name) == 0)
//				{
//					address = (void*) (nullsIndicator + exteriorRecordOffset);
//					return 0;
//				}
//				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
//			}
//			else if (attrs[i].type ==TypeReal)
//			{
//				if(attrName.compare(attrs.at(i).name) == 0)
//				{
//					address = (void*) (nullsIndicator + exteriorRecordOffset);
//					return 0;
//				}
//				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
//			}
//		}
//		else
//		{
//			//blablalbalbla
//			//return -1;
//		}
//	}
//	return -1;
//}

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


		if(attrName.compare(attrs.at(i).name) == 0)
		{
			if(nullExist)
				return 1;

			if(attrs[i].type == TypeInt)
			{
				char *valuePtr = (char*)data + exteriorRecordOffset;
				value = *((int *)valuePtr);
				return 0;
			}
			else if(attrs[i].type ==TypeReal)
			{
				char *valuePtr = (char*)data + exteriorRecordOffset;
				value = *((float *)valuePtr);
				return 0;
			}
		}
		else
		{
			if(nullExist)
				continue;

			if(attrs[i].type == TypeInt)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
			else if(attrs[i].type ==TypeReal)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(float);
			else if(attrs[i].type ==TypeVarChar)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field
				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
			}
		}

	}
	return -1;
}



RC Iterator::getOffsetNSizeOfAttr(const void* data, vector<Attribute> &attrs, string &attrName, int &offset, int &size)
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

		//target attr
		if(attrName.compare(attrs.at(i).name) == 0)
		{
			if(nullExist)
			{
				offset = -1;
				size = -1;
				return 0;
			}

			if(attrs[i].type == TypeInt)
			{
				offset = exteriorRecordOffset;
				size = sizeof(int);
				return 0;
			}
			else if(attrs[i].type ==TypeReal)
			{
				offset = exteriorRecordOffset;
				size = sizeof(float);
				return 0;
			}
			else if(attrs[i].type ==TypeReal)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				offset = exteriorRecordOffset;
				size = sizeof(int) + stringLength_int;
				return 0;
			}
		}
		else
		{
			if(nullExist)
				continue;

			if(attrs[i].type == TypeInt)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int);
			else if(attrs[i].type ==TypeReal)
				exteriorRecordOffset = exteriorRecordOffset + sizeof(float);
			else if(attrs[i].type ==TypeVarChar)
			{
				char *stringLength = (char*)data + exteriorRecordOffset;
				int stringLength_int = *((int*)stringLength);
				exteriorRecordOffset = exteriorRecordOffset + sizeof(int); //length field
				exteriorRecordOffset = exteriorRecordOffset + stringLength_int;// string
			}
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
bool Iterator::compareValues(T const valueExtracted, T const valueCompared, int compOp)
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


unsigned Iterator:: getSizeOfField(void *field, AttrType type)
{
	if(type == TypeInt)
	{
		return sizeof(int);
	}
	else if(type ==TypeReal)
	{
		return sizeof(float);
	}
	else if(type ==TypeReal)
	{
		char *stringLength = (char*)field;
		int stringLength_int = *((int*)stringLength);
		return sizeof(int) + stringLength_int;
	}

	return 0;
}

// ... the rest of your implementations go here



Project :: Project(Iterator *input,const vector<string> &attrNames)
{
	iter = input;
	iter->getAttributes(attrsFromIter);

	for(vector<string>::const_iterator it = attrNames.begin() ; it != attrNames.end() ; ++it)
	{
		for(unsigned i = 0 ; i < attrsFromIter.size() ; i++)
		{

			//Projected Attribute construction
			if(attrsFromIter[i].name.compare(*it) == 0)
			{
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

	char *nullIndicator = (char*)data;
	char *dataFieldPtr = (char*)data + initializeNullIndicator(attrs,nullIndicator);

	unsigned currentOffset = 0;
	for(unsigned i = 0 ; i <  attrs.size(); i++)
	{
		int offset = -1;
		int size = -1;
		rc = getOffsetNSizeOfAttr(returnedData,attrsFromIter,attrs[i].name,offset,size);

		if(offset == -1)
			rc = setNull(nullIndicator,i);
		else
		{
			memcpy(dataFieldPtr + currentOffset, returnedData + offset, size);
			currentOffset = currentOffset + size;
		}

	}

	return rc;
}

void Project ::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
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

	//result attribute construction

	attrs = lAttrs;
	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		attrs.push_back(rAttrs[i]);
	}


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


	lEOF = leftIter->getNextTuple(leftTuple);

}
INLJoin :: ~INLJoin(){

}


bool INLJoin :: joinSatisfied(void *leftTuple,void *rightTuple)
{

	if(leftAttrType == TypeInt && rightAttrType == TypeInt)
	{
		int lValue = -1;
		int rValue = -1;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}
	else if(leftAttrType == TypeReal && rightAttrType == TypeReal)
	{
		float lValue = -1;
		float rValue = -1;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}
	else if(leftAttrType == TypeVarChar && rightAttrType == TypeVarChar)
	{
		string lValue;
		string rValue;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}

	return false;
}

RC INLJoin :: concaternate(void *data,const void *leftTuple,const void *rightTuple)
{
	//Null indicator initialization for concaternate tuple
	char *nullIndicator = (char*)data;
	char *dataFieldPtr = (char*)data + initializeNullIndicator(attrs,nullIndicator);

	//Left tuple field preparation
	unsigned numberOfFields = lAttrs.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *lNullIndicator = (char*)leftTuple;
	char *lDataFieldPtr = (char*)leftTuple + numberOfBytesForNullIndicator;

	//Right tuple field preparation
	numberOfFields = rAttrs.size();
	numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *rNullIndicator = (char*)rightTuple;
	char *rDataFieldPtr = (char*)rightTuple + numberOfBytesForNullIndicator;

	//first filled with left
	for(unsigned i = 0 ; i < lAttrs.size() ; i++)
	{
		if(isNullField(lNullIndicator,i))
			setNull(nullIndicator,i);
		else
		{
			int lSize = getSizeOfField(lDataFieldPtr,lAttrs[i].type);
			memcpy(dataFieldPtr, lDataFieldPtr, lSize);
			dataFieldPtr = dataFieldPtr + lSize;
			lDataFieldPtr = lDataFieldPtr + lSize;
		}

	}

	//Next filled with right
	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		if(isNullField(rNullIndicator,i))
			setNull(nullIndicator,i+lAttrs.size()); //Nullindicator is extended from left
		else
		{
			int rSize = getSizeOfField(rDataFieldPtr,rAttrs[i].type);
			memcpy(dataFieldPtr, rDataFieldPtr, rSize);
			dataFieldPtr = dataFieldPtr + rSize;
			rDataFieldPtr = rDataFieldPtr + rSize;
		}

	}
	return 0;
}

RC INLJoin :: getNextTuple(void *data){

	if(lEOF == QE_EOF)
		return QE_EOF;

	char rightTuple[PAGE_SIZE];

	while(!lEOF)
	{
		while(rightIter->getNextTuple(rightTuple)!= QE_EOF)
		{
			//attribute extract;
			if(joinSatisfied(leftTuple,rightTuple))
			{
				RC rc = concaternate(data,leftTuple,rightTuple);
				return rc;
			}
		}

		lEOF = leftIter->getNextTuple(leftTuple);

		rightIter->setIterator(NULL,NULL,true,true);
	}

	return QE_EOF;

}

void INLJoin :: getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}



Aggregate :: Aggregate(Iterator *input,          // Iterator of input R
		Attribute aggAttr,        // The attribute over which we are computing an aggregate
		AggregateOp op            // Aggregate operation
)
{
	this->op = op;
	this->aggAttr = aggAttr;
	this->iter = input;
	iter->getAttributes(attrs);
	finished = false;
	groupby = false;

}


Aggregate :: Aggregate(Iterator *input,             // Iterator of input R
		Attribute aggAttr,           // The attribute over which we are computing an aggregate
		Attribute groupAttr,         // The attribute over which we are grouping the tuples
		AggregateOp op              // Aggregate operation
)
{
	this->op = op;
	this->aggAttr = aggAttr;
	this->iter = input;
	this->groupAttr = groupAttr;
	groupby = true;
	iter->getAttributes(attrs);
	finished = false;

}


RC Aggregate :: gatherAggrInfo(void *returnedData,float value)
{
	string groupKey;
	if(groupAttr.type == TypeInt)
	{
		int tempGroupKey;
		getValueOfAttr(returnedData,attrs,groupAttr.name,tempGroupKey);
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", tempGroupKey);
		groupKey = string(valueChar);

	}
	else if(groupAttr.type == TypeReal)
	{
		float tempGroupKey;
		getValueOfAttr(returnedData,attrs,groupAttr.name,tempGroupKey);
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%f", tempGroupKey);
		groupKey = string(valueChar);
	}
	else if (groupAttr.type == TypeVarChar)
	{
		getValueOfAttr(returnedData,attrs,groupAttr.name,groupKey);
	}

	std::map<string,AggInfo>::iterator it;

	it = aggrMap.find(groupKey);
	if(it == aggrMap.end())
	{
		AggInfo tempAggInfo;
		tempAggInfo.count = 1;
		tempAggInfo.max = value;
		tempAggInfo.min = value;
		tempAggInfo.sum = value;
		aggrMap.insert(std::pair<string,AggInfo>(groupKey,tempAggInfo));
	}
	else
	{
		it->second.count++;
		it->second.sum = it->second.sum + value;
		if(it->second.min > value)
			it->second.min = value;
		if(it->second.max < value)
			it->second.max = value;
	}
	return 0;

}


RC Aggregate ::putGroupByResult(void *data,string key,AggInfo aggInfo)
{

	float result = -1;

	if(op == MIN)
		result = aggInfo.min;
	else if(op == MAX)
		result = aggInfo.max;
	else if(op == SUM)
		result = aggInfo.sum;
	else if(op == AVG)
		result = aggInfo.sum / aggInfo.count;
	else if(op == COUNT)
		result = aggInfo.count;

	unsigned offset = 0;

	//put group key
	memset((char*)data + offset,0,1);
	offset = offset + 1;


	if(groupAttr.type == TypeInt)
	{
		int key_int = atoi(key.c_str());
		memcpy((char*)data + offset, &key_int, sizeof(int));
		offset = offset + sizeof(int);

	}
	else if(groupAttr.type == TypeReal)
	{
		int key_float = atof(key.c_str());
		memcpy((char*)data + offset, &key_float, sizeof(float));
		offset = offset + sizeof(float);
	}
	else if(groupAttr.type == TypeVarChar)
	{
		memcpy((char*)data + offset, key.c_str(), key.size());
		offset = offset + key.size();
	}

	//put result
	memcpy((char*)data + offset, &result, sizeof(float));

	return 0;

}


//Min, Max, Count, Sum, Avg
RC Aggregate ::getNextTuple(void *data){

	if(finished)
	{
		if(groupby && groupKeyIt != aggrMap.end())
		{
			string tempKey = groupKeyIt->first;
			AggInfo tempAggInfo = groupKeyIt->second;
			putGroupByResult(data,tempKey,tempAggInfo);
			groupKeyIt++;
			return 0;
		}
		else
			return QE_EOF;
	}

	char returnedData[PAGE_SIZE];

	//First run
	if(iter->getNextTuple(returnedData) == QE_EOF)
	{
		char *nullIndicator = (char*)data;
		nullIndicator[0] = nullIndicator[0] | 1 << 7;
		finished = true;
		return QE_EOF;
	}

	float firstValue;

	if(aggAttr.type == TypeInt)
	{
		int tempValue;
		getValueOfAttr(returnedData, attrs, aggAttr.name, tempValue);
		firstValue = tempValue;
	}
	else if(aggAttr.type == TypeReal)
	{
		float tempValue;
		getValueOfAttr(returnedData, attrs, aggAttr.name, tempValue);
		firstValue = tempValue;
	}

	if(groupby)
		gatherAggrInfo(returnedData,firstValue);


	float min = firstValue;
	float max = firstValue;
	float sum = firstValue;
	float count = 1;


	while(iter->getNextTuple(returnedData) != QE_EOF)
	{

		float value;
		if(aggAttr.type == TypeInt)
		{
			int tempValue;
			getValueOfAttr(returnedData, attrs, aggAttr.name, tempValue);
			value = tempValue;

		}
		else if(aggAttr.type == TypeReal)
		{
			float tempValue;
			getValueOfAttr(returnedData, attrs, aggAttr.name, tempValue);
			value = tempValue;
		}

		if(groupby)
			gatherAggrInfo(returnedData,value);

		if(value < min)
			min = value;
		if(value > max)
			max = value;
		sum = sum + value;
		count++;
	}

	float result = -1;

	if(groupby)
	{
		groupKeyIt = aggrMap.begin();
		string tempKey = groupKeyIt->first;
		AggInfo tempAggInfo = groupKeyIt->second;
		putGroupByResult(data,tempKey,tempAggInfo);
		groupKeyIt++;
	}
	else
	{
		if(op == MIN)
			result = min;
		else if(op == MAX)
			result = max;
		else if(op == SUM)
			result = sum;
		else if(op == AVG)
			result = sum / count;
		else if(op == COUNT)
			result = count;
		memset((char*)data,0,1);
		*(float*)((char*)data+1) = result;
	}
	finished = true;

	return 0;
}


int GHJoin :: hash(const std::string &data) {
  int h(0);
  for (int i=0; i < (int)data.length(); i++)
    h = (h << 6) ^ (h >> 26) ^ data[i];
  return h;
}

GHJoin :: GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
     )
{
	op = condition.op;
	leftIter = leftIn;
	rightIter = rightIn;
	numOfPartitions = numPartitions;
	leftAttr = condition.lhsAttr;
	rightAttr = condition.rhsAttr;
	leftIter->getAttributes(lAttrs);
	rightIter->getAttributes(rAttrs);
	currentPartition = 0;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = -1;

	attrs = lAttrs;


	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		attrs.push_back(rAttrs[i]);
	}

	for(unsigned i = 0 ; i < lAttrs.size() ; i++)
	{
		lAttributeNames.push_back(lAttrs[i].name);

		if(lAttrs[i].name == leftAttr)
		{
			leftAttrType = lAttrs[i].type;
		}
	}

	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		rAttributeNames.push_back(rAttrs[i].name);
		if(rAttrs[i].name == rightAttr)
		{
			rightAttrType = rAttrs[i].type;
		}
	}

	//Partition file creation
	for (unsigned i = 0  ; i < numPartitions ; i++)
	{
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", i);
		string partitionNum = string(valueChar);

		string leftPartition = GH_LEFT + partitionNum + "_" + leftAttr;
		string rightPartition = GH_RIGHT + partitionNum + "_" + rightAttr;
		rc = rbfm->createFile(leftPartition);
		rc = rbfm->createFile(rightPartition);
	}

	//left tuple insert
	char tupleTemp[PAGE_SIZE];

	//prepare for left partition
	while(leftIn->getNextTuple(tupleTemp) != QE_EOF)
	{
		string leftKey;
		if(leftAttrType == TypeInt)
		{
			int tempGroupKey;
			getValueOfAttr(tupleTemp,lAttrs,leftAttr,tempGroupKey);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%d", tempGroupKey);
			leftKey = string(valueChar);

		}
		else if(leftAttrType == TypeReal)
		{
			float tempGroupKey;
			getValueOfAttr(tupleTemp,lAttrs,leftAttr,tempGroupKey);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%f", tempGroupKey);
			leftKey = string(valueChar);
		}
		else if(leftAttrType == TypeVarChar)
		{
			getValueOfAttr(tupleTemp,lAttrs,leftAttr,leftKey);
		}
		int hashValue = hash(leftKey)%numOfPartitions;
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", hashValue);
		string partitionNum = string(valueChar);
		string targetGHFile = GH_LEFT + partitionNum + "_" + leftAttr;

		FileHandle fileHandle;
		RID rid;
		rc = rbfm->openFile(targetGHFile,fileHandle);
		rc = rbfm->insertRecord(fileHandle,lAttrs,tupleTemp,rid);
		rc = rbfm->closeFile(fileHandle);
	}

	//prepare for right partition
	while(rightIn->getNextTuple(tupleTemp) != QE_EOF)
	{
		string rightKey;
		if(rightAttrType == TypeInt)
		{
			int tempGroupKey;
			getValueOfAttr(tupleTemp,rAttrs,rightAttr,tempGroupKey);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%d", tempGroupKey);
			rightKey = string(valueChar);

		}
		else if(rightAttrType == TypeReal)
		{
			float tempGroupKey;
			getValueOfAttr(tupleTemp,rAttrs,rightAttr,tempGroupKey);
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%f", tempGroupKey);
			rightKey = string(valueChar);
		}
		else if(rightAttrType == TypeVarChar)
		{
			getValueOfAttr(tupleTemp,rAttrs,rightAttr,rightKey);
		}
		int hashValue = hash(rightKey)%numOfPartitions;
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", hashValue);
		string partitionNum = string(valueChar);
		string targetGHFile = GH_RIGHT + partitionNum + "_" + rightAttr;

		FileHandle fileHandle;
		RID rid;
		rc = rbfm->openFile(targetGHFile,fileHandle);
		rc = rbfm->insertRecord(fileHandle,rAttrs,tupleTemp,rid);
		rc = rbfm->closeFile(fileHandle);
	}

	char valueChar[64];
	snprintf(valueChar, sizeof(valueChar), "%d", currentPartition);
	string partitionNum = string(valueChar);
	string targetGHFile = GH_LEFT + partitionNum + "_" + leftAttr;
	rc = rbfm->openFile(targetGHFile,leftPartition_fileHandle);
	rc = rbfm->scan(leftPartition_fileHandle,lAttrs,"",NO_OP,NULL,lAttributeNames,leftPartition_ScanIterator);

	targetGHFile = GH_RIGHT + partitionNum + "_" + rightAttr;
	rc = rbfm->openFile(targetGHFile,rightPartition_fileHandle);
	rc = rbfm->scan(rightPartition_fileHandle,rAttrs,"",NO_OP,NULL,rAttributeNames,rightPartition_ScanIterator);

	RID rid;
	lEOF = leftPartition_ScanIterator.getNextRecord(rid,leftTuple);

}

RC GHJoin ::getNextTuple(void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	RC rc = -1;

	if(lEOF != QE_EOF)
	{
		rc = _getNextTuple(data);
		if(rc == 0)
				return 0;
		else
			currentPartition++;
	}

	while(currentPartition < numOfPartitions)
	{
		//Reset;
		rightPartition_ScanIterator.close();
		rbfm->closeFile(rightPartition_fileHandle);
		leftPartition_ScanIterator.close();
		rbfm->closeFile(leftPartition_fileHandle);

		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", currentPartition);
		string partitionNum = string(valueChar);
		string targetGHFile = GH_LEFT + partitionNum + "_" + leftAttr;
		rc = rbfm->openFile(targetGHFile,leftPartition_fileHandle);
		rc = rbfm->scan(leftPartition_fileHandle,lAttrs,"",NO_OP,NULL,lAttributeNames,leftPartition_ScanIterator);

		targetGHFile = GH_RIGHT + partitionNum + "_" + rightAttr;
		rc = rbfm->openFile(targetGHFile,rightPartition_fileHandle);
		rc = rbfm->scan(rightPartition_fileHandle,rAttrs,"",NO_OP,NULL,rAttributeNames,rightPartition_ScanIterator);

		RID rid;
		lEOF = leftPartition_ScanIterator.getNextRecord(rid,leftTuple);
		rc = _getNextTuple(data);
		if(rc == 0)
			return 0;
		else
			currentPartition++;
	}

    return QE_EOF;
}

RC GHJoin ::_getNextTuple(void *data)
{
	RID rid;
	RC rc = -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	char rightTuple[PAGE_SIZE];

	while(!lEOF)
	{
		while(rightPartition_ScanIterator.getNextRecord(rid,rightTuple) != QE_EOF)
		{
			//attribute extract;
			if(joinSatisfied(leftTuple,rightTuple))
			{
				RC rc = concaternate(data,leftTuple,rightTuple);
				return rc;
			}
		}

		lEOF = leftPartition_ScanIterator.getNextRecord(rid,leftTuple);

		//re-initialize right scaniterator
		if(lEOF != QE_EOF)
		{
			rightPartition_ScanIterator.close();
			char valueChar[64];
			snprintf(valueChar, sizeof(valueChar), "%d", currentPartition);
			string partitionNum = string(valueChar);
			string targetGHFile = GH_RIGHT + partitionNum + "_" + rightAttr;
			rc = rbfm->scan(rightPartition_fileHandle,rAttrs,"",NO_OP,NULL,rAttributeNames,rightPartition_ScanIterator);
		}
	}

	return QE_EOF;
}



bool GHJoin :: joinSatisfied(void *leftTuple,void *rightTuple)
{

	if(leftAttrType == TypeInt && rightAttrType == TypeInt)
	{
		int lValue = -1;
		int rValue = -1;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}
	else if(leftAttrType == TypeReal && rightAttrType == TypeReal)
	{
		float lValue = -1;
		float rValue = -1;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}
	else if(leftAttrType == TypeVarChar && rightAttrType == TypeVarChar)
	{
		string lValue;
		string rValue;
		getValueOfAttr(leftTuple,lAttrs,leftAttr,lValue);
		getValueOfAttr(rightTuple,rAttrs,rightAttr,rValue);
		return compareValues(lValue,rValue,op);
	}

	return false;
}

RC GHJoin :: concaternate(void *data,const void *leftTuple,const void *rightTuple)
{
	//Null indicator initialization for concaternate tuple
	char *nullIndicator = (char*)data;
	char *dataFieldPtr = (char*)data + initializeNullIndicator(attrs,nullIndicator);

	//Left tuple field preparation
	unsigned numberOfFields = lAttrs.size();
	unsigned numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *lNullIndicator = (char*)leftTuple;
	char *lDataFieldPtr = (char*)leftTuple + numberOfBytesForNullIndicator;

	//Right tuple field preparation
	numberOfFields = rAttrs.size();
	numberOfBytesForNullIndicator = ceil((float)numberOfFields/8);
	char *rNullIndicator = (char*)rightTuple;
	char *rDataFieldPtr = (char*)rightTuple + numberOfBytesForNullIndicator;

	//first filled with left
	for(unsigned i = 0 ; i < lAttrs.size() ; i++)
	{
		if(isNullField(lNullIndicator,i))
			setNull(nullIndicator,i);
		else
		{
			int lSize = getSizeOfField(lDataFieldPtr,lAttrs[i].type);
			memcpy(dataFieldPtr, lDataFieldPtr, lSize);
			dataFieldPtr = dataFieldPtr + lSize;
			lDataFieldPtr = lDataFieldPtr + lSize;
		}

	}

	//Next filled with right
	for(unsigned i = 0 ; i < rAttrs.size() ; i++)
	{
		if(isNullField(rNullIndicator,i))
			setNull(nullIndicator,i+lAttrs.size()); //Nullindicator is extended from left
		else
		{
			int rSize = getSizeOfField(rDataFieldPtr,rAttrs[i].type);
			memcpy(dataFieldPtr, rDataFieldPtr, rSize);
			dataFieldPtr = dataFieldPtr + rSize;
			rDataFieldPtr = rDataFieldPtr + rSize;
		}

	}
	return 0;
}


GHJoin :: ~GHJoin()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	RC rc = -1;
	for(unsigned i = 0 ; i < numOfPartitions ; i++)
	{
		char valueChar[64];
		snprintf(valueChar, sizeof(valueChar), "%d", i);
		string partitionNum = string(valueChar);
		string leftPartition = GH_LEFT + partitionNum + "_" + leftAttr;
		string rightPartition = GH_RIGHT + partitionNum + "_" + rightAttr;
		rc = rbfm->destroyFile(leftPartition);
		rc = rbfm->destroyFile(rightPartition);
	}

};
