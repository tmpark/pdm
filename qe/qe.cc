
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
			string value;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, value) != 0)
			{
				cout << "ERRORINT" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				cout <<  "ERROR: Righthandside is attr" << endl;
				return -1;
			}
			//FIXME: I think they assumed that they will not provide string in rhside
			int *length = (int *)(cond.rhsValue.data);
			string s((char *)(length + 1), *length);
			if(compareValues(value, s, cond.op) == true)
			{
				return 0;
			}
		}
		else if(type == TypeInt)
		{
			int value;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, value) != 0)
			{
				cout << "ERRORINT" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				cout <<  "ERROR: Righthandside is attr" << endl;
				return -1;
			}
			int a = *(int *)((cond.rhsValue).data);
			int b = value;
			if(compareValues(value, *(int *)((cond.rhsValue).data), cond.op) == true)
			{
				return 0;
			}
		}
		else if(type == TypeReal)
		{
			float value;
			if(getValueOfAttr(data, attrs, cond.lhsAttr, value) != 0)
			{
				cout << "ERRORREAL" << endl;
				return -1;
			}
			if(cond.bRhsIsAttr == true)
			{
				cout <<  "ERROR: Righthandside is attr" << endl;
				return -1;
			}
			if(compareValues(value, *(float *)cond.rhsValue.data, cond.op) == true)
			{
				return 0;
			}
		}

	}
	return -1;
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
}

BNLJoin::~BNLJoin()
{
	for(unsigned i = 0; i < bufferV.size(); i++)
	{
		delete[] bufferV[i];
	}
	for(unsigned i = 0; i < otherTuples.size(); i++)
	{
		delete[] otherTuples[i].tuple;
	}
}

RC BNLJoin::getNextTuple(void *data)
{

	if(otherTuples.size() > 0)
	{
		memcpy(data, otherTuples.front().tuple, otherTuples.front().size);
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

		/**************************______Check Tuple______****************************/
		std::map<string, vector<TupleInfo> >::iterator got = tuplesMap.find(key);
		if(got != tuplesMap.end())
		{
			TupleInfo tupleInfo = got->second.at(0);
			join(data, tupleInfo, rightTuple, rTupleSize);
			//FIXME:Here in the for loop store tuples in something to return later
			for(unsigned i = 1; i < got->second.size(); i++)
			{
				TupleInfo tupleInfo = got->second.at(i);
				char *otherTuple = new char[tupleInfo.size + rTupleSize];
				join(otherTuple, tupleInfo, rightTuple, rTupleSize);
				TupleInfo otherTupleInfo;
				otherTupleInfo.tuple = otherTuple;
				otherTupleInfo.size = tupleInfo.size + rTupleSize;
				otherTuples.push_back(otherTupleInfo);
			}
			return 0;
		}
		else
		{
			//				vector<TupleInfo> v;
			//				v.push_back(tupInfo);
			//				tuplesMap.insert(std::pair<string, vector<TupleInfo> >(key, v));
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

////////////////////////////////////////////
///////////////HELPERS//////////////////////
////////////////////////////////////////////

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


void BNLJoin::readLeftBlock()
{
	bufferV.clear();
	unsigned occupiedSpace = 0;
	char tuple[WHOLE_SIZE_FOR_ENTRIES];
	while(occupiedSpace <= totalBufferSize)
	{
		if(leftIt->getNextTuple(tuple) != 0)
		{
			hasMore = false;
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
				value = *((int *) (nullsIndicator + exteriorRecordOffset));
				return 0;
			}
			else if(attrs[i].type ==TypeReal)
			{
				value = *((float *) (nullsIndicator + exteriorRecordOffset));
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
	attrNamesToProject = attrNames;

	for(vector<string>::const_iterator it = attrNamesToProject.begin() ; it != attrNamesToProject.end() ; ++it)
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


	for(vector<string>::const_iterator it = attrNamesToProject.begin() ; it != attrNamesToProject.end() ; ++it)
	{
		//string



		//		for(unsigned i = 0 ; i < attrsFromIter.size() ; i++)
		//		{
		//
		//			string attributeName;
		//			split(attrsFromIter[i].name, tableName, attributeName);
		//
		//			//Projected Attribute construction
		//			if(attributeName.compare(*it) == 0)
		//			{
		//				//Projected Attr location info
		//				ExtractedAttr extractedAttr;
		//				extractedAttr.fieldNum = i;
		//				extractedAttr.type = attrsFromIter[i].type;
		//				extractedDataDescriptor.push_back(extractedAttr);
		//
		//				//Projected Attr
		//				Attribute attr;
		//				attr = attrsFromIter[i];
		//				attrs.push_back(attr);
		//			}
		//		}
	}


	//rc = projectData(extractedDataDescriptor,returnedData,data);
	return rc;
}

RC Project::projectData(vector<ExtractedAttr> &extractedDataDescriptor, char *recordToRead, void *data)
{
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



		//		for (unsigned j = 0 ; j < )
		//
		//		int fieldSize = rbfm->getRecordFieldSize(recordToRead,extractedDataDescriptor[i].fieldNum);
		//		if(fieldSize != -1)
		//		{
		//			if(extractedDataDescriptor[i].type == TypeVarChar)
		//			{
		//				int stringSize = fieldSize;
		//				char *currentRecordField = recordField + recordFieldOffset;
		//				*((int*)currentRecordField) = stringSize;
		//				recordFieldOffset = recordFieldOffset + sizeof(int);
		//			}
		//
		//			memcpy(recordField + recordFieldOffset, (char*)recordToRead + rbfm->getRecordFieldOffset(recordToRead,extractedDataDescriptor[i].fieldNum), fieldSize);
		//			recordFieldOffset = recordFieldOffset + fieldSize;
		//		}
		//		else//return value -1 means NULL
		//		{
		//			nullsIndicator[positionOfByte] = nullsIndicator[positionOfByte] | (1 << (7 - positionOfNullIndicator));
		//		}

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

RC INLJoin :: concaternate(void *data,void *leftTuple,void *rightTuple)
{



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
				RC rc = concaternate(data,leftTuple,rightTuple);
				return 0;
			}


		}
	}

	return QE_EOF;
}

void INLJoin :: getAttributes(vector<Attribute> &attrs) const{

}

