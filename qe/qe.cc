
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
				return rc;
			}
		}
	}

	return QE_EOF;
}

void INLJoin :: getAttributes(vector<Attribute> &attrs) const{
    attrs.clear();
    attrs = this->attrs;
}

