#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>


#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef signed short SlotOffset;
typedef char NodeType;
typedef unsigned short NumOfEnt;

#define ROOT_NODE 'R'
#define INTER_NODE 'I'
#define LEAF_NODE 'L'


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        /*********Pagewide helper fucntions*********/

        SlotOffset getFreeSpaceOffset(const void* pageToProcess);
        RC setFreeSpaceOffset(void* pageToProcess, SlotOffset freeSpaceOffset);
        NumOfEnt getNumOfEnt(const void* pageToProcess);
        RC setNumOfEnt(void* pageToProcess, NumOfEnt numOfEnt);
        PageNum getTombstone(const void* pageToProcess);
        RC setTombstone(void* pageToProcess, PageNum tombstone);
        NodeType getNodeType(const void* pageToProcess);
        RC setNodeType(void* pageToProcess, NodeType nodeType);
        PageNum getParentPageNum(const void* pageToProcess);
        RC setParentPageNum(void* pageToProcess, PageNum parentPageNum);
        SlotOffset getEntryOffset(const void* pageToProcess,unsigned int entryNum);
        RC setEntryOffset(void* pageToProcess, unsigned int entryNum, SlotOffset entryOffset);
        PageNum getLeftSiblingPageNum(const void* pageToProcess);
        RC setLeftSiblingPageNum(void* pageToProcess, PageNum leftSiblingPageNum);
        PageNum getRightSiblingPageNum(const void* pageToProcess);
        RC setRightSiblingPageNum(void* pageToProcess, PageNum rightSiblingPageNum);
        PageNum getLeftMostChildPageNum(const void* pageToProcess);
        RC setLeftMostChildPageNum(void* pageToProcess, PageNum leftChildPageNum);


        /*******Entrywide helper functions********/
        template <typename T>
        RC getKeyOfEntry(const void* entryToProcess, T &value);
        RC getKeyOfEntry(const void* entryToProcess, string &value);

        template <typename T>
        RC setKeyOfEntry(void* entryToProcess, T value);
        RC setKeyOfEntry(void* entryToProcess, string value);

        PageNum getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType);
        RC setChildOfIntermediateEntry(void* entryToProcess, AttrType keyType, PageNum childPageNum);


        NumOfEnt getNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType);
        RC setNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType, NumOfEnt numOfRids);

        RC getEntryInLeaf(const void* entryToProcess, AttrType keyType,unsigned entryNum, RID &rid);
        RC setEntryInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid);


        unsigned getSizeOfEntryInLeaf(const void* entryToProcess, AttrType keyType);
        unsigned getSizeOfEntryInIntermediate(const void* entryToProcess, AttrType keyType);

        SlotOffset findEntryOffsetToProcess(void *pageToProcess,AttrType attrType, const void *key);

        string extractVarChar(const void* data);


        RC _insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);


    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        char tempPage0[PAGE_SIZE];
        char tempPage1[PAGE_SIZE];

        RC splitLeaf(void *leafNode, void *newLeafNode, void * newChildEntry,
        		int offset, const Attribute &Attribute, const void *key, const RID &rid);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;
    FileHandle fileHandle;

    // Constructor
    IXFileHandle();
    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif