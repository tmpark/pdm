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

        RC setEntryInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid);
        RC getEntryInLeaf(const void* entryToProcess, AttrType keyType,unsigned entryNum, RID &rid);
        RC setNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType, NumOfEnt numOfRids);
        NumOfEnt getNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType);
        template <typename T>
        RC setChildOfIntermediateEntry(void* entryToProcess, AttrType keyType, PageNum childPageNum);
        template <typename T>
        PageNum getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType);
        template <typename T>
        RC setKeyOfIntermediateEntry(void* entryToProcess, AttrType type, T value);
        template <typename T>
        T getKeyOfEntry(const void* entryToProcess, AttrType type);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        char temp0[PAGE_SIZE];
        char temp1[PAGE_SIZE];
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
