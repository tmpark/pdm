#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>


#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef signed SlotOffset;
typedef char NodeType;
typedef unsigned short NumOfEnt;

#define ROOT_NODE 'R'
#define INTER_NODE 'I'
#define LEAF_NODE 'L'
#define OVER_NODE 'O'

typedef enum { MoveForward = 0, MoveBackward} MoveDirection;


//freespace, number of entries, tombstone, node type, parent page num, left sibling page num, right sibling page num, left child page num
#define PAGE_DIC_SIZE (sizeof(SlotOffset) + sizeof(NumOfEnt) + sizeof(PageNum) + sizeof(NodeType) + sizeof(PageNum) + sizeof(PageNum) + sizeof(PageNum) + sizeof(PageNum))
#define OVERFLOW_PAGE_DIC_SIZE (sizeof(SlotOffset) + sizeof(NumOfEnt) + sizeof(PageNum) + sizeof(NodeType))
#define WHOLE_SIZE_FOR_ENTRIES (PAGE_SIZE - PAGE_DIC_SIZE)


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

	SlotOffset getFreeSpaceOffset(const void* pageToProcess) const;
	RC setFreeSpaceOffset(void* pageToProcess, SlotOffset freeSpaceOffset);
	NumOfEnt getNumOfEnt(const void* pageToProcess) const;
	RC setNumOfEnt(void* pageToProcess, NumOfEnt numOfEnt);
	PageNum getTombstone(const void* pageToProcess) const;
	RC setTombstone(void* pageToProcess, PageNum tombstone);
	NodeType getNodeType(const void* pageToProcess) const;
	RC setNodeType(void* pageToProcess, NodeType nodeType);
	PageNum getParentPageNum(const void* pageToProcess) const;
	RC setParentPageNum(void* pageToProcess, PageNum parentPageNum);
	PageNum getLeftSiblingPageNum(const void* pageToProcess) const;
	RC setLeftSiblingPageNum(void* pageToProcess, PageNum leftSiblingPageNum);
	PageNum getRightSiblingPageNum(const void* pageToProcess) const;
	RC setRightSiblingPageNum(void* pageToProcess, PageNum rightSiblingPageNum);
	PageNum getLeftMostChildPageNum(const void* pageToProcess) const;
	RC setLeftMostChildPageNum(void* pageToProcess, PageNum leftChildPageNum);
	unsigned getFreeSpaceSize(void* pageToProcess) const;
	unsigned getFreeSpaceSizeForOverflowPage(void* pageToProcess) const;
	RC getRIDInOverFlowPage(const void* pageToProcess, unsigned entryNum, RID &rid) const;
	RC setRIDInOverFlowPage(const void* pageToProcess, unsigned entryNum, const RID &rid);

	/*******Entrywide helper functions********/
	template <typename T>
	RC getKeyOfEntry(const void* entryToProcess, T &value) const;
	RC getKeyOfEntry(const void* entryToProcess, string &value) const;

	template <typename T>
	RC setKeyOfEntry(void* entryToProcess, T value);
	RC setKeyOfEntry(void* entryToProcess, string value);

	RC copyKeyOfEntry(void *to, void *from, AttrType keyType);

	PageNum getChildOfIntermediateEntry(const void* entryToProcess, AttrType keyType) const;
	RC setChildOfIntermediateEntry(void* entryToProcess, AttrType keyType, PageNum childPageNum);


	NumOfEnt getNumOfRIDsInLeafEntry(const void* entryToProcess, AttrType keyType) const;
	RC setNumOfRIDsInLeaf(const void* entryToProcess, AttrType keyType, NumOfEnt numOfRids);

	RC getRIDInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid) const;
	RC setRIDInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum, RID &rid);

	SlotOffset getRIDOffsetInLeaf(const void* entryToProcess, AttrType keyType, unsigned entryNum) const;


	unsigned calNewLeafEntrySize(const void* key, AttrType keyType) const;
	unsigned calNewInterEntrySize(const void* key, AttrType keyType) const;

	unsigned getSizeOfEntryInLeaf(const void* entryToProcess, AttrType keyType) const;
	unsigned getSizeOfEntryInIntermediate(const void* entryToProcess, AttrType keyType) const;

	SlotOffset findEntryOffsetToProcess(void *pageToProcess,AttrType attrType, const void *key);

	string extractVarChar(const void* data);
	RC moveEntries(void *pageToProcess,SlotOffset from,unsigned amountToMove, MoveDirection direction);

	bool compareKeys(const void *key,CompOp op, const void *entryToProcess,  AttrType keyType) const;


	RC _deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
			PageNum currentNodePage, void *newChildNodeKey, PageNum &newChildNodePage);
	RC _insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid,
			PageNum currentNodePage, void *newChildNodeKey, PageNum &newChildNodePage);
	RC putEntryInItermediate(void *entryToProcess, AttrType attrType, const void *key, PageNum pageNum);
	RC putEntryInLeaf(void *entryToProcess, AttrType attrType, const void *key, RID rid, bool existing);


	RC insertEntryInOverflowPage(IXFileHandle &ixfileHandle,PageNum currentNodePage,void *pageToProcess,const RID &rid);
	RC deleteEntryInOverflowPage(IXFileHandle &ixfileHandle,PageNum currentNodePage,void *pageToProcess,const RID &rid);

	char tempPage[PAGE_SIZE];
	char tempOverFlowPage[PAGE_SIZE];

protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;

	RC splitLeaf(void *leafNode, void *newLeafNode, void * newChildEntry,
			PageNum leafNodePN, PageNum newLeafNodePN,
			int offset, const Attribute &Attribute, const void *key, const RID &rid);
	RC splitIntermediate(void *interNode, void *newInterNode, void *newRootNode,
			void *newChildEntry, void *entry, const AttrType entryType, PageNum interNodePN,
			PageNum newInterNodePN, int offset);
	void _printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute,
			PageNum pageNum, void *page, int numOfTabs, bool last) const;
	void _printLeafNode(IXFileHandle &ixfileHandle, const Attribute &attribute,
				PageNum pageNum, void *page, int numOfTabs, bool last) const;
	void tab(int numOfTabs) const;
};


class IX_ScanIterator {

public:

	// Constructor
	IX_ScanIterator();
	// Destructor
	~IX_ScanIterator();

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);
	RC _getNextEntryFromOverflow(void *pageToProcess, RID &rid);

	// Terminate index scan
	RC close();

	IndexManager *indexManager;

	FileHandle *fileHandle;
	const void *until;
	CompOp op;

	AttrType keyType;

	void *tempPage;
	SlotOffset entryOffset;
	PageNum tombStone;

	//entry  thing
	SlotOffset currentSlot;

	//overflow page things
	void *tempOverFlowPage;
	NumOfEnt numOfRidsInOverflow;
	SlotOffset currentOverFlowSlot;
	PageNum tombStoneInOverflow;

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
