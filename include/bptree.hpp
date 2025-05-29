#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

const int ORDER = 32;
constexpr size_t MAX_CACHE_SIZE = 1024; // ページキャッシュ上限

using ByteArray = std::vector<std::byte>;

bool byteKeyLessEqual(const ByteArray &a, const ByteArray &b);
bool byteKeyLess(const ByteArray &a, const ByteArray &b);

ByteArray readBytes(std::ifstream &ifs);
void writeBytes(std::ofstream &ofs, const ByteArray &arr);

ByteArray toBytes(const std::string &s);

std::string fromBytes(const ByteArray &b);

enum class WalOp : uint8_t {
    Insert = 1,
    Remove = 2,
};

struct WalEntry {
    WalOp op;
    std::vector<std::byte> key;
    std::vector<std::byte> value;
};

void writeWalEntry(std::ofstream &ofs, const WalEntry &entry);

WalEntry readWalEntry(std::ifstream &ifs);

struct Page {
    int pageID;
    bool isLeaf;
    std::vector<ByteArray> keys;
    std::vector<ByteArray> values;
    std::vector<int> childrenIDs;
    int nextLeafID = -1;
    int parentID = -1;
    bool dirty = false; // dirtyフラグ

    Page(int id, bool leaf) : pageID(id), isLeaf(leaf) {}
};

class BPTree {
private:
    int rootPageID;
    int nextPageID = 0;

    // unique_ptrベース
    std::list<std::pair<int, std::unique_ptr<Page>>> lruList;                                      // LRUリスト(先頭が最新)
    std::unordered_map<int, std::list<std::pair<int, std::unique_ptr<Page>>>::iterator> pageCache; // pageID->list iterator

    std::set<int> freeList;
    std::string directory;
    size_t maxCacheSize;

    std::string walFilename = "tree_wal.log";
    std::ofstream walWriter;

    int createPage(bool isLeaf, int parentID = -1);
    Page *getPage(int id);
    void touchLRU(int id);
    void insertInternal(const ByteArray &key, int parentID, int newChildID);
    void rebalanceAfterDeletion(Page *cursor, int cursorID);
    int findParentPageID(int currentID, int childID);
    void writePageToDisk(Page *page);
    static Page *readPageFromDisk(int pageID, const std::string &dir);
    void setChildrenParentIDs(Page *parent);
    void evictIfNeeded();
    void flushPage(Page *page);
    void flushAll();

public:
    BPTree(size_t cacheSize = MAX_CACHE_SIZE, const std::string &walFile = "tree_wal.log");
    ~BPTree();
    void insert(const ByteArray &key, const ByteArray &value);
    ByteArray search(const ByteArray &key);
    void remove(const ByteArray &key);
    void insertFromWAL(const ByteArray &key, const ByteArray &value);
    void removeFromWAL(const ByteArray &key);
    void recoverFromWAL(const std::string &dir);
    void saveTree(const std::string &dir);
    void loadTree(const std::string &dir);
    void visualize();
    std::vector<std::pair<ByteArray, ByteArray>> rangeSearch(const ByteArray &min, const ByteArray &max);
    void traverse();
    bool checkParentPointers();
    bool checkAllParentPointersStrict();
};