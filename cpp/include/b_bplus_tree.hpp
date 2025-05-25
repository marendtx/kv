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

bool byteKeyLessEqual(const ByteArray &a, const ByteArray &b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()) || a == b;
}
bool byteKeyLess(const ByteArray &a, const ByteArray &b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

ByteArray readBytes(std::ifstream &ifs) {
    int len;
    ifs.read(reinterpret_cast<char *>(&len), sizeof(int));
    if (!ifs || len < 0) {
        throw std::runtime_error("Invalid byte array length or read error.");
    }
    ByteArray data(len);
    if (len > 0) {
        ifs.read(reinterpret_cast<char *>(data.data()), len);
        if (!ifs) {
            throw std::runtime_error("Failed to read byte array data.");
        }
    }
    return data;
}

void writeBytes(std::ofstream &ofs, const ByteArray &arr) {
    int len = static_cast<int>(arr.size());
    ofs.write(reinterpret_cast<const char *>(&len), sizeof(int));
    if (len > 0)
        ofs.write(reinterpret_cast<const char *>(arr.data()), len);
}

enum class WalOp : uint8_t {
    Insert = 1,
    Remove = 2,
};

struct WalEntry {
    WalOp op;
    std::vector<std::byte> key;
    std::vector<std::byte> value;
};

void writeWalEntry(std::ofstream &ofs, const WalEntry &entry) {
    ofs.write(reinterpret_cast<const char *>(&entry.op), 1);
    writeBytes(ofs, entry.key);
    writeBytes(ofs, entry.value);
    ofs.flush(); // flush必須
    if (!ofs)
        throw std::runtime_error("WAL write failed");
}

WalEntry readWalEntry(std::ifstream &ifs) {
    WalEntry entry;
    uint8_t op = 0;
    ifs.read(reinterpret_cast<char *>(&op), 1);
    if (!ifs)
        throw std::runtime_error("WAL read error (op)");
    entry.op = static_cast<WalOp>(op);
    entry.key = readBytes(ifs);
    entry.value = readBytes(ifs);
    return entry;
}

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

class BPlusTree {
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
    BPlusTree(size_t cacheSize = MAX_CACHE_SIZE, const std::string &walFile = "tree_wal.log");
    ~BPlusTree();
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

// -- destructor --
BPlusTree::~BPlusTree() {
    flushAll();
    walWriter.close();
    lruList.clear();
    pageCache.clear();
}
BPlusTree::BPlusTree(size_t cacheSize, const std::string &walFile) : maxCacheSize(cacheSize), walFilename(walFile) {
    rootPageID = createPage(true, -1);
    walWriter.open(walFilename, std::ios::binary | std::ios::app);
    if (!walWriter)
        throw std::runtime_error("Failed to open WAL file");
}

// flush1ページ
void BPlusTree::flushPage(Page *page) {
    if (page->dirty) {
        writePageToDisk(page);
        page->dirty = false;
    }
}
// キャッシュ全flush
void BPlusTree::flushAll() {
    for (auto &pr : lruList) {
        flushPage(pr.second.get());
    }
}
// LRU更新（先頭に移動）
void BPlusTree::touchLRU(int id) {
    auto it = pageCache.find(id);
    if (it != pageCache.end()) {
        lruList.splice(lruList.begin(), lruList, it->second);
    }
}
// eviction
void BPlusTree::evictIfNeeded() {
    while (lruList.size() > maxCacheSize) {
        auto last = std::prev(lruList.end());
        Page *page = last->second.get();
        flushPage(page); // dirtyならwrite-back
        pageCache.erase(last->first);
        // delete不要（unique_ptrで自動解放）
        lruList.erase(last);
    }
}
int BPlusTree::createPage(bool isLeaf, int parentID) {
    int id;
    if (!freeList.empty()) {
        id = *freeList.begin();
        freeList.erase(freeList.begin());
    } else {
        id = nextPageID++;
    }
    auto page = std::make_unique<Page>(id, isLeaf);
    page->parentID = parentID;
    page->dirty = true; // 新規dirty
    lruList.push_front({id, std::move(page)});
    pageCache[id] = lruList.begin();
    evictIfNeeded();
    return id;
}
Page *BPlusTree::getPage(int id) {
    auto it = pageCache.find(id);
    if (it != pageCache.end()) {
        touchLRU(id);
        return it->second->second.get();
    }

    // ディスクから読み込み
    std::unique_ptr<Page> page(readPageFromDisk(id, directory));
    lruList.push_front({id, std::move(page)});
    pageCache[id] = lruList.begin();
    evictIfNeeded();
    return lruList.begin()->second.get();
}
void BPlusTree::setChildrenParentIDs(Page *parent) {
    for (int cid : parent->childrenIDs) {
        Page *child = getPage(cid);
        if (child)
            child->parentID = parent->pageID;
    }
}
int BPlusTree::findParentPageID(int currentID, int childID) {
    Page *child = getPage(childID);
    if (!child)
        return -1;
    return child->parentID;
}

void BPlusTree::insertInternal(const ByteArray &key, int parentID, int newChildID) {
    Page *parent = getPage(parentID);
    int pos = 0;
    while (pos < parent->keys.size() && byteKeyLessEqual(parent->keys[pos], key))
        pos++;
    parent->keys.insert(parent->keys.begin() + pos, key);
    parent->childrenIDs.insert(parent->childrenIDs.begin() + pos + 1, newChildID);
    parent->dirty = true;

    Page *newChild = getPage(newChildID);
    newChild->parentID = parent->pageID;
    newChild->dirty = true;

    if (parent->keys.size() < ORDER)
        return;

    int mid = (ORDER + 1) / 2;
    int newInternalID = createPage(false, parent->parentID);
    Page *newInternal = getPage(newInternalID);

    ByteArray upKey = parent->keys[mid];
    newInternal->keys.assign(parent->keys.begin() + mid + 1, parent->keys.end());
    newInternal->childrenIDs.assign(parent->childrenIDs.begin() + mid + 1, parent->childrenIDs.end());

    parent->keys.resize(mid);
    parent->childrenIDs.resize(mid + 1);

    setChildrenParentIDs(newInternal);
    setChildrenParentIDs(parent);
    parent->dirty = true;
    newInternal->dirty = true;

    if (parentID == rootPageID) {
        int newRootID = createPage(false, -1);
        Page *newRoot = getPage(newRootID);
        newRoot->keys.push_back(upKey);
        newRoot->childrenIDs.push_back(parentID);
        newRoot->childrenIDs.push_back(newInternalID);

        parent->parentID = newRootID;
        newInternal->parentID = newRootID;
        setChildrenParentIDs(newRoot);

        rootPageID = newRootID;
        newRoot->dirty = true;
        parent->dirty = true;
        newInternal->dirty = true;
    } else {
        int grandParentID = findParentPageID(rootPageID, parentID);
        insertInternal(upKey, grandParentID, newInternalID);
    }
}

void BPlusTree::rebalanceAfterDeletion(Page *node, int nodeID) {
    if (nodeID == rootPageID) {
        if (!node->isLeaf && node->childrenIDs.size() == 1) {
            rootPageID = node->childrenIDs[0];
            Page *newRoot = getPage(rootPageID);
            newRoot->parentID = -1;
            freeList.insert(nodeID);

            auto it = pageCache.find(nodeID);
            if (it != pageCache.end()) {
                lruList.erase(it->second);
                pageCache.erase(it);
            }
            // delete不要
        }
        return;
    }

    int minKeys = (ORDER + 1) / 2;
    if (node->keys.size() >= minKeys)
        return;

    int parentID = node->parentID;
    Page *parent = getPage(parentID);

    int idx = 0;
    while (idx < parent->childrenIDs.size() && parent->childrenIDs[idx] != nodeID)
        idx++;

    int leftID = (idx > 0) ? parent->childrenIDs[idx - 1] : -1;
    int rightID = (idx + 1 < parent->childrenIDs.size()) ? parent->childrenIDs[idx + 1] : -1;

    if (leftID != -1) {
        Page *left = getPage(leftID);
        if (left->keys.size() > minKeys) {
            if (node->isLeaf) {
                node->keys.insert(node->keys.begin(), left->keys.back());
                node->values.insert(node->values.begin(), left->values.back());
                left->keys.pop_back();
                left->values.pop_back();

                parent->keys[idx - 1] = node->keys[0];
                node->dirty = left->dirty = parent->dirty = true;
            } else {
                node->keys.insert(node->keys.begin(), parent->keys[idx - 1]);
                node->childrenIDs.insert(node->childrenIDs.begin(), left->childrenIDs.back());

                Page *movedChild = getPage(left->childrenIDs.back());
                movedChild->parentID = nodeID;

                parent->keys[idx - 1] = left->keys.back();

                left->keys.pop_back();
                left->childrenIDs.pop_back();
                node->dirty = left->dirty = parent->dirty = movedChild->dirty = true;
            }
            return;
        }
    }

    if (rightID != -1) {
        Page *right = getPage(rightID);
        if (right->keys.size() > minKeys) {
            if (node->isLeaf) {
                node->keys.push_back(right->keys.front());
                node->values.push_back(right->values.front());
                right->keys.erase(right->keys.begin());
                right->values.erase(right->values.begin());

                parent->keys[idx] = right->keys.front();
                node->dirty = right->dirty = parent->dirty = true;
            } else {
                node->keys.push_back(parent->keys[idx]);
                node->childrenIDs.push_back(right->childrenIDs.front());

                Page *movedChild = getPage(right->childrenIDs.front());
                movedChild->parentID = nodeID;

                parent->keys[idx] = right->keys.front();

                right->keys.erase(right->keys.begin());
                right->childrenIDs.erase(right->childrenIDs.begin());
                node->dirty = right->dirty = parent->dirty = movedChild->dirty = true;
            }
            return;
        }
    }

    if (leftID != -1) {
        Page *left = getPage(leftID);
        if (node->isLeaf) {
            left->keys.insert(left->keys.end(), node->keys.begin(), node->keys.end());
            left->values.insert(left->values.end(), node->values.begin(), node->values.end());
            left->nextLeafID = node->nextLeafID;
            left->dirty = true;
        } else {
            left->keys.push_back(parent->keys[idx - 1]);
            left->keys.insert(left->keys.end(), node->keys.begin(), node->keys.end());
            left->childrenIDs.insert(left->childrenIDs.end(), node->childrenIDs.begin(), node->childrenIDs.end());
            setChildrenParentIDs(left);
            left->dirty = true;
        }

        parent->keys.erase(parent->keys.begin() + idx - 1);
        parent->childrenIDs.erase(parent->childrenIDs.begin() + idx);
        parent->dirty = true;

        freeList.insert(nodeID);
        auto mapIt = pageCache.find(nodeID);
        if (mapIt != pageCache.end()) {
            lruList.erase(mapIt->second);
            pageCache.erase(mapIt);
        }
        // delete不要

        if (parentID != rootPageID && parent->keys.size() < minKeys)
            rebalanceAfterDeletion(parent, parentID);
        return;
    }

    if (rightID != -1) {
        Page *right = getPage(rightID);
        if (node->isLeaf) {
            node->keys.insert(node->keys.end(), right->keys.begin(), right->keys.end());
            node->values.insert(node->values.end(), right->values.begin(), right->values.end());
            node->nextLeafID = right->nextLeafID;
            node->dirty = true;
        } else {
            node->keys.push_back(parent->keys[idx]);
            node->keys.insert(node->keys.end(), right->keys.begin(), right->keys.end());
            node->childrenIDs.insert(node->childrenIDs.end(), right->childrenIDs.begin(), right->childrenIDs.end());
            setChildrenParentIDs(node);
            node->dirty = true;
        }

        parent->keys.erase(parent->keys.begin() + idx);
        parent->childrenIDs.erase(parent->childrenIDs.begin() + idx + 1);
        parent->dirty = true;

        freeList.insert(rightID);
        auto mapIt = pageCache.find(rightID);
        if (mapIt != pageCache.end()) {
            lruList.erase(mapIt->second);
            pageCache.erase(mapIt);
        }
        // delete不要

        if (parentID != rootPageID && parent->keys.size() < minKeys)
            rebalanceAfterDeletion(parent, parentID);
    }
}

void BPlusTree::writePageToDisk(Page *page) {
    if (directory.empty())
        return;
    std::string filename = directory + "/page_" + std::to_string(page->pageID) + ".bin";
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs) {
        throw std::runtime_error("Failed to open file for writing: " + filename);
    }
    ofs.write(reinterpret_cast<char *>(&page->pageID), sizeof(int));
    ofs.write(reinterpret_cast<char *>(&page->isLeaf), sizeof(bool));
    ofs.write(reinterpret_cast<char *>(&page->parentID), sizeof(int));
    int keyCount = page->keys.size();
    ofs.write(reinterpret_cast<char *>(&keyCount), sizeof(int));
    for (const auto &k : page->keys) {
        int len = k.size();
        ofs.write(reinterpret_cast<const char *>(&len), sizeof(int));
        ofs.write(reinterpret_cast<const char *>(k.data()), len);
    }
    if (page->isLeaf) {
        int valCount = page->values.size();
        ofs.write(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (const auto &v : page->values) {
            int len = v.size();
            ofs.write(reinterpret_cast<const char *>(&len), sizeof(int));
            ofs.write(reinterpret_cast<const char *>(v.data()), len);
        }
        ofs.write(reinterpret_cast<char *>(&page->nextLeafID), sizeof(int));
    } else {
        int childCount = page->childrenIDs.size();
        ofs.write(reinterpret_cast<char *>(&childCount), sizeof(int));
        for (int cid : page->childrenIDs) {
            ofs.write(reinterpret_cast<char *>(&cid), sizeof(int));
        }
    }
    ofs.close();
    if (!ofs) {
        throw std::runtime_error("Failed to finish writing page file: " + filename);
    }
}
Page *BPlusTree::readPageFromDisk(int pageID, const std::string &dir) {
    if (dir.empty())
        throw std::runtime_error("Directory for readPageFromDisk is empty");
    std::string filename = dir + "/page_" + std::to_string(pageID) + ".bin";
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) {
        throw std::runtime_error("Failed to open file for reading: " + filename);
    }
    int id;
    bool isLeaf;
    int parentID;
    ifs.read(reinterpret_cast<char *>(&id), sizeof(int));
    ifs.read(reinterpret_cast<char *>(&isLeaf), sizeof(bool));
    ifs.read(reinterpret_cast<char *>(&parentID), sizeof(int));
    if (!ifs) {
        throw std::runtime_error("Corrupt or incomplete page file: " + filename);
    }
    Page *page = new Page(id, isLeaf);
    page->parentID = parentID;
    int keyCount;
    ifs.read(reinterpret_cast<char *>(&keyCount), sizeof(int));
    if (!ifs || keyCount < 0) {
        throw std::runtime_error("Corrupt or incomplete page file (keyCount): " + filename);
    }
    for (int i = 0; i < keyCount; ++i) {
        page->keys.push_back(readBytes(ifs));
    }
    if (isLeaf) {
        int valCount;
        ifs.read(reinterpret_cast<char *>(&valCount), sizeof(int));
        if (!ifs || valCount < 0) {
            throw std::runtime_error("Corrupt or incomplete page file (valCount): " + filename);
        }
        for (int i = 0; i < valCount; ++i) {
            page->values.push_back(readBytes(ifs));
        }
        ifs.read(reinterpret_cast<char *>(&page->nextLeafID), sizeof(int));
        if (!ifs) {
            throw std::runtime_error("Corrupt or incomplete page file (nextLeafID): " + filename);
        }
    } else {
        int childCount;
        ifs.read(reinterpret_cast<char *>(&childCount), sizeof(int));
        if (!ifs || childCount < 0) {
            throw std::runtime_error("Corrupt or incomplete page file (childCount): " + filename);
        }
        for (int i = 0; i < childCount; ++i) {
            int cid;
            ifs.read(reinterpret_cast<char *>(&cid), sizeof(int));
            if (!ifs) {
                throw std::runtime_error("Corrupt or incomplete page file (childrenIDs): " + filename);
            }
            page->childrenIDs.push_back(cid);
        }
    }
    page->dirty = false;
    return page;
}
void BPlusTree::insert(const ByteArray &key, const ByteArray &value) {
    WalEntry entry{WalOp::Insert, key, value};
    writeWalEntry(walWriter, entry);

    int cursorID = rootPageID;
    Page *cursor = getPage(cursorID);

    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
    }

    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    if (it != cursor->keys.end() && !byteKeyLess(key, *it) && !byteKeyLess(*it, key)) {
        int idx = it - cursor->keys.begin();
        cursor->values[idx] = value;
        cursor->dirty = true;
        return;
    }

    int pos = it - cursor->keys.begin();
    cursor->keys.insert(it, key);
    cursor->values.insert(cursor->values.begin() + pos, value);
    cursor->dirty = true;

    if (cursor->keys.size() < ORDER)
        return;

    int newLeafID = createPage(true, cursor->parentID);
    Page *newLeaf = getPage(newLeafID);
    int mid = (ORDER + 1) / 2;

    newLeaf->keys.assign(cursor->keys.begin() + mid, cursor->keys.end());
    newLeaf->values.assign(cursor->values.begin() + mid, cursor->values.end());
    cursor->keys.resize(mid);
    cursor->values.resize(mid);

    newLeaf->nextLeafID = cursor->nextLeafID;
    cursor->nextLeafID = newLeafID;
    cursor->dirty = true;
    newLeaf->dirty = true;

    ByteArray newKey = newLeaf->keys[0];

    if (cursorID == rootPageID) {
        int newRootID = createPage(false, -1);
        Page *newRoot = getPage(newRootID);
        newRoot->keys.push_back(newKey);
        newRoot->childrenIDs.push_back(cursorID);
        newRoot->childrenIDs.push_back(newLeafID);

        cursor->parentID = newRootID;
        newLeaf->parentID = newRootID;
        setChildrenParentIDs(newRoot);

        rootPageID = newRootID;
        newRoot->dirty = true;
        cursor->dirty = true;
        newLeaf->dirty = true;
    } else {
        int parentID = cursor->parentID;
        insertInternal(newKey, parentID, newLeafID);
    }
}
ByteArray BPlusTree::search(const ByteArray &key) {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursor = getPage(cursor->childrenIDs[i]);
    }
    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    if (it != cursor->keys.end() && !byteKeyLess(key, *it) && !byteKeyLess(*it, key)) {
        int idx = it - cursor->keys.begin();
        return cursor->values[idx];
    }
    return {};
}
void BPlusTree::remove(const ByteArray &key) {
    WalEntry entry{WalOp::Remove, key, {}};
    writeWalEntry(walWriter, entry);

    Page *cursor = getPage(rootPageID);
    int cursorID = rootPageID;
    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
    }

    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    if (it != cursor->keys.end() && !byteKeyLess(key, *it) && !byteKeyLess(*it, key)) {
        int idx = it - cursor->keys.begin();
        cursor->keys.erase(cursor->keys.begin() + idx);
        cursor->values.erase(cursor->values.begin() + idx);
        cursor->dirty = true;
        rebalanceAfterDeletion(cursor, cursorID);
    }
}

void BPlusTree::insertFromWAL(const ByteArray &key, const ByteArray &value) {
    if (rootPageID == -1) {
        rootPageID = createPage(true, -1); // 必ずrootを作成
    }

    int cursorID = rootPageID;
    Page *cursor = getPage(cursorID);
    if (!cursor)
        return;

    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
        if (!cursor)
            return;
    }

    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    if (it != cursor->keys.end() && !byteKeyLess(key, *it) && !byteKeyLess(*it, key)) {
        int idx = it - cursor->keys.begin();
        cursor->values[idx] = value;
        cursor->dirty = true;
        return;
    }

    int pos = it - cursor->keys.begin();
    cursor->keys.insert(it, key);
    cursor->values.insert(cursor->values.begin() + pos, value);
    cursor->dirty = true;

    if (cursor->keys.size() < ORDER)
        return;

    int newLeafID = createPage(true, cursor->parentID);
    Page *newLeaf = getPage(newLeafID);
    if (!newLeaf)
        return;
    int mid = (ORDER + 1) / 2;

    newLeaf->keys.assign(cursor->keys.begin() + mid, cursor->keys.end());
    newLeaf->values.assign(cursor->values.begin() + mid, cursor->values.end());
    cursor->keys.resize(mid);
    cursor->values.resize(mid);

    newLeaf->nextLeafID = cursor->nextLeafID;
    cursor->nextLeafID = newLeafID;
    cursor->dirty = true;
    newLeaf->dirty = true;

    ByteArray newKey = newLeaf->keys[0];

    if (cursorID == rootPageID) {
        int newRootID = createPage(false, -1);
        Page *newRoot = getPage(newRootID);
        newRoot->keys.push_back(newKey);
        newRoot->childrenIDs.push_back(cursorID);
        newRoot->childrenIDs.push_back(newLeafID);

        cursor->parentID = newRootID;
        newLeaf->parentID = newRootID;
        setChildrenParentIDs(newRoot);

        rootPageID = newRootID;
        newRoot->dirty = true;
        cursor->dirty = true;
        newLeaf->dirty = true;
    } else {
        int parentID = cursor->parentID;
        insertInternal(newKey, parentID, newLeafID);
    }
}
void BPlusTree::removeFromWAL(const ByteArray &key) {
    if (rootPageID == -1)
        return;
    Page *cursor = getPage(rootPageID);
    if (!cursor)
        return;
    int cursorID = rootPageID;
    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
        if (!cursor)
            return;
    }

    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    if (it != cursor->keys.end() && !byteKeyLess(key, *it) && !byteKeyLess(*it, key)) {
        int idx = it - cursor->keys.begin();
        cursor->keys.erase(cursor->keys.begin() + idx);
        cursor->values.erase(cursor->values.begin() + idx);
        cursor->dirty = true;
        rebalanceAfterDeletion(cursor, cursorID);
    }
}

void BPlusTree::recoverFromWAL(const std::string &dir) {
    flushAll();
    lruList.clear();
    pageCache.clear();
    freeList.clear();
    rootPageID = -1;
    directory = dir;
    std::error_code ec;
    std::filesystem::create_directory(dir, ec);
    if (ec)
        throw std::runtime_error("Failed to create directory: " + dir);

    std::ifstream walRead(walFilename, std::ios::binary);
    if (!walRead)
        return;

    while (true) {
        try {
            WalEntry entry = readWalEntry(walRead);
            if (entry.op == WalOp::Insert) {
                insertFromWAL(entry.key, entry.value);
            } else if (entry.op == WalOp::Remove) {
                removeFromWAL(entry.key);
            }
        } catch (const std::exception &e) {
            if (walRead.eof() || walRead.peek() == EOF) {
                // 通常終了
                std::cerr << "[WAL RECOVERY] complete: " << e.what() << std::endl;
            } else {
                // それ以外の失敗は本当のエラー
                std::cerr << "[WAL RECOVERY] stop: " << e.what() << std::endl;
            }
            break;
        }
    }

    if (rootPageID == -1) {
        rootPageID = createPage(true, -1);
    }
}

void BPlusTree::saveTree(const std::string &dir) {
    directory = dir;
    std::error_code ec;
    std::filesystem::create_directory(dir, ec);
    if (ec)
        throw std::runtime_error("Failed to create directory: " + dir);
    std::string metaFilename = dir + "/meta.bin";
    std::ofstream meta(metaFilename, std::ios::binary);
    if (!meta)
        throw std::runtime_error("Failed to open meta file for writing: " + metaFilename);
    meta.write(reinterpret_cast<char *>(&rootPageID), sizeof(int));
    meta.write(reinterpret_cast<char *>(&nextPageID), sizeof(int));
    int freeCount = freeList.size();
    meta.write(reinterpret_cast<char *>(&freeCount), sizeof(int));
    for (int id : freeList)
        meta.write(reinterpret_cast<char *>(&id), sizeof(int));
    meta.close();
    if (!meta)
        throw std::runtime_error("Failed to finish writing meta file: " + metaFilename);

    flushAll();
}
void BPlusTree::loadTree(const std::string &dir) {
    flushAll(); // 念のため
    lruList.clear();
    pageCache.clear();
    freeList.clear();
    directory = dir;
    std::string metaFilename = dir + "/meta.bin";
    std::ifstream meta(metaFilename, std::ios::binary);
    if (!meta)
        throw std::runtime_error("Failed to open meta file for reading: " + metaFilename);
    meta.read(reinterpret_cast<char *>(&rootPageID), sizeof(int));
    meta.read(reinterpret_cast<char *>(&nextPageID), sizeof(int));
    int freeCount;
    meta.read(reinterpret_cast<char *>(&freeCount), sizeof(int));
    if (!meta)
        throw std::runtime_error("Corrupt or incomplete meta file: " + metaFilename);
    for (int i = 0; i < freeCount; ++i) {
        int id;
        meta.read(reinterpret_cast<char *>(&id), sizeof(int));
        if (!meta)
            throw std::runtime_error("Corrupt meta file (freeList): " + metaFilename);
        freeList.insert(id);
    }
    meta.close();
}

void BPlusTree::visualize() {
    std::function<void(int, std::string, bool)> dfs =
        [&](int pageID, std::string prefix, bool isLast) {
            Page *p = getPage(pageID);
            std::cout << prefix;
            std::cout << (isLast ? "└── " : "├── ");
            std::string nodeType = p->isLeaf ? "Leaf" : "Internal";
            std::cout << "[" << nodeType << " " << pageID << "] Keys: ";
            for (size_t i = 0; i < p->keys.size(); ++i) {
                std::string ks(reinterpret_cast<const char *>(p->keys[i].data()), p->keys[i].size());
                std::cout << "\"" << ks << "\"";
                if (i + 1 < p->keys.size())
                    std::cout << ", ";
            }
            if (p->isLeaf) {
                if (p->nextLeafID != -1)
                    std::cout << " → Leaf " << p->nextLeafID;
            }
            std::cout << "\n";
            if (p->isLeaf) {
                std::cout << prefix << (isLast ? "    " : "│   ");
                std::cout << "    Values: ";
                for (size_t i = 0; i < p->values.size(); ++i) {
                    std::string vs(reinterpret_cast<const char *>(p->values[i].data()), p->values[i].size());
                    std::cout << "\"" << vs << "\"";
                    if (i + 1 < p->values.size())
                        std::cout << ", ";
                }
                std::cout << "\n";
                return;
            }
            for (size_t i = 0; i < p->childrenIDs.size(); ++i) {
                bool last = (i == p->childrenIDs.size() - 1);
                dfs(p->childrenIDs[i], prefix + (isLast ? "    " : "│   "), last);
            }
        };
    std::cout << "B+ Tree Structure\n";
    dfs(rootPageID, "", true);
}
std::vector<std::pair<ByteArray, ByteArray>> BPlusTree::rangeSearch(const ByteArray &min, const ByteArray &max) {
    std::vector<std::pair<ByteArray, ByteArray>> result;
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), min, byteKeyLess);
        int i = it - cursor->keys.begin();
        cursor = getPage(cursor->childrenIDs[i]);
    }
    // minの位置から始める
    auto it = std::lower_bound(cursor->keys.begin(), cursor->keys.end(), min, byteKeyLess);
    size_t idx = it - cursor->keys.begin();
    while (cursor) {
        for (size_t i = idx; i < cursor->keys.size(); ++i) {
            if (byteKeyLess(max, cursor->keys[i]))
                return result;
            result.emplace_back(cursor->keys[i], cursor->values[i]);
        }
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
        idx = 0;
    }
    return result;
}
void BPlusTree::traverse() {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        cursor = getPage(cursor->childrenIDs[0]);
    }
    while (cursor) {
        for (size_t i = 0; i < cursor->keys.size(); ++i) {
            std::string k(reinterpret_cast<const char *>(cursor->keys[i].data()), cursor->keys[i].size());
            std::string v(reinterpret_cast<const char *>(cursor->values[i].data()), cursor->values[i].size());
            std::cout << k << " → " << v << "  ";
        }
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
    }
    std::cout << std::endl;
}

bool BPlusTree::checkParentPointers() {
    std::set<int> visited;
    std::function<bool(int, int)> dfs = [&](int pageID, int parentID) {
        // getPageはconst外れるのでここはreadPageFromDiskを使うのが安全ですが、単体テスト時はキャッシュでOK
        auto it = pageCache.find(pageID);
        if (it == pageCache.end())
            return false;
        const Page *page = it->second->second.get();
        if (page->parentID != parentID && parentID != -2)
            return false; // -2: rootの親は除外
        visited.insert(pageID);
        if (!page->isLeaf) {
            for (int cid : page->childrenIDs) {
                if (!dfs(cid, pageID))
                    return false;
            }
        }
        return true;
    };
    bool ok = dfs(rootPageID, -2); // rootの親は特殊値
    // 孤立ノードがないかも念のため
    if (visited.size() != lruList.size())
        return false;
    return ok;
}

bool BPlusTree::checkAllParentPointersStrict() {
    std::unordered_map<int, std::unique_ptr<Page>> allPages;
    for (const auto &entry : std::filesystem::directory_iterator(directory)) {
        auto path = entry.path().string();
        if (path.find("/page_") != std::string::npos && path.find(".bin") != std::string::npos) {
            size_t s = path.find("/page_") + 6;
            size_t e = path.find(".bin");
            int pid = std::stoi(path.substr(s, e - s));
            try {
                allPages[pid] = std::unique_ptr<Page>(BPlusTree::readPageFromDisk(pid, directory));
            } catch (...) {
                return false;
            }
        }
    }
    // (a) RootからDFSで全ノード到達性/循環チェック
    std::set<int> visited;
    std::function<bool(int, int)> dfs = [&](int pid, int parentID) {
        if (visited.count(pid))
            return false; // サイクル検出
        visited.insert(pid);
        const Page *page = allPages[pid].get();
        // parent pointer check
        if (pid == rootPageID) {
            if (page->parentID != -1)
                return false;
        } else {
            if (page->parentID != parentID)
                return false;
            if (allPages.count(page->parentID) == 0)
                return false;
            // 親のchildrenIDsに自分が含まれるか
            const Page *parent = allPages.at(page->parentID).get();
            if (std::find(parent->childrenIDs.begin(), parent->childrenIDs.end(), pid) == parent->childrenIDs.end())
                return false;
        }
        // (c) Key昇順チェック
        if (!std::is_sorted(page->keys.begin(), page->keys.end(), byteKeyLess))
            return false;
        // 子の親参照整合性
        if (!page->isLeaf) {
            for (int cid : page->childrenIDs) {
                if (allPages.count(cid) == 0)
                    return false;
                const Page *child = allPages.at(cid).get();
                if (child->parentID != pid)
                    return false;
                if (!dfs(cid, pid))
                    return false;
            }
        }
        return true;
    };
    if (!dfs(rootPageID, -1))
        return false;
    // (a) DFSで全ノードを訪問できたか
    if (visited.size() != allPages.size())
        return false; // 孤立ノード検出
    return true;
}
