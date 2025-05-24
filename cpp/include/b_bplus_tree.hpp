#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

const int ORDER = 4;
using ByteArray = std::vector<uint8_t>;

// --- Page 構造体 ---
struct Page {
    int pageID;
    bool isLeaf;
    std::vector<ByteArray> keys;
    std::vector<ByteArray> values;
    std::vector<int> childrenIDs;
    int nextLeafID = -1;
    Page(int id, bool leaf) : pageID(id), isLeaf(leaf) {}
};

// --- 補助関数 ---
bool byteKeyLessEqual(const ByteArray &a, const ByteArray &b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()) || a == b;
}

bool byteKeyLess(const ByteArray &a, const ByteArray &b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

ByteArray readBytes(std::ifstream &ifs) {
    int len;
    ifs.read(reinterpret_cast<char *>(&len), sizeof(int));
    ByteArray data(len);
    if (len > 0)
        ifs.read(reinterpret_cast<char *>(data.data()), len);
    return data;
}

// --- B+Tree クラス ---
class BPlusTree {
private:
    int rootPageID;
    int nextPageID = 0;
    std::unordered_map<int, Page *> pageCache;
    std::set<int> freeList;
    std::string directory;

    int createPage(bool isLeaf);
    Page *getPage(int id);
    void insertInternal(const ByteArray &key, int parentID, int newChildID);
    void rebalanceAfterDeletion(Page *cursor, int cursorID);
    int findParentPageID(int currentID, int childID);
    void writePageToDisk(Page *page);
    Page *readPageFromDisk(int pageID, const std::string &dir);

public:
    BPlusTree();
    void insert(const ByteArray &key, const ByteArray &value);
    ByteArray search(const ByteArray &key);
    void remove(const ByteArray &key);
    void saveTree(const std::string &dir);
    void loadTree(const std::string &dir);
    void visualize();
    std::vector<std::pair<ByteArray, ByteArray>> rangeSearch(const ByteArray &min, const ByteArray &max);
    void traverse();
};

// --- コンストラクタ ---
BPlusTree::BPlusTree() {
    rootPageID = createPage(true);
}

int BPlusTree::createPage(bool isLeaf) {
    int id;
    if (!freeList.empty()) {
        id = *freeList.begin();
        freeList.erase(freeList.begin());
    } else {
        id = nextPageID++;
    }
    pageCache[id] = new Page(id, isLeaf);
    return id;
}
void BPlusTree::rebalanceAfterDeletion(Page *cursor, int cursorID) {
    if (cursorID == rootPageID) {
        if (!cursor->isLeaf && cursor->childrenIDs.size() == 1) {
            rootPageID = cursor->childrenIDs[0];
            freeList.insert(cursorID);
            pageCache.erase(cursorID);
            delete cursor;
        }
        return;
    }

    if (cursor->keys.size() >= (ORDER + 1) / 2)
        return;

    int parentID = findParentPageID(rootPageID, cursorID);
    Page *parent = getPage(parentID);
    int index = 0;
    while (index < parent->childrenIDs.size() && parent->childrenIDs[index] != cursorID)
        ++index;

    int leftID = (index > 0) ? parent->childrenIDs[index - 1] : -1;
    int rightID = (index + 1 < parent->childrenIDs.size()) ? parent->childrenIDs[index + 1] : -1;

    Page *sibling = nullptr;
    int siblingID = -1;
    bool isLeft = false;

    if (leftID != -1 && getPage(leftID)->keys.size() > (ORDER + 1) / 2) {
        sibling = getPage(leftID);
        siblingID = leftID;
        isLeft = true;
    } else if (rightID != -1 && getPage(rightID)->keys.size() > (ORDER + 1) / 2) {
        sibling = getPage(rightID);
        siblingID = rightID;
        isLeft = false;
    } else {
        // マージ
        if (leftID != -1) {
            sibling = getPage(leftID);
            siblingID = leftID;
            isLeft = true;
        } else if (rightID != -1) {
            sibling = getPage(rightID);
            siblingID = rightID;
            isLeft = false;
        }

        if (sibling) {
            if (isLeft) {
                sibling->keys.insert(sibling->keys.end(), cursor->keys.begin(), cursor->keys.end());
                sibling->values.insert(sibling->values.end(), cursor->values.begin(), cursor->values.end());
                sibling->nextLeafID = cursor->nextLeafID;
            } else {
                cursor->keys.insert(cursor->keys.end(), sibling->keys.begin(), sibling->keys.end());
                cursor->values.insert(cursor->values.end(), sibling->values.begin(), sibling->values.end());
                cursor->nextLeafID = sibling->nextLeafID;
            }

            freeList.insert(isLeft ? cursorID : siblingID);
            pageCache.erase(isLeft ? cursorID : siblingID);
            delete (isLeft ? cursor : sibling);

            parent->keys.erase(parent->keys.begin() + (isLeft ? index - 1 : index));
            parent->childrenIDs.erase(parent->childrenIDs.begin() + (isLeft ? index : index + 1));

            if (parentID != rootPageID && parent->keys.size() < (ORDER + 1) / 2) {
                rebalanceAfterDeletion(parent, parentID);
            }
        }
    }
}

int BPlusTree::findParentPageID(int currentID, int childID) {
    Page *current = getPage(currentID);
    if (current->isLeaf)
        return -1;
    for (int cid : current->childrenIDs) {
        if (cid == childID)
            return currentID;
        int res = findParentPageID(cid, childID);
        if (res != -1)
            return res;
    }
    return -1;
}

void BPlusTree::writePageToDisk(Page *page) {
    std::string filename = directory + "/page_" + std::to_string(page->pageID) + ".bin";
    std::ofstream ofs(filename, std::ios::binary);
    ofs.write(reinterpret_cast<char *>(&page->pageID), sizeof(int));
    ofs.write(reinterpret_cast<char *>(&page->isLeaf), sizeof(bool));
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
}

Page *BPlusTree::readPageFromDisk(int pageID, const std::string &dir) {
    std::string filename = dir + "/page_" + std::to_string(pageID) + ".bin";
    std::ifstream ifs(filename, std::ios::binary);
    int id;
    bool isLeaf;
    ifs.read(reinterpret_cast<char *>(&id), sizeof(int));
    ifs.read(reinterpret_cast<char *>(&isLeaf), sizeof(bool));
    Page *page = new Page(id, isLeaf);
    int keyCount;
    ifs.read(reinterpret_cast<char *>(&keyCount), sizeof(int));
    for (int i = 0; i < keyCount; ++i) {
        page->keys.push_back(readBytes(ifs));
    }
    if (isLeaf) {
        int valCount;
        ifs.read(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (int i = 0; i < valCount; ++i) {
            page->values.push_back(readBytes(ifs));
        }
        ifs.read(reinterpret_cast<char *>(&page->nextLeafID), sizeof(int));
    } else {
        int childCount;
        ifs.read(reinterpret_cast<char *>(&childCount), sizeof(int));
        for (int i = 0; i < childCount; ++i) {
            int cid;
            ifs.read(reinterpret_cast<char *>(&cid), sizeof(int));
            page->childrenIDs.push_back(cid);
        }
    }
    return page;
}

void BPlusTree::remove(const ByteArray &key) {
    Page *cursor = getPage(rootPageID);
    int cursorID = rootPageID;
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && byteKeyLessEqual(cursor->keys[i], key))
            i++;
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
    }
    for (size_t i = 0; i < cursor->keys.size(); ++i) {
        if (cursor->keys[i] == key) {
            cursor->keys.erase(cursor->keys.begin() + i);
            cursor->values.erase(cursor->values.begin() + i);
            rebalanceAfterDeletion(cursor, cursorID);
            return;
        }
    }
}

void BPlusTree::saveTree(const std::string &dir) {
    directory = dir;
    std::filesystem::create_directory(dir);
    std::ofstream meta(dir + "/meta.bin", std::ios::binary);
    meta.write(reinterpret_cast<char *>(&rootPageID), sizeof(int));
    meta.write(reinterpret_cast<char *>(&nextPageID), sizeof(int));
    int freeCount = freeList.size();
    meta.write(reinterpret_cast<char *>(&freeCount), sizeof(int));
    for (int id : freeList)
        meta.write(reinterpret_cast<char *>(&id), sizeof(int));
    meta.close();
    for (const auto &[id, page] : pageCache) {
        if (freeList.find(id) == freeList.end()) {
            writePageToDisk(page);
        }
    }
}

void BPlusTree::loadTree(const std::string &dir) {
    directory = dir;
    pageCache.clear();
    freeList.clear();
    std::ifstream meta(dir + "/meta.bin", std::ios::binary);
    meta.read(reinterpret_cast<char *>(&rootPageID), sizeof(int));
    meta.read(reinterpret_cast<char *>(&nextPageID), sizeof(int));
    int freeCount;
    meta.read(reinterpret_cast<char *>(&freeCount), sizeof(int));
    for (int i = 0; i < freeCount; ++i) {
        int id;
        meta.read(reinterpret_cast<char *>(&id), sizeof(int));
        freeList.insert(id);
    }
    meta.close();
}

void BPlusTree::visualize() {
    std::function<void(int, int)> dfs = [&](int pageID, int depth) {
        Page *p = getPage(pageID);
        std::string indent(depth * 2, ' ');
        std::cout << indent << (p->isLeaf ? "Leaf" : "Internal") << " Page " << pageID << " Keys: ";
        for (const auto &k : p->keys) {
            std::string ks(k.begin(), k.end());
            std::cout << ks << " ";
        }
        std::cout << "\n";
        if (!p->isLeaf) {
            for (int cid : p->childrenIDs)
                dfs(cid, depth + 1);
        }
    };
    dfs(rootPageID, 0);
}

Page *BPlusTree::getPage(int id) {
    if (pageCache.find(id) == pageCache.end()) {
        pageCache[id] = readPageFromDisk(id, directory);
    }
    return pageCache[id];
}

// --- insert 実装 ---
void BPlusTree::insert(const ByteArray &key, const ByteArray &value) {
    int cursorID = rootPageID;
    Page *cursor = getPage(cursorID);

    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && byteKeyLessEqual(cursor->keys[i], key))
            i++;
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
    }

    auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key, byteKeyLess);
    int pos = it - cursor->keys.begin();
    cursor->keys.insert(it, key);
    cursor->values.insert(cursor->values.begin() + pos, value);

    if (cursor->keys.size() < ORDER)
        return;

    int newLeafID = createPage(true);
    Page *newLeaf = getPage(newLeafID);
    int mid = (ORDER + 1) / 2;

    newLeaf->keys.assign(cursor->keys.begin() + mid, cursor->keys.end());
    newLeaf->values.assign(cursor->values.begin() + mid, cursor->values.end());
    cursor->keys.resize(mid);
    cursor->values.resize(mid);

    newLeaf->nextLeafID = cursor->nextLeafID;
    cursor->nextLeafID = newLeafID;

    ByteArray newKey = newLeaf->keys[0];

    if (cursorID == rootPageID) {
        int newRootID = createPage(false);
        Page *newRoot = getPage(newRootID);
        newRoot->keys.push_back(newKey);
        newRoot->childrenIDs.push_back(cursorID);
        newRoot->childrenIDs.push_back(newLeafID);
        rootPageID = newRootID;
    } else {
        int parentID = findParentPageID(rootPageID, cursorID);
        insertInternal(newKey, parentID, newLeafID);
    }
}

void BPlusTree::insertInternal(const ByteArray &key, int parentID, int newChildID) {
    Page *parent = getPage(parentID);
    int pos = 0;
    while (pos < parent->keys.size() && byteKeyLessEqual(parent->keys[pos], key))
        pos++;

    parent->keys.insert(parent->keys.begin() + pos, key);
    parent->childrenIDs.insert(parent->childrenIDs.begin() + pos + 1, newChildID);

    if (parent->keys.size() < ORDER)
        return;

    int mid = (ORDER + 1) / 2;
    int newInternalID = createPage(false);
    Page *newInternal = getPage(newInternalID);

    ByteArray upKey = parent->keys[mid];
    newInternal->keys.assign(parent->keys.begin() + mid + 1, parent->keys.end());
    newInternal->childrenIDs.assign(parent->childrenIDs.begin() + mid + 1, parent->childrenIDs.end());

    parent->keys.resize(mid);
    parent->childrenIDs.resize(mid + 1);

    if (parentID == rootPageID) {
        int newRootID = createPage(false);
        Page *newRoot = getPage(newRootID);
        newRoot->keys.push_back(upKey);
        newRoot->childrenIDs.push_back(parentID);
        newRoot->childrenIDs.push_back(newInternalID);
        rootPageID = newRootID;
    } else {
        int grandParentID = findParentPageID(rootPageID, parentID);
        insertInternal(upKey, grandParentID, newInternalID);
    }
}

// --- search 実装 ---
ByteArray BPlusTree::search(const ByteArray &key) {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && byteKeyLessEqual(cursor->keys[i], key))
            i++;
        cursor = getPage(cursor->childrenIDs[i]);
    }

    for (size_t i = 0; i < cursor->keys.size(); ++i) {
        if (cursor->keys[i] == key)
            return cursor->values[i];
    }
    return {};
}

std::vector<std::pair<ByteArray, ByteArray>> BPlusTree::rangeSearch(const ByteArray &min, const ByteArray &max) {
    std::vector<std::pair<ByteArray, ByteArray>> result;
    Page *cursor = getPage(rootPageID);

    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && byteKeyLessEqual(cursor->keys[i], min))
            i++;
        cursor = getPage(cursor->childrenIDs[i]);
    }

    while (cursor) {
        for (size_t i = 0; i < cursor->keys.size(); ++i) {
            if (byteKeyLess(max, cursor->keys[i]))
                return result;
            if (byteKeyLessEqual(min, cursor->keys[i]))
                result.emplace_back(cursor->keys[i], cursor->values[i]);
        }
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
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
            std::string k(cursor->keys[i].begin(), cursor->keys[i].end());
            std::string v(cursor->values[i].begin(), cursor->values[i].end());
            std::cout << k << " → " << v << "  ";
        }
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
    }
    std::cout << std::endl;
}
