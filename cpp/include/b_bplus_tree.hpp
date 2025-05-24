#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

const int ORDER = 4;

struct Page {
    int pageID;
    bool isLeaf;
    std::vector<int> keys;
    std::vector<std::string> values;
    std::vector<int> childrenIDs;
    int nextLeafID = -1;

    Page(int id, bool leaf) : pageID(id), isLeaf(leaf) {}
};

// Disk helpers
void writeString(std::ofstream &ofs, const std::string &str) {
    int len = str.size();
    ofs.write(reinterpret_cast<char *>(&len), sizeof(int));
    ofs.write(str.data(), len);
}

std::string readString(std::ifstream &ifs) {
    int len;
    ifs.read(reinterpret_cast<char *>(&len), sizeof(int));
    std::string str(len, ' ');
    ifs.read(&str[0], len);
    return str;
}

void writePageToDisk(Page *page, const std::string &dir) {
    std::string filename = dir + "/page_" + std::to_string(page->pageID) + ".bin";
    std::ofstream ofs(filename, std::ios::binary);
    ofs.write(reinterpret_cast<char *>(&page->pageID), sizeof(int));
    ofs.write(reinterpret_cast<char *>(&page->isLeaf), sizeof(bool));
    int keyCount = page->keys.size();
    ofs.write(reinterpret_cast<char *>(&keyCount), sizeof(int));
    for (int k : page->keys)
        ofs.write(reinterpret_cast<char *>(&k), sizeof(int));
    if (page->isLeaf) {
        int valCount = page->values.size();
        ofs.write(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (const std::string &val : page->values)
            writeString(ofs, val);
        ofs.write(reinterpret_cast<char *>(&page->nextLeafID), sizeof(int));
    } else {
        int childCount = page->childrenIDs.size();
        ofs.write(reinterpret_cast<char *>(&childCount), sizeof(int));
        for (int cid : page->childrenIDs)
            ofs.write(reinterpret_cast<char *>(&cid), sizeof(int));
    }
}

Page *readPageFromDisk(int pageID, const std::string &dir) {
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
        int k;
        ifs.read(reinterpret_cast<char *>(&k), sizeof(int));
        page->keys.push_back(k);
    }
    if (isLeaf) {
        int valCount;
        ifs.read(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (int i = 0; i < valCount; ++i)
            page->values.push_back(readString(ifs));
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

class BPlusTree {
private:
    int rootPageID;
    int nextPageID = 0;
    std::unordered_map<int, Page *> pageCache;
    std::vector<int> freeList;

    int createPage(bool isLeaf) {
        int id;
        if (!freeList.empty()) {
            id = freeList.back();
            freeList.pop_back();
        } else {
            id = nextPageID++;
        }
        pageCache[id] = new Page(id, isLeaf);
        return id;
    }

    Page *getPage(int id) {
        return pageCache[id];
    }

    int findParentPageID(int currentID, int childID);
    void insertInternal(int key, int parentID, int newChildID);

public:
    BPlusTree();
    void insert(int key, const std::string &value);
    std::string search(int key);
    void remove(int key);
    std::vector<std::pair<int, std::string>> rangeSearch(int min, int max);
    void traverse();
    void saveTree(const std::string &dir);
    void loadTree(const std::string &dir);
    void visualize();
    void visualizeRecursive(int pageID, int level, bool isLast);
};

BPlusTree::BPlusTree() {
    rootPageID = createPage(true);
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

void BPlusTree::insertInternal(int key, int parentID, int newChildID) {
    Page *parent = getPage(parentID);
    int pos = 0;
    while (pos < parent->keys.size() && key >= parent->keys[pos])
        pos++;
    parent->keys.insert(parent->keys.begin() + pos, key);
    parent->childrenIDs.insert(parent->childrenIDs.begin() + pos + 1, newChildID);

    if (parent->keys.size() < ORDER)
        return;

    int newInternalID = createPage(false);
    Page *newInternal = getPage(newInternalID);
    int mid = (ORDER + 1) / 2;
    int upKey = parent->keys[mid];

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

void BPlusTree::insert(int key, const std::string &value) {
    int cursorID = rootPageID;
    Page *cursor = getPage(cursorID);
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && key >= cursor->keys[i])
            i++;
        cursorID = cursor->childrenIDs[i];
        cursor = getPage(cursorID);
    }

    auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key);
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
    int newKey = newLeaf->keys[0];

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

std::string BPlusTree::search(int key) {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && key >= cursor->keys[i])
            i++;
        cursor = getPage(cursor->childrenIDs[i]);
    }
    for (size_t i = 0; i < cursor->keys.size(); ++i) {
        if (cursor->keys[i] == key)
            return cursor->values[i];
    }
    return "Not Found";
}

void BPlusTree::remove(int key) {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && key >= cursor->keys[i])
            i++;
        cursor = getPage(cursor->childrenIDs[i]);
    }
    for (size_t i = 0; i < cursor->keys.size(); ++i) {
        if (cursor->keys[i] == key) {
            cursor->keys.erase(cursor->keys.begin() + i);
            cursor->values.erase(cursor->values.begin() + i);
            if (cursor->keys.empty()) {
                int id = cursor->pageID;
                pageCache.erase(id);
                delete cursor;
                freeList.push_back(id);
            }
            return;
        }
    }
}

std::vector<std::pair<int, std::string>> BPlusTree::rangeSearch(int min, int max) {
    std::vector<std::pair<int, std::string>> result;
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf) {
        int i = 0;
        while (i < cursor->keys.size() && min >= cursor->keys[i])
            i++;
        cursor = getPage(cursor->childrenIDs[i]);
    }
    while (cursor) {
        for (size_t i = 0; i < cursor->keys.size(); ++i) {
            int key = cursor->keys[i];
            if (key > max)
                return result;
            if (key >= min)
                result.emplace_back(key, cursor->values[i]);
        }
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
    }
    return result;
}

void BPlusTree::traverse() {
    Page *cursor = getPage(rootPageID);
    while (!cursor->isLeaf)
        cursor = getPage(cursor->childrenIDs[0]);
    while (cursor) {
        for (size_t i = 0; i < cursor->keys.size(); ++i)
            std::cout << cursor->keys[i] << " → " << cursor->values[i] << "  ";
        if (cursor->nextLeafID == -1)
            break;
        cursor = getPage(cursor->nextLeafID);
    }
    std::cout << "\n";
}

void BPlusTree::saveTree(const std::string &dir) {
    std::ofstream meta(dir + "/meta.bin", std::ios::binary);
    meta.write(reinterpret_cast<char *>(&rootPageID), sizeof(int));
    meta.write(reinterpret_cast<char *>(&nextPageID), sizeof(int));
    int freeCount = freeList.size();
    meta.write(reinterpret_cast<char *>(&freeCount), sizeof(int));
    for (int id : freeList)
        meta.write(reinterpret_cast<char *>(&id), sizeof(int));
    meta.close();
    for (const auto &[id, page] : pageCache)
        writePageToDisk(page, dir);
}

void BPlusTree::loadTree(const std::string &dir) {
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
        freeList.push_back(id);
    }
    meta.close();
    for (int i = 0; i < nextPageID; ++i) {
        if (std::find(freeList.begin(), freeList.end(), i) == freeList.end())
            pageCache[i] = readPageFromDisk(i, dir);
    }
}

void BPlusTree::visualize() {
    std::cout << "B+ Tree Structure:\n";
    visualizeRecursive(rootPageID, 0, true);
}

void BPlusTree::visualizeRecursive(int pageID, int level, bool isLast) {
    Page *page = getPage(pageID);
    std::string indent(level * 4, ' ');
    std::cout << indent;
    if (level > 0)
        std::cout << (isLast ? "└─" : "├─");
    std::cout << (page->isLeaf ? "[Leaf " : "[Internal ") << pageID << "] Keys:";
    for (int k : page->keys)
        std::cout << " " << k;
    std::cout << "\n";
    if (!page->isLeaf) {
        for (size_t i = 0; i < page->childrenIDs.size(); ++i) {
            bool childLast = (i == page->childrenIDs.size() - 1);
            visualizeRecursive(page->childrenIDs[i], level + 1, childLast);
        }
    }
}
