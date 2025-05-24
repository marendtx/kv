#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

const int ORDER = 4;

// Helper: lexicographical comparison for byte strings
inline int cmpKey(const std::string &a, const std::string &b) {
    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

// Binary write helper for std::string (byte blob)
inline void writeBlob(std::ofstream &ofs, const std::string &data) {
    int len = data.size();
    ofs.write(reinterpret_cast<char *>(&len), sizeof(int));
    ofs.write(data.data(), len);
}

// Binary read helper for std::string (byte blob)
inline std::string readBlob(std::ifstream &ifs) {
    int len;
    ifs.read(reinterpret_cast<char *>(&len), sizeof(int));
    std::string data(len, '\0');
    ifs.read(&data[0], len);
    return data;
}

struct Page {
    int pageID;
    bool isLeaf;
    std::vector<std::string> keys;   // now as bytes (std::string for binary)
    std::vector<std::string> values; // also bytes
    std::vector<int> childrenIDs;
    int nextLeafID = -1;

    Page(int id, bool leaf) : pageID(id), isLeaf(leaf) {}
};

inline void writePageToDisk(Page *page, const std::string &dir) {
    std::string filename = dir + "/page_" + std::to_string(page->pageID) + ".bin";
    std::ofstream ofs(filename, std::ios::binary);
    ofs.write(reinterpret_cast<char *>(&page->pageID), sizeof(int));
    ofs.write(reinterpret_cast<char *>(&page->isLeaf), sizeof(bool));
    int keyCount = page->keys.size();
    ofs.write(reinterpret_cast<char *>(&keyCount), sizeof(int));
    for (const std::string &k : page->keys)
        writeBlob(ofs, k);
    if (page->isLeaf) {
        int valCount = page->values.size();
        ofs.write(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (const std::string &val : page->values)
            writeBlob(ofs, val);
        ofs.write(reinterpret_cast<char *>(&page->nextLeafID), sizeof(int));
    } else {
        int childCount = page->childrenIDs.size();
        ofs.write(reinterpret_cast<char *>(&childCount), sizeof(int));
        for (int cid : page->childrenIDs)
            ofs.write(reinterpret_cast<char *>(&cid), sizeof(int));
    }
}

inline Page *readPageFromDisk(int pageID, const std::string &dir) {
    std::string filename = dir + "/page_" + std::to_string(pageID) + ".bin";
    std::ifstream ifs(filename, std::ios::binary);
    int id;
    bool isLeaf;
    ifs.read(reinterpret_cast<char *>(&id), sizeof(int));
    ifs.read(reinterpret_cast<char *>(&isLeaf), sizeof(bool));
    Page *page = new Page(id, isLeaf);
    int keyCount;
    ifs.read(reinterpret_cast<char *>(&keyCount), sizeof(int));
    for (int i = 0; i < keyCount; ++i)
        page->keys.push_back(readBlob(ifs));
    if (isLeaf) {
        int valCount;
        ifs.read(reinterpret_cast<char *>(&valCount), sizeof(int));
        for (int i = 0; i < valCount; ++i)
            page->values.push_back(readBlob(ifs));
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

    Page *getPage(int id) { return pageCache[id]; }

    int findParentPageID(int currentID, int childID) {
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

    void insertInternal(const std::string &key, int parentID, int newChildID) {
        Page *parent = getPage(parentID);
        int pos = 0;
        while (pos < parent->keys.size() && cmpKey(key, parent->keys[pos]) >= 0)
            pos++;
        parent->keys.insert(parent->keys.begin() + pos, key);
        parent->childrenIDs.insert(parent->childrenIDs.begin() + pos + 1, newChildID);

        if (parent->keys.size() < ORDER)
            return;

        int newInternalID = createPage(false);
        Page *newInternal = getPage(newInternalID);
        int mid = (ORDER + 1) / 2;
        std::string upKey = parent->keys[mid];

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

public:
    BPlusTree() { rootPageID = createPage(true); }

    void insert(const std::string &key, const std::string &value) {
        int cursorID = rootPageID;
        Page *cursor = getPage(cursorID);
        while (!cursor->isLeaf) {
            int i = 0;
            while (i < cursor->keys.size() && cmpKey(key, cursor->keys[i]) >= 0)
                i++;
            cursorID = cursor->childrenIDs[i];
            cursor = getPage(cursorID);
        }

        auto it = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key,
                                   [](const std::string &a, const std::string &b) { return cmpKey(a, b) < 0; });
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
        std::string newKey = newLeaf->keys[0];

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

    std::string search(const std::string &key) {
        Page *cursor = getPage(rootPageID);
        while (!cursor->isLeaf) {
            int i = 0;
            while (i < cursor->keys.size() && cmpKey(key, cursor->keys[i]) >= 0)
                i++;
            cursor = getPage(cursor->childrenIDs[i]);
        }
        for (size_t i = 0; i < cursor->keys.size(); ++i) {
            if (cmpKey(cursor->keys[i], key) == 0)
                return cursor->values[i];
        }
        return "Not Found";
    }

    void remove(const std::string &key) {
        Page *cursor = getPage(rootPageID);
        while (!cursor->isLeaf) {
            int i = 0;
            while (i < cursor->keys.size() && cmpKey(key, cursor->keys[i]) >= 0)
                i++;
            cursor = getPage(cursor->childrenIDs[i]);
        }
        for (size_t i = 0; i < cursor->keys.size(); ++i) {
            if (cmpKey(cursor->keys[i], key) == 0) {
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

    std::vector<std::pair<std::string, std::string>> rangeSearch(const std::string &min, const std::string &max) {
        std::vector<std::pair<std::string, std::string>> result;
        Page *cursor = getPage(rootPageID);
        while (!cursor->isLeaf) {
            int i = 0;
            while (i < cursor->keys.size() && cmpKey(min, cursor->keys[i]) >= 0)
                i++;
            cursor = getPage(cursor->childrenIDs[i]);
        }
        while (cursor) {
            for (size_t i = 0; i < cursor->keys.size(); ++i) {
                const std::string &key = cursor->keys[i];
                if (cmpKey(key, max) > 0)
                    return result;
                if (cmpKey(key, min) >= 0)
                    result.emplace_back(key, cursor->values[i]);
            }
            if (cursor->nextLeafID == -1)
                break;
            cursor = getPage(cursor->nextLeafID);
        }
        return result;
    }

    void traverse() {
        Page *cursor = getPage(rootPageID);
        while (!cursor->isLeaf)
            cursor = getPage(cursor->childrenIDs[0]);
        while (cursor) {
            for (size_t i = 0; i < cursor->keys.size(); ++i) {
                // バイナリキーは16進文字列などで出力するのが本格的ですが、ここでは可読性重視で文字列出力
                std::cout << "[";
                for (unsigned char c : cursor->keys[i])
                    printf("%02x", c);
                std::cout << "] → " << cursor->values[i] << "  ";
            }
            if (cursor->nextLeafID == -1)
                break;
            cursor = getPage(cursor->nextLeafID);
        }
        std::cout << "\n";
    }

    void saveTree(const std::string &dir) {
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

    void loadTree(const std::string &dir) {
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

    void visualize() {
        std::cout << "B+ Tree Structure:\n";
        visualizeRecursive(rootPageID, 0, true);
    }

    void visualizeRecursive(int pageID, int level, bool isLast) {
        Page *page = getPage(pageID);
        std::string indent(level * 4, ' ');
        std::cout << indent;
        if (level > 0)
            std::cout << (isLast ? "└─" : "├─");
        std::cout << (page->isLeaf ? "[Leaf " : "[Internal ") << pageID << "] Keys:";
        for (const auto &k : page->keys) {
            std::cout << " [";
            for (unsigned char c : k)
                printf("%02x", c);
            std::cout << "]";
        }
        std::cout << "\n";
        if (!page->isLeaf) {
            for (size_t i = 0; i < page->childrenIDs.size(); ++i) {
                bool childLast = (i == page->childrenIDs.size() - 1);
                visualizeRecursive(page->childrenIDs[i], level + 1, childLast);
            }
        }
    }
};
