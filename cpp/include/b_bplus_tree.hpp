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

struct Page {
    int pageID;
    bool isLeaf;
    std::vector<ByteArray> keys;
    std::vector<ByteArray> values;
    std::vector<int> childrenIDs;
    int nextLeafID = -1;
    Page(int id, bool leaf) : pageID(id), isLeaf(leaf) {}
};

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

Page *BPlusTree::getPage(int id) {
    if (pageCache.find(id) == pageCache.end()) {
        pageCache[id] = readPageFromDisk(id, directory);
    }
    return pageCache[id];
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

void BPlusTree::rebalanceAfterDeletion(Page *node, int nodeID) {
    if (nodeID == rootPageID) {
        if (!node->isLeaf && node->childrenIDs.size() == 1) {
            rootPageID = node->childrenIDs[0];
            freeList.insert(nodeID);
            pageCache.erase(nodeID);
            delete node;
        }
        return;
    }

    int minKeys = (ORDER + 1) / 2;
    if (node->keys.size() >= minKeys)
        return;

    int parentID = findParentPageID(rootPageID, nodeID);
    Page *parent = getPage(parentID);

    int idx = 0;
    while (idx < parent->childrenIDs.size() && parent->childrenIDs[idx] != nodeID)
        idx++;

    int leftID = (idx > 0) ? parent->childrenIDs[idx - 1] : -1;
    int rightID = (idx + 1 < parent->childrenIDs.size()) ? parent->childrenIDs[idx + 1] : -1;

    if (leftID != -1) {
        Page *left = getPage(leftID);

        if (left->keys.size() > minKeys) {
            // ✅ 左から借用
            if (node->isLeaf) {
                node->keys.insert(node->keys.begin(), left->keys.back());
                node->values.insert(node->values.begin(), left->values.back());
                left->keys.pop_back();
                left->values.pop_back();

                // 親キー更新（左の最後のキー → 現ノードの最初のキー）
                parent->keys[idx - 1] = node->keys[0];
            } else {
                node->keys.insert(node->keys.begin(), parent->keys[idx - 1]);
                node->childrenIDs.insert(node->childrenIDs.begin(), left->childrenIDs.back());

                parent->keys[idx - 1] = left->keys.back();

                left->keys.pop_back();
                left->childrenIDs.pop_back();
            }
            return;
        }
    }

    if (rightID != -1) {
        Page *right = getPage(rightID);

        if (right->keys.size() > minKeys) {
            // ✅ 右から借用
            if (node->isLeaf) {
                node->keys.push_back(right->keys.front());
                node->values.push_back(right->values.front());
                right->keys.erase(right->keys.begin());
                right->values.erase(right->values.begin());

                parent->keys[idx] = right->keys.front();
            } else {
                node->keys.push_back(parent->keys[idx]);
                node->childrenIDs.push_back(right->childrenIDs.front());

                parent->keys[idx] = right->keys.front();

                right->keys.erase(right->keys.begin());
                right->childrenIDs.erase(right->childrenIDs.begin());
            }
            return;
        }
    }

    // ✅ 借用不可 → マージ処理へ
    if (leftID != -1) {
        Page *left = getPage(leftID);
        if (node->isLeaf) {
            left->keys.insert(left->keys.end(), node->keys.begin(), node->keys.end());
            left->values.insert(left->values.end(), node->values.begin(), node->values.end());
            left->nextLeafID = node->nextLeafID;
        } else {
            left->keys.push_back(parent->keys[idx - 1]);
            left->keys.insert(left->keys.end(), node->keys.begin(), node->keys.end());
            left->childrenIDs.insert(left->childrenIDs.end(), node->childrenIDs.begin(), node->childrenIDs.end());
        }

        parent->keys.erase(parent->keys.begin() + idx - 1);
        parent->childrenIDs.erase(parent->childrenIDs.begin() + idx);

        freeList.insert(nodeID);
        pageCache.erase(nodeID);
        delete node;

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
        } else {
            node->keys.push_back(parent->keys[idx]);
            node->keys.insert(node->keys.end(), right->keys.begin(), right->keys.end());
            node->childrenIDs.insert(node->childrenIDs.end(), right->childrenIDs.begin(), right->childrenIDs.end());
        }

        parent->keys.erase(parent->keys.begin() + idx);
        parent->childrenIDs.erase(parent->childrenIDs.begin() + idx + 1);

        freeList.insert(rightID);
        pageCache.erase(rightID);
        delete right;

        if (parentID != rootPageID && parent->keys.size() < minKeys)
            rebalanceAfterDeletion(parent, parentID);
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

    std::function<void(int, std::string, bool)> dfs =
        [&](int pageID, std::string prefix, bool isLast) {
            Page *p = getPage(pageID);

            std::cout << prefix;
            std::cout << (isLast ? "└── " : "├── ");

            std::string nodeType = p->isLeaf ? "Leaf" : "Internal";
            std::cout << "[" << nodeType << " " << pageID << "] Keys: ";
            for (size_t i = 0; i < p->keys.size(); ++i) {
                std::string ks(p->keys[i].begin(), p->keys[i].end());
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
                    std::string vs(p->values[i].begin(), p->values[i].end());
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