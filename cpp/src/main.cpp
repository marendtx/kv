#include "b_bplus_tree.hpp"
#include <cstddef>
#include <iostream>
#include <string>

ByteArray toBytes(const std::string &s) {
    return ByteArray(reinterpret_cast<const std::byte *>(s.data()), reinterpret_cast<const std::byte *>(s.data() + s.size()));
}

std::string fromBytes(const ByteArray &b) {
    return std::string(reinterpret_cast<const char *>(b.data()), b.size());
}

int main() {
    BPlusTree tree;

    // データ挿入
    tree.insert(toBytes("apple"), toBytes("red"));
    tree.insert(toBytes("banana"), toBytes("yellow"));
    tree.insert(toBytes("cherry"), toBytes("dark red"));
    tree.insert(toBytes("date"), toBytes("brown"));
    tree.insert(toBytes("fig"), toBytes("purple"));
    tree.insert(toBytes("grape"), toBytes("green"));

    std::cout << "Inserted records:\n";
    tree.traverse();

    // ツリー構造表示
    std::cout << "\nVisualizing B+ Tree:\n";
    tree.visualize();

    // 検索テスト
    auto val = tree.search(toBytes("cherry"));
    std::cout << "\nSearch 'cherry': ";
    if (!val.empty()) {
        std::cout << fromBytes(val) << "\n";
    } else {
        std::cout << "Not found\n";
    }

    // 削除テスト
    tree.remove(toBytes("date"));
    std::cout << "\nAfter removing 'date':\n";
    tree.traverse();

    // 範囲検索テスト
    auto range = tree.rangeSearch(toBytes("banana"), toBytes("grape"));
    std::cout << "\nRange search from 'banana' to 'grape':\n";
    for (auto &[k, v] : range) {
        std::cout << fromBytes(k) << " → " << fromBytes(v) << "\n";
    }

    // 永続化テスト
    std::string dir = "tree_data";
    tree.saveTree(dir);
    std::cout << "\nTree saved to disk.\n";

    BPlusTree reloaded;
    reloaded.loadTree(dir);
    std::cout << "\nReloaded tree from disk:\n";
    reloaded.traverse();
    reloaded.visualize();

    return 0;
}
