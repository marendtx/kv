#include "b_bplus_tree.hpp"
#include "mylib.h"
#include <iostream>
int main() {
    BPlusTree tree;
    for (int i = 1; i <= 20; ++i)
        tree.insert(i * 5, "val" + std::to_string(i));

    tree.traverse();
    std::cout << "Search 25: " << tree.search(25) << "\n";

    tree.remove(25);
    std::cout << "After delete 25:\n";
    tree.traverse();

    auto range = tree.rangeSearch(30, 70);
    std::cout << "Range [30-70]:\n";
    for (auto &[k, v] : range)
        std::cout << k << " → " << v << "\n";

    tree.traverse();
    tree.visualize();
    tree.saveTree("tree_data");

    BPlusTree loaded;
    loaded.loadTree("tree_data");
    std::cout << "Loaded tree:\n";
    loaded.traverse();
    loaded.visualize();
}