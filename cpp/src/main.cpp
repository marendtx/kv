#include "b_bplus_tree.hpp"
#include <iostream>
int main() {
    BPlusTree tree;
    tree.insert("abc", "val1");
    tree.insert(std::string("\x01\x02", 2), "bin2");
    tree.traverse();
    std::cout << "Search abc: " << tree.search("abc") << "\n";
    std::cout << "Search \\x01\\x02: " << tree.search(std::string("\x01\x02", 2)) << "\n";
    tree.remove("abc");
    tree.traverse();
    tree.visualize();
    tree.saveTree("tree_data");

    BPlusTree loaded;
    loaded.loadTree("tree_data");
    std::cout << "Loaded tree:\n";
    loaded.traverse();
    loaded.visualize();
}