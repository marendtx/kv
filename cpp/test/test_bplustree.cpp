#include "b_bplus_tree.hpp"
#include <filesystem>
#include <gtest/gtest.h>

ByteArray toBytes(const std::string &s) {
    return ByteArray(s.begin(), s.end());
}

std::string fromBytes(const ByteArray &b) {
    return std::string(b.begin(), b.end());
}

const std::string testDir = "test_tree_data";

class BPlusTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory(testDir);
    }

    void TearDown() override {
        std::filesystem::remove_all(testDir);
    }
};

TEST_F(BPlusTreeTest, InsertAndSearch) {
    BPlusTree tree;
    tree.insert(toBytes("key1"), toBytes("val1"));
    tree.insert(toBytes("key2"), toBytes("val2"));
    tree.insert(toBytes("key3"), toBytes("val3"));

    EXPECT_EQ(fromBytes(tree.search(toBytes("key1"))), "val1");
    EXPECT_EQ(fromBytes(tree.search(toBytes("key2"))), "val2");
    EXPECT_EQ(fromBytes(tree.search(toBytes("key3"))), "val3");
    EXPECT_TRUE(tree.search(toBytes("keyX")).empty());
}

TEST_F(BPlusTreeTest, DeleteKey) {
    BPlusTree tree;
    tree.insert(toBytes("k1"), toBytes("v1"));
    tree.insert(toBytes("k2"), toBytes("v2"));
    tree.remove(toBytes("k1"));

    EXPECT_TRUE(tree.search(toBytes("k1")).empty());
    EXPECT_EQ(fromBytes(tree.search(toBytes("k2"))), "v2");
}

TEST_F(BPlusTreeTest, RangeSearch) {
    BPlusTree tree;
    tree.insert(toBytes("a"), toBytes("1"));
    tree.insert(toBytes("b"), toBytes("2"));
    tree.insert(toBytes("c"), toBytes("3"));
    tree.insert(toBytes("d"), toBytes("4"));

    auto result = tree.rangeSearch(toBytes("b"), toBytes("c"));
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(fromBytes(result[0].first), "b");
    EXPECT_EQ(fromBytes(result[1].first), "c");
}

TEST_F(BPlusTreeTest, SaveAndLoadTree) {
    {
        BPlusTree tree;
        tree.insert(toBytes("one"), toBytes("1"));
        tree.insert(toBytes("two"), toBytes("2"));
        tree.remove(toBytes("one"));
        tree.saveTree(testDir);
    }

    {
        BPlusTree tree2;
        tree2.loadTree(testDir);
        EXPECT_EQ(fromBytes(tree2.search(toBytes("two"))), "2");
        EXPECT_TRUE(tree2.search(toBytes("one")).empty());
    }
}
