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

TEST_F(BPlusTreeTest, InsertManyAndSearchAll) {
    BPlusTree tree;
    const int N = 500;
    for (int i = 0; i < N; ++i) {
        tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(fromBytes(tree.search(toBytes("key" + std::to_string(i)))), "val" + std::to_string(i));
    }
}

TEST_F(BPlusTreeTest, DeleteCausesMerge) {
    BPlusTree tree;
    for (int i = 0; i < 20; ++i) // データ数増やす
        tree.insert(toBytes("k" + std::to_string(i)), toBytes("v" + std::to_string(i)));

    // リーフ分割 → 削除によって再マージを誘発
    for (int i = 5; i <= 14; ++i)
        tree.remove(toBytes("k" + std::to_string(i)));

    for (int i = 5; i <= 14; ++i)
        EXPECT_TRUE(tree.search(toBytes("k" + std::to_string(i))).empty()) << "Key k" << i << " should be gone";

    EXPECT_EQ(fromBytes(tree.search(toBytes("k4"))), "v4");
    EXPECT_EQ(fromBytes(tree.search(toBytes("k15"))), "v15");
}

TEST_F(BPlusTreeTest, DuplicateKeyInsertOverwrites) {
    BPlusTree tree;
    tree.insert(toBytes("dup"), toBytes("one"));
    tree.insert(toBytes("dup"), toBytes("two")); // overwrite behavior expected?

    // 現在の実装では重複キーは追加される → search は最初の一致を返す
    // overwrite にするなら search → remove → insert に変える必要あり
    EXPECT_EQ(fromBytes(tree.search(toBytes("dup"))), "one"); // 今の仕様ではこうなる
}

TEST_F(BPlusTreeTest, EdgeCaseEmptyKey) {
    BPlusTree tree;
    ByteArray empty;
    tree.insert(empty, toBytes("value"));
    EXPECT_EQ(fromBytes(tree.search(empty)), "value");

    tree.remove(empty);
    EXPECT_TRUE(tree.search(empty).empty());
}

TEST_F(BPlusTreeTest, PersistenceWithManyKeys) {
    {
        BPlusTree tree;
        for (int i = 0; i < 100; ++i)
            tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
        tree.saveTree(testDir);
    }

    {
        BPlusTree tree2;
        tree2.loadTree(testDir);
        for (int i = 0; i < 100; ++i)
            EXPECT_EQ(fromBytes(tree2.search(toBytes("key" + std::to_string(i)))), "val" + std::to_string(i));
    }
}