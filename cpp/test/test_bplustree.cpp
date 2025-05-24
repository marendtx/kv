#include "b_bplus_tree.hpp"
#include <filesystem>
#include <gtest/gtest.h>

// バイト列比較用ヘルパー
std::string hex(const std::string &s) {
    std::string h;
    char buf[4];
    for (unsigned char c : s) {
        snprintf(buf, sizeof(buf), "%02x", c);
        h += buf;
    }
    return h;
}

class BPlusTreeTest : public ::testing::Test {
protected:
    const std::string testDir = "testdata";

    void SetUp() override {
        std::filesystem::create_directory(testDir);
    }

    void TearDown() override {
        std::filesystem::remove_all(testDir);
    }
};

TEST_F(BPlusTreeTest, InsertAndSearchStringKey) {
    BPlusTree tree;
    tree.insert("aaa", "ten");
    tree.insert("bbb", "twenty");
    tree.insert("ccc", "fifteen");

    EXPECT_EQ(tree.search("aaa"), "ten");
    EXPECT_EQ(tree.search("ccc"), "fifteen");
    EXPECT_EQ(tree.search("zzz"), "Not Found");
}

TEST_F(BPlusTreeTest, InsertAndSearchBinaryKey) {
    BPlusTree tree;
    std::string key1 = std::string("\x00\x01", 2);
    std::string key2 = std::string("\x00\x02", 2);
    std::string key3 = std::string("\xff\xff", 2);

    tree.insert(key1, "v1");
    tree.insert(key2, "v2");
    tree.insert(key3, "v3");

    EXPECT_EQ(tree.search(key1), "v1");
    EXPECT_EQ(tree.search(key2), "v2");
    EXPECT_EQ(tree.search(key3), "v3");
    EXPECT_EQ(tree.search(std::string("\x01\x01", 2)), "Not Found");
}

TEST_F(BPlusTreeTest, RemoveKeyAndFreeListReuse) {
    BPlusTree tree;
    std::string k1 = "k1";
    std::string k2 = "k2";
    tree.insert(k1, "five");
    tree.insert(k2, "ten");

    EXPECT_EQ(tree.search(k1), "five");
    tree.remove(k1);
    EXPECT_EQ(tree.search(k1), "Not Found");

    // Insert again to check ID reuse via freelist
    std::string k3 = "k3";
    tree.insert(k3, "twentyfive");
    EXPECT_EQ(tree.search(k3), "twentyfive");
}

TEST_F(BPlusTreeTest, RangeSearch) {
    BPlusTree tree;
    // "001", "010", "020", "030", "040"
    for (int i = 1; i <= 5; ++i) {
        std::string k = std::string(1, (char)i); // バイナリでも可
        tree.insert(k, "val" + std::to_string(i));
    }

    std::string minKey = std::string(1, (char)2);
    std::string maxKey = std::string(1, (char)4);
    auto results = tree.rangeSearch(minKey, maxKey);

    std::vector<std::string> expectedKeys = {std::string(1, (char)2), std::string(1, (char)3), std::string(1, (char)4)};
    ASSERT_EQ(results.size(), expectedKeys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].first, expectedKeys[i]);
    }
}

TEST_F(BPlusTreeTest, SaveLoadWithFreeList) {
    {
        BPlusTree tree;
        for (int i = 1; i <= 5; ++i)
            tree.insert(std::string(1, (char)i), "val" + std::to_string(i));
        tree.remove(std::string(1, (char)1)); // freeList should now contain a page ID
        tree.saveTree(testDir);
    }

    {
        BPlusTree tree2;
        tree2.loadTree(testDir);

        EXPECT_EQ(tree2.search(std::string(1, (char)1)), "Not Found");
        EXPECT_EQ(tree2.search(std::string(1, (char)2)), "val2");

        auto results = tree2.rangeSearch(std::string(1, (char)1), std::string(1, (char)5));
        std::vector<std::string> expectedKeys = {
            std::string(1, (char)2), std::string(1, (char)3), std::string(1, (char)4), std::string(1, (char)5)};
        ASSERT_EQ(results.size(), expectedKeys.size());
        for (size_t i = 0; i < results.size(); ++i) {
            EXPECT_EQ(results[i].first, expectedKeys[i]);
        }
    }
}
