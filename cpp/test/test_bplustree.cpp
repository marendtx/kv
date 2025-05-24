#include "b_bplus_tree.hpp"
#include <filesystem>
#include <gtest/gtest.h>

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

TEST_F(BPlusTreeTest, InsertAndSearch) {
    BPlusTree tree;
    tree.insert(10, "ten");
    tree.insert(20, "twenty");
    tree.insert(15, "fifteen");

    EXPECT_EQ(tree.search(10), "ten");
    EXPECT_EQ(tree.search(15), "fifteen");
    EXPECT_EQ(tree.search(99), "Not Found");
}

TEST_F(BPlusTreeTest, RemoveKeyAndFreeListReuse) {
    BPlusTree tree;
    tree.insert(5, "five");
    tree.insert(10, "ten");

    EXPECT_EQ(tree.search(5), "five");
    tree.remove(5);
    EXPECT_EQ(tree.search(5), "Not Found");

    // Insert again to check ID reuse via freelist
    tree.insert(25, "twentyfive");
    EXPECT_EQ(tree.search(25), "twentyfive");
    // Visual confirmation or internal inspection could be added
}

TEST_F(BPlusTreeTest, RangeSearch) {
    BPlusTree tree;
    for (int i = 1; i <= 5; ++i)
        tree.insert(i * 10, "val" + std::to_string(i));

    auto results = tree.rangeSearch(15, 35);
    std::vector<int> expectedKeys = {20, 30};
    ASSERT_EQ(results.size(), expectedKeys.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i].first, expectedKeys[i]);
    }
}

TEST_F(BPlusTreeTest, SaveLoadWithFreeList) {
    {
        BPlusTree tree;
        for (int i = 1; i <= 5; ++i)
            tree.insert(i * 10, "val" + std::to_string(i));
        tree.remove(10); // freeList should now contain a page ID
        tree.saveTree(testDir);
    }

    {
        BPlusTree tree2;
        tree2.loadTree(testDir);

        EXPECT_EQ(tree2.search(10), "Not Found");
        EXPECT_EQ(tree2.search(20), "val2");

        auto results = tree2.rangeSearch(10, 50);
        std::vector<int> expectedKeys = {20, 30, 40, 50};
        ASSERT_EQ(results.size(), expectedKeys.size());
        for (size_t i = 0; i < results.size(); ++i) {
            EXPECT_EQ(results[i].first, expectedKeys[i]);
        }
    }
}