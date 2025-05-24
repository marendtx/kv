#include "b_bplus_tree.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <random>

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
    tree.insert(toBytes("dup"), toBytes("two")); // 2回目は上書き

    EXPECT_EQ(fromBytes(tree.search(toBytes("dup"))), "two"); // 上書きした値が返る
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

// --- 破損ファイルで例外を投げるか ---
TEST_F(BPlusTreeTest, CorruptFileThrows) {
    // わざとmeta.binを書き潰す
    std::ofstream(testDir + "/meta.bin") << "broken!";
    BPlusTree tree;
    EXPECT_THROW(tree.loadTree(testDir), std::runtime_error);
}

// --- 空Treeでのsearch/remove ---
TEST_F(BPlusTreeTest, EmptyTreeOperations) {
    BPlusTree tree;
    EXPECT_TRUE(tree.search(toBytes("any")).empty());
    tree.remove(toBytes("any")); // no throw
}

// --- キー・値にバイナリ（0x00, 0xff, 長大文字列）---
TEST_F(BPlusTreeTest, BinaryKeyAndValue) {
    ByteArray key = {0x00, 0xff, 0x41, 0x42};
    ByteArray val = {0x30, 0x31, 0x32, 0x00};
    BPlusTree tree;
    tree.insert(key, val);
    EXPECT_EQ(tree.search(key), val);
}

// --- ORDER-1, ORDER, ORDER+1件で分割/マージ境界を攻める ---
TEST_F(BPlusTreeTest, OrderBoundarySplitMerge) {
    BPlusTree tree;
    int N = ORDER + 2;
    for (int i = 0; i < N; ++i) {
        std::string s = "k" + std::to_string(i);
        tree.insert(toBytes(s), toBytes("v" + std::to_string(i)));
    }
    for (int i = 0; i < N; ++i) {
        std::string s = "k" + std::to_string(i);
        EXPECT_EQ(fromBytes(tree.search(toBytes(s))), "v" + std::to_string(i));
    }
    // 一気に削除してマージ/高さ減少をテスト
    for (int i = 0; i < N; ++i)
        tree.remove(toBytes("k" + std::to_string(i)));
    for (int i = 0; i < N; ++i)
        EXPECT_TRUE(tree.search(toBytes("k" + std::to_string(i))).empty());
}

// --- save/loadを何回も繰り返しても壊れない ---
TEST_F(BPlusTreeTest, SaveLoadRepeat) {
    BPlusTree tree;
    for (int i = 0; i < 100; ++i)
        tree.insert(toBytes("k" + std::to_string(i)), toBytes("v" + std::to_string(i)));
    for (int j = 0; j < 5; ++j) {
        tree.saveTree(testDir);
        BPlusTree t2;
        t2.loadTree(testDir);
        for (int i = 0; i < 100; ++i)
            EXPECT_EQ(fromBytes(t2.search(toBytes("k" + std::to_string(i)))), "v" + std::to_string(i));
    }
}

TEST_F(BPlusTreeTest, LargeScaleInsertSearchDelete) {
    BPlusTree tree;
    tree.saveTree(testDir); // ディレクトリセット
    const int N = 10000;
    std::vector<std::string> keys, vals;
    for (int i = 0; i < N; ++i) {
        keys.push_back("key" + std::to_string(i));
        vals.push_back("val" + std::to_string(i));
    }
    // シャッフル挿入
    std::mt19937 rng(std::random_device{}());
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int i = 0; i < N; ++i)
        tree.insert(toBytes(keys[i]), toBytes(vals[i]));
    // 検索
    for (int i = 0; i < N; ++i)
        EXPECT_EQ(fromBytes(tree.search(toBytes(keys[i]))), vals[i]);
    // ランダム削除
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int i = 0; i < N; ++i)
        tree.remove(toBytes(keys[i]));
    // 全部消えたことを確認
    for (int i = 0; i < N; ++i)
        EXPECT_TRUE(tree.search(toBytes(keys[i])).empty());
}

TEST_F(BPlusTreeTest, RandomBinaryKeyValue) {
    BPlusTree tree;
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint8_t> dist(0, 255);

    for (int i = 0; i < 1000; ++i) {
        ByteArray key(10), val(20);
        for (auto &b : key)
            b = dist(rng);
        for (auto &b : val)
            b = dist(rng);
        tree.insert(key, val);
        EXPECT_EQ(tree.search(key), val);
    }
}

TEST_F(BPlusTreeTest, ZeroAndLongKeysAndValues) {
    BPlusTree tree;
    ByteArray empty;
    ByteArray bigKey(4096, 'K'), bigVal(4096, 'V');
    tree.insert(empty, empty);
    tree.insert(bigKey, bigVal);

    EXPECT_EQ(tree.search(empty), empty);
    EXPECT_EQ(tree.search(bigKey), bigVal);

    tree.remove(empty);
    tree.remove(bigKey);

    EXPECT_TRUE(tree.search(empty).empty());
    EXPECT_TRUE(tree.search(bigKey).empty());
}

TEST_F(BPlusTreeTest, InsertDeleteChurn) {
    BPlusTree tree;
    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 100; ++i)
            tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i) + "_" + std::to_string(round)));
        for (int i = 0; i < 100; ++i)
            EXPECT_EQ(fromBytes(tree.search(toBytes("key" + std::to_string(i)))), "val" + std::to_string(i) + "_" + std::to_string(round));
        for (int i = 0; i < 100; ++i)
            tree.remove(toBytes("key" + std::to_string(i)));
        for (int i = 0; i < 100; ++i)
            EXPECT_TRUE(tree.search(toBytes("key" + std::to_string(i))).empty());
    }
}

TEST_F(BPlusTreeTest, SaveImmediatelyAfterInsert) {
    for (int rep = 0; rep < 5; ++rep) {
        BPlusTree tree;
        for (int i = 0; i < 50; ++i) {
            tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
            tree.saveTree(testDir);
            BPlusTree reloaded;
            reloaded.loadTree(testDir);
            for (int j = 0; j <= i; ++j)
                EXPECT_EQ(fromBytes(reloaded.search(toBytes("key" + std::to_string(j)))), "val" + std::to_string(j));
        }
    }
}

TEST_F(BPlusTreeTest, RandomRemoveOrder) {
    BPlusTree tree;
    const int N = 256;
    std::vector<std::string> keys;
    for (int i = 0; i < N; ++i)
        keys.push_back("key" + std::to_string(i));
    for (const auto &k : keys)
        tree.insert(toBytes(k), toBytes(k));
    std::mt19937 rng(43);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (const auto &k : keys)
        tree.remove(toBytes(k));
    for (const auto &k : keys)
        EXPECT_TRUE(tree.search(toBytes(k)).empty());
}

TEST_F(BPlusTreeTest, EmptyStringKeyAndValue) {
    BPlusTree tree;
    tree.insert(ByteArray(), ByteArray());          // 両方empty
    tree.insert(toBytes(""), toBytes("not empty")); // ←上書きされる
    tree.insert(toBytes("not empty"), toBytes("")); // ←別キー

    // 空キーは一つだけ存在できる。その値は「not empty」。
    EXPECT_EQ(tree.search(ByteArray()), toBytes("not empty"));
    EXPECT_EQ(tree.search(toBytes("")), toBytes("not empty"));   // 同じ
    EXPECT_EQ(fromBytes(tree.search(toBytes("not empty"))), ""); // これは空文字列
}

TEST_F(BPlusTreeTest, OverwriteValueManyTimes) {
    BPlusTree tree;
    tree.insert(toBytes("repeat"), toBytes("1"));
    for (int i = 2; i <= 100; ++i) {
        tree.insert(toBytes("repeat"), toBytes(std::to_string(i)));
        EXPECT_EQ(fromBytes(tree.search(toBytes("repeat"))), std::to_string(i));
    }
}
