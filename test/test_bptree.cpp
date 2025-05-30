#include "bptree.hpp"
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <random>

const std::string testDir = "test_tree_data";

class BPTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory(testDir);
    }

    void TearDown() override {
        std::filesystem::remove_all(testDir);
    }
};

TEST_F(BPTreeTest, InsertAndSearch) {
    BPTree tree;
    tree.insert(toBytes("key1"), toBytes("val1"));
    tree.insert(toBytes("key2"), toBytes("val2"));
    tree.insert(toBytes("key3"), toBytes("val3"));

    EXPECT_EQ(fromBytes(tree.search(toBytes("key1"))), "val1");
    EXPECT_EQ(fromBytes(tree.search(toBytes("key2"))), "val2");
    EXPECT_EQ(fromBytes(tree.search(toBytes("key3"))), "val3");
    EXPECT_TRUE(tree.search(toBytes("keyX")).empty());
}

TEST_F(BPTreeTest, DeleteKey) {
    BPTree tree;
    tree.insert(toBytes("k1"), toBytes("v1"));
    tree.insert(toBytes("k2"), toBytes("v2"));
    tree.remove(toBytes("k1"));

    EXPECT_TRUE(tree.search(toBytes("k1")).empty());
    EXPECT_EQ(fromBytes(tree.search(toBytes("k2"))), "v2");
}

TEST_F(BPTreeTest, RangeSearch) {
    BPTree tree;
    tree.insert(toBytes("a"), toBytes("1"));
    tree.insert(toBytes("b"), toBytes("2"));
    tree.insert(toBytes("c"), toBytes("3"));
    tree.insert(toBytes("d"), toBytes("4"));

    auto result = tree.rangeSearch(toBytes("b"), toBytes("c"));
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(fromBytes(result[0].first), "b");
    EXPECT_EQ(fromBytes(result[1].first), "c");
}

TEST_F(BPTreeTest, SaveAndLoadTree) {
    {
        BPTree tree;
        tree.insert(toBytes("one"), toBytes("1"));
        tree.insert(toBytes("two"), toBytes("2"));
        tree.remove(toBytes("one"));
        tree.saveTree(testDir);
    }

    {
        BPTree tree2;
        tree2.loadTree(testDir);
        EXPECT_EQ(fromBytes(tree2.search(toBytes("two"))), "2");
        EXPECT_TRUE(tree2.search(toBytes("one")).empty());
    }
}

TEST_F(BPTreeTest, InsertManyAndSearchAll) {
    BPTree tree;
    const int N = 500;
    for (int i = 0; i < N; ++i) {
        tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
    }

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(fromBytes(tree.search(toBytes("key" + std::to_string(i)))), "val" + std::to_string(i));
    }
}

TEST_F(BPTreeTest, DeleteCausesMerge) {
    BPTree tree;
    for (int i = 0; i < 20; ++i)
        tree.insert(toBytes("k" + std::to_string(i)), toBytes("v" + std::to_string(i)));

    for (int i = 5; i <= 14; ++i)
        tree.remove(toBytes("k" + std::to_string(i)));

    for (int i = 5; i <= 14; ++i)
        EXPECT_TRUE(tree.search(toBytes("k" + std::to_string(i))).empty()) << "Key k" << i << " should be gone";

    EXPECT_EQ(fromBytes(tree.search(toBytes("k4"))), "v4");
    EXPECT_EQ(fromBytes(tree.search(toBytes("k15"))), "v15");
}

TEST_F(BPTreeTest, DuplicateKeyInsertOverwrites) {
    BPTree tree;
    tree.insert(toBytes("dup"), toBytes("one"));
    tree.insert(toBytes("dup"), toBytes("two")); // 上書き

    EXPECT_EQ(fromBytes(tree.search(toBytes("dup"))), "two");
}

TEST_F(BPTreeTest, EdgeCaseEmptyKey) {
    BPTree tree;
    ByteArray empty;
    tree.insert(empty, toBytes("value"));
    EXPECT_EQ(fromBytes(tree.search(empty)), "value");

    tree.remove(empty);
    EXPECT_TRUE(tree.search(empty).empty());
}

TEST_F(BPTreeTest, PersistenceWithManyKeys) {
    {
        BPTree tree;
        for (int i = 0; i < 100; ++i)
            tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
        tree.saveTree(testDir);
    }

    {
        BPTree tree2;
        tree2.loadTree(testDir);
        for (int i = 0; i < 100; ++i)
            EXPECT_EQ(fromBytes(tree2.search(toBytes("key" + std::to_string(i)))), "val" + std::to_string(i));
    }
}

TEST_F(BPTreeTest, CorruptFileThrows) {
    std::ofstream(testDir + "/meta.bin") << "broken!";
    BPTree tree;
    EXPECT_THROW(tree.loadTree(testDir), std::runtime_error);
}

TEST_F(BPTreeTest, EmptyTreeOperations) {
    BPTree tree;
    EXPECT_TRUE(tree.search(toBytes("any")).empty());
    tree.remove(toBytes("any"));
}

TEST_F(BPTreeTest, BinaryKeyAndValue) {
    ByteArray key = {std::byte(0x00), std::byte(0xff), std::byte(0x41), std::byte(0x42)};
    ByteArray val = {std::byte(0x30), std::byte(0x31), std::byte(0x32), std::byte(0x00)};
    BPTree tree;
    tree.insert(key, val);
    EXPECT_EQ(tree.search(key), val);
}

TEST_F(BPTreeTest, OrderBoundarySplitMerge) {
    BPTree tree;
    int N = ORDER + 2;
    for (int i = 0; i < N; ++i) {
        std::string s = "k" + std::to_string(i);
        tree.insert(toBytes(s), toBytes("v" + std::to_string(i)));
    }
    for (int i = 0; i < N; ++i) {
        std::string s = "k" + std::to_string(i);
        EXPECT_EQ(fromBytes(tree.search(toBytes(s))), "v" + std::to_string(i));
    }
    for (int i = 0; i < N; ++i)
        tree.remove(toBytes("k" + std::to_string(i)));
    for (int i = 0; i < N; ++i)
        EXPECT_TRUE(tree.search(toBytes("k" + std::to_string(i))).empty());
}

TEST_F(BPTreeTest, SaveLoadRepeat) {
    BPTree tree;
    for (int i = 0; i < 100; ++i)
        tree.insert(toBytes("k" + std::to_string(i)), toBytes("v" + std::to_string(i)));
    for (int j = 0; j < 5; ++j) {
        tree.saveTree(testDir);
        BPTree t2;
        t2.loadTree(testDir);
        for (int i = 0; i < 100; ++i)
            EXPECT_EQ(fromBytes(t2.search(toBytes("k" + std::to_string(i)))), "v" + std::to_string(i));
    }
}

TEST_F(BPTreeTest, LargeScaleInsertSearchDelete) {
    BPTree tree;
    tree.saveTree(testDir);
    const int N = 10000;
    std::vector<std::string> keys, vals;
    for (int i = 0; i < N; ++i) {
        keys.push_back("key" + std::to_string(i));
        vals.push_back("val" + std::to_string(i));
    }
    std::mt19937 rng(std::random_device{}());
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int i = 0; i < N; ++i)
        tree.insert(toBytes(keys[i]), toBytes(vals[i]));
    for (int i = 0; i < N; ++i)
        EXPECT_EQ(fromBytes(tree.search(toBytes(keys[i]))), vals[i]);
    std::shuffle(keys.begin(), keys.end(), rng);
    for (int i = 0; i < N; ++i)
        tree.remove(toBytes(keys[i]));
    for (int i = 0; i < N; ++i)
        EXPECT_TRUE(tree.search(toBytes(keys[i])).empty());
}

TEST_F(BPTreeTest, RandomBinaryKeyValue) {
    BPTree tree;
    std::mt19937 rng(42);
    std::uniform_int_distribution<uint8_t> dist(0, 255);

    for (int i = 0; i < 1000; ++i) {
        ByteArray key(10), val(20);
        for (auto &b : key)
            b = std::byte(dist(rng));
        for (auto &b : val)
            b = std::byte(dist(rng));
        tree.insert(key, val);
        EXPECT_EQ(tree.search(key), val);
    }
}

TEST_F(BPTreeTest, ZeroAndLongKeysAndValues) {
    BPTree tree;
    ByteArray empty;
    ByteArray bigKey(4096, std::byte('K')), bigVal(4096, std::byte('V'));
    tree.insert(empty, empty);
    tree.insert(bigKey, bigVal);

    EXPECT_EQ(tree.search(empty), empty);
    EXPECT_EQ(tree.search(bigKey), bigVal);

    tree.remove(empty);
    tree.remove(bigKey);

    EXPECT_TRUE(tree.search(empty).empty());
    EXPECT_TRUE(tree.search(bigKey).empty());
}

TEST_F(BPTreeTest, InsertDeleteChurn) {
    BPTree tree;
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

TEST_F(BPTreeTest, SaveImmediatelyAfterInsert) {
    for (int rep = 0; rep < 5; ++rep) {
        BPTree tree;
        for (int i = 0; i < 50; ++i) {
            tree.insert(toBytes("key" + std::to_string(i)), toBytes("val" + std::to_string(i)));
            tree.saveTree(testDir);
            BPTree reloaded;
            reloaded.loadTree(testDir);
            for (int j = 0; j <= i; ++j)
                EXPECT_EQ(fromBytes(reloaded.search(toBytes("key" + std::to_string(j)))), "val" + std::to_string(j));
        }
    }
}

TEST_F(BPTreeTest, RandomRemoveOrder) {
    BPTree tree;
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

TEST_F(BPTreeTest, EmptyStringKeyAndValue) {
    BPTree tree;
    tree.insert(ByteArray(), ByteArray());
    tree.insert(toBytes(""), toBytes("not empty"));
    tree.insert(toBytes("not empty"), toBytes(""));

    EXPECT_EQ(tree.search(ByteArray()), toBytes("not empty"));
    EXPECT_EQ(tree.search(toBytes("")), toBytes("not empty"));
    EXPECT_EQ(fromBytes(tree.search(toBytes("not empty"))), "");
}

TEST_F(BPTreeTest, OverwriteValueManyTimes) {
    BPTree tree;
    tree.insert(toBytes("repeat"), toBytes("1"));
    for (int i = 2; i <= 100; ++i) {
        tree.insert(toBytes("repeat"), toBytes(std::to_string(i)));
        EXPECT_EQ(fromBytes(tree.search(toBytes("repeat"))), std::to_string(i));
    }
}

TEST(BPTreeIntegrityTest, ParentPointerConsistencyAfterSplitAndMerge) {
    BPTree tree;
    for (int i = 0; i < 100; ++i) {
        ByteArray k = {std::byte('k'), std::byte(i)};
        ByteArray v = {std::byte('v'), std::byte(i)};
        tree.insert(k, v);
    }
    tree.traverse();
    EXPECT_TRUE(tree.checkParentPointers());

    for (int i = 0; i < 90; ++i) {
        ByteArray k = {std::byte('k'), std::byte(i)};
        tree.remove(k);
    }
    tree.traverse();
    EXPECT_TRUE(tree.checkParentPointers());
}

TEST(BPTreeIntegrityTest, ParentPointerConsistencyOnInsertSplitMerge) {
    BPTree tree;
    for (int i = 0; i < 100; ++i)
        tree.insert(ByteArray({std::byte('k'), std::byte(i)}), ByteArray({std::byte('v'), std::byte(i)}));
    tree.saveTree("test_tree_dir_integrity1");
    EXPECT_TRUE(tree.checkAllParentPointersStrict());

    for (int i = 0; i < 95; ++i)
        tree.remove(ByteArray({std::byte('k'), std::byte(i)}));
    tree.saveTree("test_tree_dir_integrity2");
    EXPECT_TRUE(tree.checkAllParentPointersStrict());
}

TEST(BPTreeIntegrityTest, ParentPointerAfterReload) {
    {
        BPTree tree;
        for (int i = 0; i < 40; ++i)
            tree.insert(ByteArray({std::byte('a'), std::byte(i)}), ByteArray({std::byte('v'), std::byte(i)}));
        tree.saveTree("test_tree_dir_integrity3");
    }
    {
        BPTree tree2;
        tree2.loadTree("test_tree_dir_integrity3");
        EXPECT_TRUE(tree2.checkAllParentPointersStrict());
    }
}

TEST_F(BPTreeTest, WALRecovery) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("foo"), toBytes("val1"));
        tree.insert(toBytes("bar"), toBytes("val2"));
        tree.remove(toBytes("foo"));
        tree.saveTree(testDir);
    }

    std::filesystem::remove_all(testDir);

    BPTree tree2;
    tree2.recoverFromWAL("test_wal_recovery");
    EXPECT_TRUE(tree2.search(toBytes("foo")).empty());
    EXPECT_EQ(fromBytes(tree2.search(toBytes("bar"))), "val2");
}

TEST_F(BPTreeTest, WALRecovery_OnlyInsert) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("A"), toBytes("a"));
        tree.insert(toBytes("B"), toBytes("b"));
        tree.insert(toBytes("C"), toBytes("c"));
    }
    std::filesystem::remove_all(testDir);

    BPTree tree2;
    tree2.recoverFromWAL(testDir);
    EXPECT_EQ(fromBytes(tree2.search(toBytes("A"))), "a");
    EXPECT_EQ(fromBytes(tree2.search(toBytes("B"))), "b");
    EXPECT_EQ(fromBytes(tree2.search(toBytes("C"))), "c");
}

TEST_F(BPTreeTest, WALRecovery_InsertRemoveMix) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("A"), toBytes("a"));
        tree.insert(toBytes("B"), toBytes("b"));
        tree.remove(toBytes("A"));
        tree.insert(toBytes("C"), toBytes("c"));
        tree.remove(toBytes("C"));
    }
    std::filesystem::remove_all(testDir);

    BPTree tree2;
    tree2.recoverFromWAL(testDir);
    EXPECT_TRUE(tree2.search(toBytes("A")).empty());
    EXPECT_EQ(fromBytes(tree2.search(toBytes("B"))), "b");
    EXPECT_TRUE(tree2.search(toBytes("C")).empty());
}

TEST_F(BPTreeTest, WALRecovery_DuplicateKeyInsert) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("dup"), toBytes("one"));
        tree.insert(toBytes("dup"), toBytes("two")); // overwrite
        tree.insert(toBytes("dup"), toBytes("three"));
    }
    std::filesystem::remove_all(testDir);

    BPTree tree2;
    tree2.recoverFromWAL(testDir);
    EXPECT_EQ(fromBytes(tree2.search(toBytes("dup"))), "three");
}

TEST_F(BPTreeTest, WALRecovery_RandomOrderAndCrash) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("a"), toBytes("1"));
        tree.insert(toBytes("b"), toBytes("2"));
        tree.remove(toBytes("a"));
        // WAL途中で「壊れた」ふり（ファイル途中で切る）
        std::ofstream ofs("tree_wal.log", std::ios::in | std::ios::out | std::ios::binary);
        ofs.seekp(0, std::ios::end);
        auto pos = ofs.tellp();
        ofs.seekp(pos - 6); // 適当な位置でぶった切る
        ofs.close();
    }
    std::filesystem::remove_all(testDir);

    BPTree tree2;
    // 途中で止まっても壊れないこと（例外をcatchし、部分復旧を期待）
    EXPECT_NO_THROW(tree2.recoverFromWAL(testDir));
}

TEST_F(BPTreeTest, WALRecovery_Idempotency) {
    std::filesystem::remove("tree_wal.log");
    {
        BPTree tree;
        tree.insert(toBytes("foo"), toBytes("v1"));
        tree.insert(toBytes("bar"), toBytes("v2"));
        tree.remove(toBytes("foo"));
    }
    std::filesystem::remove_all(testDir);

    // 2回連続でリカバリしても同じ結果（冪等性）
    for (int i = 0; i < 2; ++i) {
        BPTree tree2;
        tree2.recoverFromWAL(testDir);
        EXPECT_TRUE(tree2.search(toBytes("foo")).empty());
        EXPECT_EQ(fromBytes(tree2.search(toBytes("bar"))), "v2");
    }
}

TEST_F(BPTreeTest, WALRecovery_EmptyWAL) {
    std::filesystem::remove("tree_wal.log");
    {
        // 書かない
    }
    std::filesystem::remove_all(testDir);

    BPTree tree2;
    tree2.recoverFromWAL(testDir);
    // 何もないはず
    EXPECT_TRUE(tree2.search(toBytes("A")).empty());
    EXPECT_TRUE(tree2.search(toBytes("")).empty());
}