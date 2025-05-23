#include "mylib.h"
#include <gtest/gtest.h>

TEST(MathTest, Add) {
    EXPECT_EQ(add(2, 3), 5);
}
