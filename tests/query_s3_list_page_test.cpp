#include <servicelib/dataquery/s3/list_page.hpp>
#include <gtest/gtest.h>

namespace {
using servicelib::dataquery::s3::detail::nextListMarker;
TEST(QueryS3ListPage, TerminalAndEmptyPages) {
  EXPECT_FALSE(nextListMarker("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>", ""));
  EXPECT_FALSE(nextListMarker("<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>last</Key></Contents></ListBucketResult>", "before"));
}
TEST(QueryS3ListPage, MarkerAndFallbackPreserveDecodedKeys) {
  EXPECT_EQ(nextListMarker("<ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'><IsTruncated>true</IsTruncated><NextMarker>b&amp;c</NextMarker></ListBucketResult>", "a"), "b&c");
  EXPECT_EQ(nextListMarker("<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>b</Key></Contents><Contents><Key>c</Key></Contents></ListBucketResult>", "a"), "c");
}
TEST(QueryS3ListPage, RejectsMalformedOrNonAdvancingPages) {
  for (const auto* xml : {"", "<broken", "<Other/>",
       "<ListBucketResult/>",
       "<ListBucketResult><IsTruncated>maybe</IsTruncated></ListBucketResult>",
       "<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>",
       "<ListBucketResult><IsTruncated>true</IsTruncated><NextMarker>a</NextMarker></ListBucketResult>",
       "<ListBucketResult><IsTruncated>true</IsTruncated><NextMarker>0</NextMarker></ListBucketResult>"}) {
    EXPECT_THROW(nextListMarker(xml, "a"), std::runtime_error);
  }
}
}
