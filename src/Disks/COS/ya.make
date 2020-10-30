LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

CFLAGS(-g0)

SRCS(
    DiskCOS.cpp
    registerDiskCOS.cpp
)

END()
