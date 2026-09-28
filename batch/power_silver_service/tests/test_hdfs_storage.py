from power_silver.storage import FileStatus, HdfsLakeStorage


class StatusWalkClient:
    def status(self, path, strict=True):
        return {"type": "DIRECTORY"}

    def walk(self, path, status=False):
        assert status is True
        yield ((path, {"type": "DIRECTORY"}), [], [
            ("part-b.parquet", {"length": 20, "modificationTime": 2}),
            ("part-a.parquet", {"length": 10, "modificationTime": 1}),
        ])


def test_hdfs_walk_files_accepts_status_directory_tuple():
    storage = HdfsLakeStorage(StatusWalkClient(), "hdfs://namenode:9000")

    assert storage.walk_files("/nilm/silver/power_clean") == [
        FileStatus("/nilm/silver/power_clean/part-a.parquet", 10, 1),
        FileStatus("/nilm/silver/power_clean/part-b.parquet", 20, 2),
    ]
