namespace java qa.qcri.nadeef.lab.dedup

service TDedupService {
    list<i32> incrementalDedup(1:list<i32> newItems);
    void cureMissingValue();
}
