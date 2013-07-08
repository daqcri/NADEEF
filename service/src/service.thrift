namespace java qa.qcri.nadeef.service.thrift

enum TRuleType {
    UDF = 0,
    FD = 1,
    CFD = 2
}

struct TRule {
    1: string name,
    2: TRuleType type,
    3: string code
}


service TNadeefService {
    string generate(1:TRule rule),
    i32 verify(1: TRule rule)
}