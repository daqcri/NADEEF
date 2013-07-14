/**
 * NADEEF service thrift definition.
 */
namespace java qa.qcri.nadeef.service.thrift

enum TRuleType {
    UDF = 0,
    FD = 1,
    CFD = 2
}

enum TNadeefExceptionType{
    UNKNOWN = 0,
    COMPILE_ERROR = 1,
    INVALID_RULE = 2,
}

enum TJobStatusType {
    WAITING = 0,
    RUNNING = 1,
    NOTAVAILABLE = 2
}

struct TJobStatus {
    1: TJobStatusType status,
    2: i32 progress
}

struct TRule {
    1: string name,
    2: TRuleType type,
    3: string code
}

exception TNadeefRemoteException {
    1:TNadeefExceptionType type,
    2:optional string message
}

service TNadeefService {
    /**
     * Generates UDF code for the rule.
     */
    string generate(1:TRule rule) throws (1:TNadeefRemoteException re),

    /**
     * Verify the given rule.
     */
    bool verify(1: TRule rule),

    /**
     * Detect with the given rule.
     */
    string detect(1: TRule rule, 2: string tableName) throws (1:TNadeefRemoteException re),

    /**
     * Repair with the given rule.
     */
    string repair(1: TRule rule, 2: string tableName) throws (1:TNadeefRemoteException re),

    /**
     * Gets status of a specific job.
     */
    TJobStatus getJobStatus(1: string key),

    /**
     * Gets all job status.
     */
    list<TJobStatus> getAllJobStatus()
}