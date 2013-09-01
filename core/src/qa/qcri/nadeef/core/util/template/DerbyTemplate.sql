InstallViolationTable(violationTableName) ::= <<
  CREATE TABLE $violationTableName$ (
      vid int,
      rid varchar(255),
      tablename varchar(63),
      tupleid int,
      attribute varchar(63),
      value varchar(32768)
  )
>>

InstallRepairTable(repairTableName) ::= <<
    CREATE TABLE $repairTableName$ (
        id int,
        vid int,
        c1_tupleid int,
        c1_tablename varchar(63),
        c1_attribute varchar(63),
        c1_value varchar(32768),
        op int,
        c2_tupleid int,
        c2_tablename varchar(63),
        c2_attribute varchar(63),
        c2_value varchar(32768)
    )
>>

InstallAuditTable(auditTableName) ::= <<
    CREATE TABLE $auditTableName$ (
        id serial primary key,
        vid int,
        tupleid int,
        tablename varchar(63),
        attribute varchar(63),
        oldvalue varchar(32768),
        newvalue varchar(32768),
        time timestamp
    )
>>