# SQL Standard Change Summary

This document summarizes the changes introduced in commit `fe81e33` (`AG-190 add sql standard in Behaviour`).

## Goal

Add Go SDK support for DolphinDB SQL standard selection so the client can behave consistently with the existing Python SDK implementation.

The supported SQL standards are:

- `DolphinDB`
- `Oracle`
- `MySQL`

## Protocol Compatibility

The implementation is intentionally aligned with the Python/C++ SDK behavior.

Reference implementation from Python SDK:

- `python-client-sdk/dolphindb/settings.py`
  - `SqlStd.DolphinDB = 0`
  - `SqlStd.Oracle = 1`
  - `SqlStd.MySQL = 2`
- `python-client-sdk/core/src/DolphinDBImp.cpp`
  - request flag contains `sqlStd << 19`

Go SDK follows the same numeric mapping and bit layout.

## Files Changed

### `dialer/behavior.go`

Added:

- `type SQLStandard int`
- enum values:
  - `SQLStandardDolphinDB = 0`
  - `SQLStandardOracle = 1`
  - `SQLStandardMySQL = 2`
  - `SQLStandardDefault = SQLStandardDolphinDB`
- `BehaviorOptions.SQLStandard`
- `(*BehaviorOptions).SetSQLStandard(...)`

Purpose:

- expose SQL standard selection as part of connection behavior configuration

### `dialer/util.go`

Updated `generatorRequestFlag(opt *BehaviorOptions)`.

Added protocol encoding:

```go
if opt.SQLStandard != SQLStandardDolphinDB {
    flag += int(opt.SQLStandard) << 19
}
```

Purpose:

- send SQL standard through the existing DolphinDB request flag field
- keep wire behavior compatible with Python/C++

### `dialer/dialer.go`

Added:

- `NewSimpleConnWithBehavior(...)`

Purpose:

- allow lower-level callers to create a logged-in connection while still passing `BehaviorOptions`

Note:

- product-facing SQL standard usage is still expected to go through `api.NewDolphinDBClient(..., behavior)` rather than through a new high-level simple client API

### `api/pool.go`

Added:

- `PoolOption.SQLStandard`

And passed it into internal `dialer.BehaviorOptions` when pool connections are created.

Purpose:

- make `DBConnectionPool` support SQL standard selection
- this is kept because pool tasks may execute user-provided SQL scripts

### `example/sql/main.go`

Added a runnable example program.

Behavior:

- connects to a real DolphinDB server
- creates three sessions with different SQL standards
- runs the same SQL: `sysdate()`
- prints success/failure for each standard

Purpose:

- give test engineers a quick manual verification tool
- demonstrate the expected difference:
  - DolphinDB standard: `sysdate()` should fail
  - Oracle/MySQL standard: `sysdate()` should succeed

## Scope Deliberately Not Extended

The following areas were intentionally left unchanged:

- `streaming`
- `multigoroutinetable`

Reason:

- these modules do not directly expose user-entered SQL strings as their main API surface
- to keep the configuration boundary smaller and clearer

## Expected Usage

### Client

```go
behavior := (&dialer.BehaviorOptions{}).
    SetSQLStandard(dialer.SQLStandardOracle)

db, err := api.NewDolphinDBClient(context.TODO(), addr, behavior)
```

Then:

```go
err = db.Connect()
err = db.Login(new(api.LoginRequest).SetUserID(user).SetPassword(password))
```

### Connection Pool

```go
opt := &api.PoolOption{
    Address:     addr,
    UserID:      user,
    Password:    password,
    PoolSize:    1,
    SQLStandard: dialer.SQLStandardMySQL,
}
```

## Recommended Verification

### Manual

Run:

```bash
go run ./example/sql
```

Expected:

- `DolphinDB` session reports failure on `sysdate()`
- `Oracle` session reports success
- `MySQL` session reports success

### Automated

There is a small protocol-level test for flag encoding:

- `dialer/sql_std_test.go`

It verifies:

- `DolphinDB -> 0`
- `Oracle -> 1 << 19`
- `MySQL -> 2 << 19`

## Notes for Testing

- server-side code was not changed
- behavior depends on existing server support for the SQL standard bits in the request flag
- if manual verification differs from Python SDK behavior, the first thing to check is whether the request path is using `BehaviorOptions` correctly
