# ⚠️ Deprecated: delayTask is no longer maintained

This project is no longer maintained and has been archived.  
No further updates, issues, or pull requests will be accepted.

If you still rely on this package, please fork it and maintain your own version.

# delayTask

# usage 

```go
    dt:= delayTask.NewService("sample:svc", redisClient),

	dt.RegisterEventFunc(roomWorkerMessage.QEndChengFa, svr.EndChengFaHandleV1)
	dt.RegisterEventFunc(roomWorkerMessage.QEndPk, svr.EndPkHandleV1)
	
	dt.RegisterEvent(roomWorkerMessage.QEndChengFa,"",10)
```