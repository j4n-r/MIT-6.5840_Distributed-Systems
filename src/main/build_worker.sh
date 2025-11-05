set -xe
go build -gcflags  "all=-N -l" -buildmode=plugin ../mrapps/wc.go
go build -gcflags  "all=-N -l" mrworker.go

#./mrworker wc.so

