package raft

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 300
)
