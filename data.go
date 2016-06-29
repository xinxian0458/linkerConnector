package main

import (
	linuxproc "github.com/c9s/goprocinfo/linux"
)

//ProcessDetail :
type ProcessDetail struct {
	ProcID     uint64                  `json:"proc_id"`
	StatusInfo linuxproc.ProcessStatus `json:"status_info"`
	StateInfo  linuxproc.ProcessStat   `json:"stat_info"`
}

//ProcessInfo :
type ProcessInfo struct {
	MachineID string `json:"machine_id"`
	//Timestamp : Unix time
	Timestamp int64 `json:"timestamp"`

	Procs []ProcessDetail `json:"procs"`
}

//MachineInfo :Machine information
type MachineInfo struct {
	MachineID string `json:"machine_id"`
	//Timestamp : Unix time
	Timestamp int64 `json:"timestamp"`
	CPUInfo   []struct {
		Processor string `json:"processor"`
		Model     string `json:"model"`
		ModelName string `json:"model_name"`
		CPUMHz    int    `json:"cpu MHz"`
		CacheSize int    `json:"cache size"`
	} `json:"cpu_info"`
	MemInfo struct {
		MemTotal     int `json:"MemTotal"`
		MemFree      int `json:"MemFree"`
		MemAvailable int `json:"MemAvailable"`
	} `json:"mem_info"`
	NetInfo []struct {
		Protocal   string `json:"protocal"`
		Mac        string `json:"mac"`
		IP         string `json:"ip"`
		Rate       string `json:"rate"`
		Errs       string `json:"errs"`
		Drop       string `json:"drop"`
		Compressed string `json:"compressed"`
	} `json:"net_info"`
	DiskInfo struct {
		Io int `json:"io"`
	} `json:"disk_info"`
}