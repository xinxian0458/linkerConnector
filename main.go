package main

import (
	"bufio"
	"fmt"
	sendr "github.com/LinkerNetworks/linkerConnector/sender"
	"github.com/spf13/cobra"
	"io"
	"net"
	"net/textproto"
	"os"
	"runtime"
	"time"
)

var (
	send *sendr.Sender
)

func main() {
	send = sendr.NewSender("linkerConnector")
	var sourceType, sourceAddr string
	var serverAddr, topic, dest, cAdvisorAddr, readProcPath string
	var interval int
	var usingPipe, disableFile bool
	rootCmd := &cobra.Command{
		Use:   "linkerConnector",
		Short: "Linux system data collector and send to target destination",
		Run: func(cmd *cobra.Command, args []string) {
			if usingPipe {
				info, _ := os.Stdin.Stat()

				if info.Size() > 0 {
					reader := bufio.NewReader(os.Stdin)
					processPipe(reader, dest, serverAddr, topic, disableFile)
				}
				return
			}
			if runtime.GOOS != "linux" {
				fmt.Println("Collect data from linux for now, application exit.")
				return
			}

			if sourceType == "remote" {
				if sourceAddr == "" {
					fmt.Println("Need source address when sourctType is remote")
					return
				}
				fmt.Println("Collect data from", serverAddr)
				conn, err := net.Dial("tcp", sourceAddr)
				if err != nil {
					fmt.Println("Connect failed!", err)
					return
				}
				defer conn.Close()

				reader := bufio.NewReader(conn)
				tp := textproto.NewReader(reader)
				for {
					// read one line (ended with \n or \r\n)
					sparklogInfo, err := tp.ReadLine()
					if err != nil {
						fmt.Println("failed to read line!", err)
						return
					}
					fmt.Println("--------------", sparklogInfo)
					send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: serverAddr, Topic: topic, Key: "sparklog", Value: sparklogInfo, DisableFileSave: disableFile})
				}
			} else {
				fmt.Println("Collect data from local server.")
				for {
					data := NewDataCollector()
					procInfo := data.GetProcessInfo(cAdvisorAddr, readProcPath)
					machineInfo := data.GetMachineInfo(readProcPath)
					send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: serverAddr, Topic: topic, Key: "ProcessInfo", Value: procInfo, DisableFileSave: disableFile})
					send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: serverAddr, Topic: topic, Key: "MachineInfo", Value: machineInfo, DisableFileSave: disableFile})
					if interval == 0 {
						return
					}
					time.Sleep(time.Millisecond * time.Duration(interval))
				}
			}
		},
	}

	rootCmd.Flags().IntVarP(&interval, "interval", "i", 0, "Interval to retrieval data(millisecond), default 0 is not repeat.")

	rootCmd.Flags().StringVarP(&sourceType, "sourceType", "m", "remote", "Type of the source, local or remote")
	rootCmd.Flags().StringVarP(&sourceAddr, "sourceAddr", "a", "127.0.0.1:9999", "Remote socket address, (e.g. hostip:port)")

	rootCmd.Flags().StringVarP(&readProcPath, "readProcPath", "r", "/proc", "File path of proc for linkerConnector to read from.")
	rootCmd.Flags().StringVarP(&serverAddr, "server", "s", "", "The comma separated list of server could be brokers in the Kafka cluster or spark address")
	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "The topic to kafka produce")
	rootCmd.Flags().StringVarP(&dest, "dest", "d", "stdout", "Destination to kafka, spark and stdout")
	rootCmd.Flags().StringVarP(&cAdvisorAddr, "cAdvisorAddr", "c", "", "Http Url and port for cAdvisor REST API (e.g. http://hostip:port)")
	rootCmd.Flags().BoolVarP(&usingPipe, "pipe", "p", false, "Using pipe mode to forward data")
	rootCmd.Flags().BoolVarP(&disableFile, "dsiableFileSave", "f", true, "Disable local file save.")

	rootCmd.Execute()

}

func processPipe(reader *bufio.Reader, dest, serverAddr, topic string, disableFileSave bool) {
	line := 1
	for {
		input, err := reader.ReadString('\n')
		if err != nil && err == io.EOF {
			break
		}
		send.SendData(sendr.SendDataParam{Dest: dest, SerAddr: serverAddr, Topic: topic, Key: "Pipe", Value: input, DisableFileSave: disableFileSave})
		line++
	}
}
