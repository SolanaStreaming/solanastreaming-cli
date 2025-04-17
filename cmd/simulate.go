package main

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"math/rand"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type SimulateTask struct {
	nextSubID  uint
	outputFeed chan JSONRPC
	pairsSubID uint
	swapsSubID uint
	params     struct {
		fromDate string
		fromSlot uint
		dataDir  string
		port     uint
	}
}

const (
	MethodStartSimulation  = "startSimulation"
	MethodNewPairSubscribe = "newPairSubscribe"
	MethodSwapSubscribe    = "swapSubscribe"
)

func NewSimulateTask() *SimulateTask {
	return &SimulateTask{
		nextSubID:  1,
		outputFeed: make(chan JSONRPC, 1),
	}
}

func (o *SimulateTask) SetupParameters(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.params.fromDate, "from-date", "f", "", "Specify when to start the simulation from. Format: YYYY-MM-DD. If none specified, it will run with all the consecutive files in the data dir.")
	cmd.Flags().UintVarP(&o.params.fromSlot, "from-slot", "s", 0, "Specify the slot to start the simulation from. The from-date param must also be provided")
	cmd.Flags().StringVarP(&o.params.dataDir, "data-dir", "d", "out", "The dir to get the data from for streaming")
	cmd.Flags().UintVarP(&o.params.port, "port", "p", 8000, "The port the websocket server will bind to on localhost")
}

func (o *SimulateTask) GetMeta() Meta {
	return Meta{
		Name:        "SimulateTask",
		Use:         "simulate",
		Description: "Simulate the SolanaStreaming websocket server with archive data. A websocker server will run the the specified port bound to localhost. You can subscribe to the endpoints as normal but to start the simulation, you must send the 'startSimulation' command.",
	}
}

type JSONRPC struct {
	ID             int             `json:"id,omitempty"`
	SubscriptionID uint            `json:"subscription_id,omitempty"`
	Method         string          `json:"method"`
	Params         json.RawMessage `json:"params"`
}

func (o *SimulateTask) Execute(ctx context.Context) error {
	if err := o.validateParams(); err != nil {
		return err
	}
	upgrader := websocket.Upgrader{} // use default options
	websocket := func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logrus.Errorf("upgrade: %s", err.Error())
			return
		}
		logrus.Infof("websocket connection established")
		defer func() {
			logrus.Infof("websocket connection closed")
		}()
		defer c.Close()
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				logrus.Errorf("read: %s", err.Error())
				break
			}
			jsonrpc := JSONRPC{}
			err = json.Unmarshal(message, &jsonrpc)
			if err != nil {
				logrus.Errorf("unmarshal: %s", err.Error())
				break
			}
			switch jsonrpc.Method {
			case MethodStartSimulation:
				go func() {
					for {
						v, open := <-o.outputFeed
						if !open {
							return
						}
						raw, err := json.Marshal(v)
						if err != nil {
							logrus.Errorf("write: %s", err.Error())
							break
						}
						err = c.WriteMessage(websocket.TextMessage, raw)
						if err != nil {
							logrus.Errorf("write: %s", err.Error())
							break
						}
					}
				}()

				err = o.RunSimulation(ctx, rand.Intn(100000))
				if err != nil {
					logrus.Errorf("run simulation: %s", err.Error())
				}
				logrus.Infof("simulation finished, disconnecting clients...")
				return
			case MethodNewPairSubscribe:
				o.pairsSubID = o.nextSubID
				err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"id":%d,"result":{"subscription_id":%d}}`, jsonrpc.ID, o.pairsSubID)))
				if err != nil {
					logrus.Errorf("read: %s", err.Error())
					break
				}
				o.nextSubID++
			case MethodSwapSubscribe:
				o.swapsSubID = o.nextSubID
				err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"id":%d,"result":{"subscription_id":%d}}`, jsonrpc.ID, o.swapsSubID)))
				if err != nil {
					logrus.Errorf("read: %s", err.Error())
					break
				}
				o.nextSubID++
			default:
				logrus.Errorf("unknown method: %s", jsonrpc.Method)
			}
		}
	}

	logrus.Infof("To start a simulation, connect to the websocket, subscribe to the desired feed, then send the startSimulation method. Your subscriptions will then receive events")
	logrus.Infof("Websocket server listening on localhost:%d configured with data in dir: %s", o.params.port, o.params.dataDir)
	http.HandleFunc("/", websocket)
	return http.ListenAndServe(fmt.Sprintf("localhost:%d", o.params.port), nil)
}

func (o *SimulateTask) RunSimulation(ctx context.Context, simID int) error {
	dataFiles, err := o.getDataFiles()
	if err != nil {
		return err
	}
	slot := uint64(0)
	events := 0
	for dataFileNum, v := range dataFiles {
		logrus.Infof("running sim data from file (%d of %d) %s", dataFileNum+1, len(dataFiles), v)
		// unzip file and write to disk to keep mem usage low
		r, err := zip.OpenReader(o.params.dataDir + "/" + v)
		if err != nil {
			return err
		}
		unzippedFiles := []string{}
		// Iterate through the files in the archive,
		logrus.Debugf("unzipping files %s", v)
		start := time.Now()
		for _, f := range r.File {
			rc, err := f.Open()
			if err != nil {
				return err
			}
			tmpFile := fmt.Sprintf("%s.%d", f.Name, simID)
			outFile, err := os.OpenFile(fmt.Sprintf("%s/%s", o.params.dataDir, tmpFile), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			_, err = io.Copy(outFile, rc)
			if err != nil {
				return err
			}
			rc.Close()
			outFile.Close()
			unzippedFiles = append(unzippedFiles, tmpFile)
		}
		logrus.Debugf("unzipped %s in %s", v, time.Since(start))
		start = time.Now()
		r.Close()

		// get the starting slot
		if dataFileNum == 0 {
			slot, err = o.getStartingSlot(unzippedFiles)
			if err != nil {
				return err
			}
			logrus.Infof("starting slot: %d", slot)
			logrus.Debugf("got starting slot in %s", time.Since(start))
		}

		// go through data files
		dataChans := make([]chan []byte, len(unzippedFiles))
		for i, v := range unzippedFiles {
			dataChans[i] = make(chan []byte, 1)
			err := o.streamFromFile(v, dataChans[i])
			if err != nil {
				return err
			}
		}

		buffers := make([][]byte, len(dataChans))
		dones := make([]bool, len(dataChans))
		for {
			for i, dataChan := range dataChans {
				for {
					// used buffered row before checking the channel
					var dataRow []byte
					if len(buffers[i]) != 0 {
						dataRow = buffers[i]
					} else {
						dataRow = <-dataChan
					}
					if len(dataRow) == 0 {
						dones[i] = true
						break
					}
					data := DataFormat{}
					err := json.Unmarshal(dataRow, &data)
					if err != nil {
						return errors.Wrap(err, "cant unmarshal event")
					}

					// if we are in the future, save the row for later and continue
					if data.Slot > slot {
						buffers[i] = dataRow
						break
					} else {
						buffers[i] = []byte{}
					}

					// at this point we should be in order so post
					// fmt.Println(string(dataRow))
					ev := JSONRPC{}
					if o.pairsSubID != 0 && data.Pair != nil {
						ev.Method = "newPairNotification"
						ev.Params = dataRow
						ev.SubscriptionID = (o.pairsSubID)
						o.outputFeed <- ev
					}
					if o.swapsSubID != 0 && data.Swap != nil {
						ev.Method = "swapNotification"
						ev.Params = dataRow
						ev.SubscriptionID = (o.swapsSubID)
						o.outputFeed <- ev
					}
					events++
				}
			}
			// fmt.Println("events, ", events)
			// fmt.Println("slot, ", slot)
			done := true
			for _, v := range dones {
				done = done && v
			}
			if done {
				break
			}
			slot++
		}
	}
	logrus.Infof("simulated events: %d", events)
	logrus.Infof("ending slot: %d", slot-1)

	return nil
}

type DataFormat struct {
	Slot uint64    `json:"slot"`
	Pair *struct{} `json:"pair"`
	Swap *struct{} `json:"swap"`
}

func (o *SimulateTask) validateParams() error {
	if o.params.fromSlot != 0 && o.params.fromDate == "" {
		return errors.New("from-date must be specified when from-slot is set")
	}
	return nil
}

func (o *SimulateTask) getDataFiles() ([]string, error) {
	// loop through dir contents
	files, err := os.ReadDir(o.params.dataDir)
	if err != nil {
		return nil, err
	}
	filtered := []string{}
	for _, v := range files {
		if v.IsDir() {
			continue
		}
		// check extension
		if len(v.Name()) < 4 || v.Name()[len(v.Name())-4:] != ".zip" {
			continue
		}
		filtered = append(filtered, v.Name())
	}
	// ensure they are ordered by date (oldest first)
	return filtered, nil
}

func (o *SimulateTask) streamFromFile(fileName string, rows chan []byte) error {
	go func() {

		file, err := os.Open(o.params.dataDir + "/" + fileName)
		if err != nil {
			logrus.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		// optionally, resize scanner's capacity for lines over 64K, see next example
		for scanner.Scan() {
			row := scanner.Bytes()
			// make a copy otherwise row buf is overwritten by goroutines before being used down the line
			buf := make([]byte, len(row))
			copy(buf, row)
			rows <- buf
		}

		if err := scanner.Err(); err != nil {
			logrus.Fatal(err)
		}
		// delete file
		err = os.Remove(o.params.dataDir + "/" + fileName)
		if err != nil {
			logrus.Warnf("could not delete interrim file (your disk space may be used up quickly) %s: %s", fileName, err.Error())
		}
		close(rows)
	}()
	return nil
}

func (o *SimulateTask) getStartingSlot(unzippedFiles []string) (uint64, error) {
	var startingSlot uint64
	for _, v := range unzippedFiles {
		file, err := os.Open(o.params.dataDir + "/" + v)
		if err != nil {
			return 0, err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			row := scanner.Bytes()
			data := DataFormat{}
			err := json.Unmarshal(row, &data)
			if err != nil {
				return 0, errors.Wrap(err, "cant unmarshal event")
			}
			if data.Slot < startingSlot || startingSlot == 0 {
				startingSlot = data.Slot
			}
			break
		}

		if err := scanner.Err(); err != nil {
			return 0, err
		}
	}
	return startingSlot, nil
}
