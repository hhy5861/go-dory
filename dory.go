package go_dory

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"gopkg.in/fatih/pool.v2"
)

var (
	connetLock sync.RWMutex
	conn       pool.Pool
	err        error
)

const (
	MSGSIZEFIELDSIZE    = 4
	APIKEYFIELDSIZE     = 2
	APIVERSIONFIELDSIZE = 2

	FLAGSFIELDSIZE        = 2
	PARTITIONKEYFIELDSIZE = 4
	TOPICSIZEFIELDSIZE    = 2
	TIMESTAMPFIELDSIZE    = 8
	KEYSIZEFIELDSIZE      = 4
	VALUESIZEFIELDSIZE    = 4

	ANYPARTITIONFIXEDBYTES = MSGSIZEFIELDSIZE + APIKEYFIELDSIZE + APIVERSIONFIELDSIZE +
		FLAGSFIELDSIZE + TOPICSIZEFIELDSIZE +
		TIMESTAMPFIELDSIZE + KEYSIZEFIELDSIZE +
		VALUESIZEFIELDSIZE

	PARTITIONKEYFIXEDBYTES = ANYPARTITIONFIXEDBYTES + PARTITIONKEYFIELDSIZE

	ANYPARTITIONAPIKEY     = 256
	ANYPARTITIONAPIVERSION = 0

	PARTITIONKEYAPIKEY     = 257
	PARTITIONKEYAPIVERSION = 0
)

type DoryServer struct {
	Topic      string
	Host       string
	Post       int64
	InitialCap int
	MaxCap     int
}

/**
 * 创建TCP连接
 * start
 */
func (dory *DoryServer) Connection() error {
	connetLock.Lock()
	defer connetLock.Unlock()

	factory := func() (net.Conn, error) {
		address := fmt.Sprintf("%s:%d", dory.Host, dory.Post)
		return net.Dial("tcp", address)
	}

	conn, err = pool.NewChannelPool(dory.InitialCap, dory.MaxCap, factory)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

/**
 * connection 维护
 */
func (dory *DoryServer) getInstance() (net.Conn, error) {
	defer func() {
		es := recover()
		if es != nil {
			log.Fatal(es)
		}
	}()

	if conn == nil {
		err = dory.Connection()
		if err != nil {
			return nil, err
		}
	}

	c, errs := conn.Get()
	if errs != nil {
		log.Fatal(err)
	}

	return c, err
}

/**
 * 发送数据
 */
func (dory *DoryServer) Send(message proto.Message) bool {
	var c net.Conn
	c, err = dory.getInstance()
	defer c.Close()

	if err != nil {
		conn = nil
		c, err = dory.getInstance()
	}

	msg := dory.MessageFormat(message)
	_, errs := c.Write(msg)
	if errs != nil {
		return false
	}

	return true
}

func (dory *DoryServer) MessageFormat(message proto.Message) (data []byte) {
	mData, err := proto.Marshal(message)
	if err != nil {
		fmt.Printf("Marshal Error : %v\n", err.Error())
		return
	}

	data, err = dory.createAnyPartitionMsg(dory.getEpochMilliseconds(), dory.Topic, "", mData)
	if err != nil {
		return
	}

	return
}

func (dory *DoryServer) getMaxTopicSize() int64 {
	i, _ := strconv.ParseInt("0x7fff", 0, 0)
	return i
}

func (dory *DoryServer) getMaxMsgSize() int64 {
	i, _ := strconv.ParseInt("0x7fffffff", 0, 0)
	return i
}

func (dory *DoryServer) createAnyPartitionMsg(timestamp int64, topic, key string, value []byte) ([]byte, error) {
	topicBytes := []byte(topic)
	valueBytes := value
	keyBytes := []byte(key)

	if int64(len(topicBytes)) > dory.getMaxTopicSize() {
		return nil, errors.New("Kafka topic is too large")
	}

	msgSize := ANYPARTITIONFIXEDBYTES + len(topicBytes) + len(keyBytes) + len(valueBytes)

	if int64(msgSize) > dory.getMaxMsgSize() {
		return nil, errors.New("Cannot create a message that large")
	}

	b := new(bytes.Buffer)

	pData := []interface{}{
		int32(msgSize),
		uint16(ANYPARTITIONAPIKEY),
		uint16(ANYPARTITIONAPIVERSION),
		uint16(0),
		int16(len(topicBytes)),
	}

	for _, v := range pData {
		err := binary.Write(b, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}

	b.Write(topicBytes)
	binary.Write(b, binary.BigEndian, timestamp)
	binary.Write(b, binary.BigEndian, int32(len(keyBytes)))
	b.Write(keyBytes)
	binary.Write(b, binary.BigEndian, int32(len(valueBytes)))
	b.Write(valueBytes)
	result := make([]byte, b.Len())
	b.Read(result)

	return result, nil
}

func (dory *DoryServer) createPartitionKeyMsg(timestamp int64, partitionkey int, topic, key, value string) ([]byte, error) {
	topicBytes := []byte(topic)
	valueBytes := []byte(value)
	keyBytes := []byte(key)

	if int64(len(topicBytes)) > dory.getMaxTopicSize() {
		return nil, errors.New("Kafka topic is too large")
	}

	msgSize := PARTITIONKEYFIXEDBYTES + len(topicBytes) + len(keyBytes) + len(valueBytes)
	if int64(msgSize) > dory.getMaxMsgSize() {
		return nil, errors.New("Cannot create a message that large")
	}

	b := new(bytes.Buffer)
	pData := []interface{}{
		int32(msgSize),
		uint16(PARTITIONKEYAPIKEY),
		uint16(PARTITIONKEYAPIVERSION),
		uint16(0),
		uint32(partitionkey),
		int16(len(topicBytes)),
	}

	for _, v := range pData {
		err := binary.Write(b, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}

	b.Write(topicBytes)
	binary.Write(b, binary.BigEndian, timestamp)
	binary.Write(b, binary.BigEndian, int32(len(keyBytes)))
	b.Write(keyBytes)
	binary.Write(b, binary.BigEndian, int32(len(valueBytes)))
	b.Write(valueBytes)
	p := make([]byte, b.Len())
	b.Read(p)

	return p, nil
}

func (dory *DoryServer) GetEpochMilliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
