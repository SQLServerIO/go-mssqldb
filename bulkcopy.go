package mssql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"strconv"

	"golang.org/x/net/context" // use the "x/net/context" for backwards compatibility.
)

// BulkInsert ...
type BulkInsert struct {
	cn          *MssqlConn
	metadata    []columnStruct
	bulkColumns []columnStruct
	columnsName []string
	tablename   string
	numRows     int

	headerSent bool
	Options    BulkInsertOptions
	Debug      bool
}

//BulkInsertOptions ...
type BulkInsertOptions struct {
	CheckConstraints  bool
	FireTriggers      bool
	KeepNulls         bool
	KilobytesPerBatch int
	RowsPerBatch      int
	Order             []string
	Tablock           bool
}

// DataValue ...
type DataValue interface{}

// CreateBulk ...
func (cn *MssqlConn) CreateBulk(table string, columns []string) (_ *BulkInsert) {
	b := BulkInsert{cn: cn, tablename: table, headerSent: false, columnsName: columns}
	b.Debug = false
	return &b
}

func (b *BulkInsert) sendBulkCommand() (err error) {
	//get table columns info
	err = b.getMetadata()
	if err != nil {
		return err
	}

	//match the columns
	for _, colname := range b.columnsName {
		var bulkCol *columnStruct

		for _, m := range b.metadata {
			if m.ColName == colname {
				bulkCol = &m
				break
			}
		}
		if bulkCol != nil {

			if bulkCol.ti.TypeId == typeUdt {
				//send udt as binary
				bulkCol.ti.TypeId = typeBigVarBin
			}
			b.bulkColumns = append(b.bulkColumns, *bulkCol)
			b.dlogf("Adding column %s %s %#x", colname, bulkCol.ColName, bulkCol.ti.TypeId)
		} else {
			return fmt.Errorf("Column %s does not exist in destination table %s", colname, b.tablename)
		}
	}

	//create the bulk command

	//columns definitions
	var colDefs bytes.Buffer
	for i, col := range b.bulkColumns {
		if i != 0 {
			colDefs.WriteString(", ")
		}
		colDefs.WriteString("[" + col.ColName + "] " + makeDecl(col.ti))
	}

	//options
	var withOpts []string

	if b.Options.CheckConstraints {
		withOpts = append(withOpts, "CHECK_CONSTRAINTS")
	}
	if b.Options.FireTriggers {
		withOpts = append(withOpts, "FIRE_TRIGGERS")
	}
	if b.Options.KeepNulls {
		withOpts = append(withOpts, "KEEP_NULLS")
	}
	if b.Options.KilobytesPerBatch > 0 {
		withOpts = append(withOpts, fmt.Sprintf("KILOBYTES_PER_BATCH = %d", b.Options.KilobytesPerBatch))
	}
	if b.Options.RowsPerBatch > 0 {
		withOpts = append(withOpts, fmt.Sprintf("ROWS_PER_BATCH = %d", b.Options.RowsPerBatch))
	}
	if len(b.Options.Order) > 0 {
		withOpts = append(withOpts, fmt.Sprintf("ORDER(%s)", strings.Join(b.Options.Order, ",")))
	}
	if b.Options.Tablock {
		withOpts = append(withOpts, "TABLOCK")
	}
	var withPart string
	if len(withOpts) > 0 {
		withPart = fmt.Sprintf("WITH (%s)", strings.Join(withOpts, ","))
	}

	query := fmt.Sprintf("INSERT BULK %s (%s) %s", b.tablename, colDefs.String(), withPart)

	stmt, err := b.cn.Prepare(query)
	if err != nil {
		return fmt.Errorf("Prepare failed: %s", err.Error())
	}
	b.dlogf(query)

	_, err = stmt.Exec(nil)
	if err != nil {
		return err
	}

	b.headerSent = true

	var buf = b.cn.sess.buf
	buf.BeginPacket(packBulkLoadBCP)

	// send the columns metadata
	columnMetadata := b.createColMetadata()
	_, err = buf.Write(columnMetadata)

	return
}

// AddRow immediately writes the row to the destination table.
// The arguments are the row values in the order they were specified.
func (b *BulkInsert) AddRow(row []interface{}) (err error) {
	if !b.headerSent {
		err = b.sendBulkCommand()
		if err != nil {
			return
		}
	}

	if len(row) != len(b.bulkColumns) {
		return fmt.Errorf("Row does not have the same number of columns than the destination table %d %d",
			len(row), len(b.bulkColumns))
	}

	bytes, err := b.makeRowData(row)
	if err != nil {
		return
	}

	_, err = b.cn.sess.buf.Write(bytes)
	if err != nil {
		return
	}

	b.numRows = b.numRows + 1
	return
}

func (b *BulkInsert) makeRowData(row []interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(tokenRow))

	var logcol bytes.Buffer
	for i, col := range b.bulkColumns {

		if b.Debug {
			logcol.WriteString(fmt.Sprintf(" col[%d]='%v' ", i, row[i]))
		}
		param, err := b.makeParam(row[i], col)
		if err != nil {
			return nil, fmt.Errorf("bulkcopy: %s", err.Error())
		}

		if col.ti.Writer == nil {
			return nil, fmt.Errorf("no writer for column: %s, TypeId: %#x",
				col.ColName, col.ti.TypeId)
		}
		err = col.ti.Writer(buf, param.ti, param.buffer)
		if err != nil {
			return nil, fmt.Errorf("bulkcopy: %s", err.Error())
		}
	}

	b.dlogf("row[%d] %s\n", b.numRows, logcol.String())

	return buf.Bytes(), nil
}

// Done ...
func (b *BulkInsert) Done() (rowcount int64, err error) {
	if !b.headerSent {
		//no rows had been sent
		return 0, nil
	}
	var buf = b.cn.sess.buf
	er := buf.WriteByte(byte(tokenDone))
	if er != nil {
		return 0, er
	}

	er = binary.Write(buf, binary.LittleEndian, uint16(doneFinal))
	if er != nil {
		return 0, er
	}

	er = binary.Write(buf, binary.LittleEndian, uint16(0)) //     curcmd
	if er != nil {
		return 0, er
	}

	if b.cn.sess.loginAck.TDSVersion >= verTDS72 {
		er = binary.Write(buf, binary.LittleEndian, uint64(0)) //rowcount 0
		if er != nil {
			return 0, er
		}

	} else {
		er = binary.Write(buf, binary.LittleEndian, uint32(0)) //rowcount 0
		if er != nil {
			return 0, er
		}
	}

	er = buf.FinishPacket()
	if er != nil {
		return 0, er
	}

	tokchan := make(chan tokenStruct, 5)
	go processResponse(context.Background(), b.cn.sess, tokchan)

	var rowCount int64
	for token := range tokchan {
		switch token := token.(type) {
		case doneStruct:
			if token.Status&doneCount != 0 {
				rowCount = int64(token.RowCount)
			}
			if token.isError() {
				return 0, token.getError()
			}
		case error:
			return 0, token
		}
	}
	return rowCount, nil
}

func (b *BulkInsert) createColMetadata() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(tokenColMetadata))                                    // token
	er := binary.Write(buf, binary.LittleEndian, uint16(len(b.bulkColumns))) // column count
	if er != nil {
		return nil
	}

	for i, col := range b.bulkColumns {

		if b.cn.sess.loginAck.TDSVersion >= verTDS72 {
			er = binary.Write(buf, binary.LittleEndian, col.UserType) //  usertype, always 0?
			if er != nil {
				return nil
			}
		} else {
			er = binary.Write(buf, binary.LittleEndian, uint16(col.UserType))
			if er != nil {
				return nil
			}

		}
		er = binary.Write(buf, binary.LittleEndian, col.Flags)
		if er != nil {
			return nil
		}

		er = writeTypeInfo(buf, &b.bulkColumns[i].ti)
		if er != nil {
			return nil
		}

		if col.ti.TypeId == typeNText ||
			col.ti.TypeId == typeText ||
			col.ti.TypeId == typeImage {

			tablenameUcs2 := str2ucs2(b.tablename)
			er = binary.Write(buf, binary.LittleEndian, uint16(len(tablenameUcs2)/2))
			if er != nil {
				return nil
			}
			_, er = buf.Write(tablenameUcs2)
			if er != nil {
				return nil
			}
		}
		colnameUcs2 := str2ucs2(col.ColName)
		buf.WriteByte(uint8(len(colnameUcs2) / 2))
		buf.Write(colnameUcs2)
	}

	return buf.Bytes()
}

func (b *BulkInsert) getMetadata() (err error) {
	stmt, err := b.cn.Prepare("SET FMTONLY ON")
	if err != nil {
		return
	}

	_, err = stmt.Exec(nil)
	if err != nil {
		return
	}

	//get columns info
	stmt, err = b.cn.Prepare(fmt.Sprintf("select * from %s SET FMTONLY OFF", b.tablename))
	if err != nil {
		return
	}
	stmt2 := stmt.(*MssqlStmt)
	cols, err := stmt2.queryMeta()
	if err != nil {
		return fmt.Errorf("get columns info failed: %v", err.Error())
	}
	b.metadata = cols

	if b.Debug {
		for _, col := range b.metadata {
			b.dlogf("col: %s typeId: %#x size: %d scale: %d prec: %d flags: %d lcid: %#x\n",
				col.ColName, col.ti.TypeId, col.ti.Size, col.ti.Scale, col.ti.Prec,
				col.Flags, col.ti.Collation.lcidAndFlags)
		}
	}

	return nil
}

// queryMeta is almost the same as MssqlStmt.Query, but returns all the columns info.
func (s *MssqlStmt) queryMeta() (cols []columnStruct, err error) {
	if err = s.sendQuery(nil); err != nil {
		return
	}
	tokchan := make(chan tokenStruct, 5)
	go processResponse(context.Background(), s.c.sess, tokchan)
loop:
	for tok := range tokchan {
		switch token := tok.(type) {
		case doneStruct:
			break loop
		case []columnStruct:
			cols = token
			break loop
		case error:
			return nil, token
		}
	}
	return cols, nil
}

func (b *BulkInsert) makeParam(val DataValue, col columnStruct) (res Param, err error) {
	res.ti.Size = col.ti.Size
	res.ti.TypeId = col.ti.TypeId

	if val == nil {
		res.ti.Size = 0
		return
	}

	switch col.ti.TypeId {

	case typeInt1, typeInt2, typeInt4, typeInt8, typeIntN:
		var intvalue int64

		switch val := val.(type) {
		case int:
			intvalue = int64(val)
		case int32:
			intvalue = int64(val)
		case int64:
			intvalue = val
		default:
			err = fmt.Errorf("mssql: invalid type for int column")
			return
		}

		res.buffer = make([]byte, res.ti.Size)
		if col.ti.Size == 1 {
			res.buffer[0] = byte(intvalue)
		} else if col.ti.Size == 2 {
			binary.LittleEndian.PutUint16(res.buffer, uint16(intvalue))
		} else if col.ti.Size == 4 {
			binary.LittleEndian.PutUint32(res.buffer, uint32(intvalue))
		} else if col.ti.Size == 8 {
			binary.LittleEndian.PutUint64(res.buffer, uint64(intvalue))
		}
	case typeFlt4, typeFlt8, typeFltN:
		var floatvalue float64

		switch val := val.(type) {
		case float32:
			floatvalue = float64(val)
		case float64:
			floatvalue = val
		case int:
			floatvalue = float64(val)
		case int64:
			floatvalue = float64(val)
		default:
			err = fmt.Errorf("mssql: invalid type for float column: %s", val)
			return
		}

		if col.ti.Size == 4 {
			res.buffer = make([]byte, 4)
			binary.LittleEndian.PutUint32(res.buffer, math.Float32bits(float32(floatvalue)))
		} else if col.ti.Size == 8 {
			res.buffer = make([]byte, 8)
			binary.LittleEndian.PutUint64(res.buffer, math.Float64bits(floatvalue))
		}
	case typeNVarChar, typeNText, typeNChar:

		switch val := val.(type) {
		case string:
			res.buffer = str2ucs2(val)
		case []byte:
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for nvarchar column: %s", val)
			return
		}
		res.ti.Size = len(res.buffer)

	case typeVarChar, typeBigVarChar, typeText, typeChar, typeBigChar:
		switch val := val.(type) {
		case string:
			res.buffer = []byte(val)
		case []byte:
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for varchar column: %s", val)
			return
		}
		res.ti.Size = len(res.buffer)

	case typeBit, typeBitN:
		if reflect.TypeOf(val).Kind() != reflect.Bool {
			err = fmt.Errorf("mssql: invalid type for bit column: %s", val)
			return
		}
		res.ti.TypeId = typeBitN
		res.ti.Size = 1
		res.buffer = make([]byte, 1)
		if val.(bool) {
			res.buffer[0] = 1
		}

	case typeDateTime2N, typeDateTimeOffsetN:
		switch val := val.(type) {
		case time.Time:
			days, ns := dateTime2(val)
			ns /= int64(math.Pow10(int(col.ti.Scale)*-1) * 1000000000)

			var data = make([]byte, 5)

			data[0] = byte(ns)
			data[1] = byte(ns >> 8)
			data[2] = byte(ns >> 16)
			data[3] = byte(ns >> 24)
			data[4] = byte(ns >> 32)

			if col.ti.Scale <= 2 {
				res.ti.Size = 6
			} else if col.ti.Scale <= 4 {
				res.ti.Size = 7
			} else {
				res.ti.Size = 8
			}
			var buf []byte
			buf = make([]byte, res.ti.Size)
			copy(buf, data[0:res.ti.Size-3])

			buf[res.ti.Size-3] = byte(days)
			buf[res.ti.Size-2] = byte(days >> 8)
			buf[res.ti.Size-1] = byte(days >> 16)

			if col.ti.TypeId == typeDateTimeOffsetN {
				_, offset := val.Zone()
				var offsetMinute = uint16(offset / 60)
				buf = append(buf, byte(offsetMinute))
				buf = append(buf, byte(offsetMinute>>8))
				res.ti.Size = res.ti.Size + 2
			}

			res.buffer = buf

		default:
			err = fmt.Errorf("mssql: invalid type for datetime2 column: %s", val)
			return
		}
	case typeDateN:
		switch val := val.(type) {
		case time.Time:
			days, _ := dateTime2(val)

			res.ti.Size = 3
			res.buffer = make([]byte, 3)
			res.buffer[0] = byte(days)
			res.buffer[1] = byte(days >> 8)
			res.buffer[2] = byte(days >> 16)
		default:
			err = fmt.Errorf("mssql: invalid type for date column: %s", val)
			return
		}
	case typeDateTime, typeDateTimeN, typeDateTim4:
		switch val := val.(type) {
		case time.Time:
			if col.ti.Size == 4 {
				res.ti.Size = 4
				res.buffer = make([]byte, 4)

				ref := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
				dur := val.Sub(ref)
				days := dur / (24 * time.Hour)
				if days < 0 {
					err = fmt.Errorf("mssql: Date %s is out of range", val)
					return
				}
				mins := val.Hour()*60 + val.Minute()

				binary.LittleEndian.PutUint16(res.buffer[0:2], uint16(days))
				binary.LittleEndian.PutUint16(res.buffer[2:4], uint16(mins))
			} else if col.ti.Size == 8 {
				res.ti.Size = 8
				res.buffer = make([]byte, 8)

				days := divFloor(val.Unix(), 24*60*60)
				//25567 - number of days since Jan 1 1900 UTC to Jan 1 1970
				days = days + 25567
				tm := (val.Hour()*60*60+val.Minute()*60+val.Second())*300 + (val.Nanosecond() / 10000000 * 3)

				binary.LittleEndian.PutUint32(res.buffer[0:4], uint32(days))
				binary.LittleEndian.PutUint32(res.buffer[4:8], uint32(tm))
			} else {
				err = fmt.Errorf("mssql: invalid size of column")
			}

		default:
			err = fmt.Errorf("mssql: invalid type for datetime column: %s", val)
		}

	// BUG: Casting everything to float can cause a loss of precision
	// and cause unexpected results to be inserted into your table
	case typeDecimal, typeDecimalN, typeNumeric, typeNumericN:
		var value float64
		switch v := val.(type) {
		case int:
			value = float64(v)
		case int8:
			value = float64(v)
		case int16:
			value = float64(v)
		case int32:
			value = float64(v)
		case int64:
			value = float64(v)
		case float32:
			value = float64(v)
		case float64:
			value = v
		case string:
			if value, err = strconv.ParseFloat(v, 64); err != nil {
				return res, fmt.Errorf("bulk: unable to convert string to float: %v", err)
			}
		default:
			return res, fmt.Errorf("unknown value for decimal: %#v", v)
		}

		perc := col.ti.Prec
		scale := col.ti.Scale
		var dec Decimal
		dec, err = Float64ToDecimalScale(value, scale)
		if err != nil {
			return res, err
		}
		dec.prec = perc

		var length byte
		switch {
		case perc <= 9:
			length = 4
		case perc <= 19:
			length = 8
		case perc <= 28:
			length = 12
		default:
			length = 16
		}

		buf := make([]byte, length+1)
		// first byte length written by typeInfo.writer
		res.ti.Size = int(length) + 1
		// second byte sign
		if value < 0 {
			buf[0] = 0
		} else {
			buf[0] = 1
		}

		ub := dec.UnscaledBytes()
		l := len(ub)
		if l > int(length) {
			err = fmt.Errorf("decimal out of range: %s", dec)
			return res, err
		}
		// reverse the bytes
		for i, j := 1, l-1; j >= 0; i, j = i+1, j-1 {
			buf[i] = ub[j]
		}
		res.buffer = buf

	case typeMoney, typeMoney4, typeMoneyN:
		var value float64
		// switch v := val.(type) {
		// case int:
		// 	value = float64(v)
		// case int8:
		// 	value = float64(v)
		// case int16:
		// 	value = float64(v)
		// case int32:
		// 	value = float64(v)
		// case int64:
		// 	value = float64(v)
		// case float32:
		// 	value = float64(v)
		// case float64:
		// 	value = v
		// case string:
		// 	value, err = strconv.ParseFloat(v, 64)
		// 	if err != nil {
		// 		err = fmt.Errorf("mssql: invalid string for money column: %s", v)
		// 		return
		// 	}
		// default:
		// 	err = fmt.Errorf("mssql: invalid type for money column: %s", v)
		// 	return
		// }

		if col.ti.Size == 4 {
			res.ti.Size = 4
			res.buffer = make([]byte, 4)
			var i32 uint32
			switch v := val.(type) {
			case string:
				if strings.Contains(v, ".") {
					pointLoc := strings.Index(v, ".")
					intStr := strings.Split(v, ".")
					i64, _ := strconv.ParseInt((intStr[0] + intStr[1]), 10, 64)
					var i, e = big.NewInt(10), big.NewInt(int64((pointLoc)))
					i.Exp(i, e, nil)
					i64 = i64 * i.Int64()
					i32 = uint32(i64 * 10 * int64(pointLoc))
				}
			}
			binary.LittleEndian.PutUint32(res.buffer, i32)
		} else if col.ti.Size == 8 {
			res.ti.Size = 8
			res.buffer = make([]byte, 8)
			var high uint32
			var low uint32
			switch v := val.(type) {
			case int:
				value = float64(v)
				intValue := int64((value + 0.000000000000001) * 10000)
				high = uint32(intValue >> 32)
				low = uint32(intValue - int64(high))
			case int8:
				value = float64(v)
				intValue := int64((value + 0.000000000000001) * 10000)
				high = uint32(intValue >> 32)
				low = uint32(intValue - int64(high))
			case int16:
				value = float64(v)
				intValue := int64((value + 0.000000000000001) * 10000)
				high = uint32(intValue >> 32)
				low = uint32(intValue - int64(high))
			case int32:
				value = float64(v)
				intValue := int64((value + 0.000000000000001) * 10000)
				high = uint32(intValue >> 32)
				low = uint32(intValue - int64(high))
			case int64:
				value = float64(v)
				intValue := int64((value + 0.000000000000001) * 10000)
				high = uint32(intValue >> 32)
				low = uint32(intValue - int64(high))

			case string:
				var intValue int64
				if strings.Contains(v, ".") {
					intStr := strings.Split(v, ".")

					bnoden := intStr[1]
					if len(intStr[1]) > 4 {
						intStr[1] = bnoden[0:4]
					}
					if len(intStr[1]) == 1 {
						intStr[1] = intStr[1] + "0"
					}
					if len(intStr[1]) == 3 {
						intStr[1] = intStr[1] + "0"
					}

					i64, _ := strconv.ParseInt((intStr[0] + intStr[1]), 10, 64)

					if len(intStr[1]) == 2 {
						intValue = i64 * 100
					}
					if len(intStr[1]) == 4 {
						intValue = i64
					}

					high = uint32(intValue >> 32)
					low = uint32(intValue - int64(high))
				} else {
					value, _ = strconv.ParseFloat(v, 64)
					intValue := int64((value + 0.000000000000001) * 10000)
					high = uint32(intValue >> 32)
					low = uint32(intValue - int64(high))
				}
			}

			binary.LittleEndian.PutUint32(res.buffer[0:4], high)
			binary.LittleEndian.PutUint32(res.buffer[4:8], low)
		} else {
			err = fmt.Errorf("mssql: invalid size of money column")
		}

	case typeBigVarBin:
		switch val := val.(type) {
		case []byte:
			res.ti.Size = len(val)
			res.buffer = val
		default:
			err = fmt.Errorf("mssql: invalid type for Binary column: %s", val)
			return
		}

	default:
		err = fmt.Errorf("mssql: type %x not implemented", col.ti.TypeId)
	}
	return

}

func (b *BulkInsert) dlogf(format string, v ...interface{}) {
	if b.Debug {
		b.cn.sess.log.Printf(format, v...)
	}
}
