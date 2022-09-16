package model

import (
	"math"

	"github.com/dolphindb/api-go/dialer/protocol"
)

func (d *dataType) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	var err error

	switch d.t {
	case DtString, DtCode, DtFunction, DtHandle, DtDictionary, DtSymbol:
		err = writeString(w, d.data.(string))
	case DtBlob:
		err = writeBlob(w, bo, d.data.([]byte))
	case DtAny:
		err = d.data.(DataForm).Render(w, bo)
	case DtBool, DtChar, DtCompress:
		err = w.WriteByte(d.data.(byte))
	case DtInt, DtTime, DtDate, DtMonth, DtMinute, DtSecond, DtDatetime, DtDateHour:
		err = writeInt(w, bo, d.data.(int32))
	case DtShort:
		err = writeShort(w, bo, d.data.(int16))
	case DtVoid:
		err = w.WriteByte(0)
	case DtDouble:
		err = writeDouble(w, bo, d.data.(float64))
	case DtFloat:
		err = writeFloat(w, bo, d.data.(float32))
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		err = writeLong(w, bo, d.data.(int64))
	case DtDuration:
		err = writeDuration(w, bo, d.data.([2]uint32))
	case DtPoint, DtComplex:
		err = writeDouble2(w, bo, d.data.([2]float64))
	case DtInt128, DtUUID, DtIP:
		err = writeLong2(w, bo, d.data.([2]uint64))
	}

	return err
}

func writeInt(w *protocol.Writer, bo protocol.ByteOrder, data int32) error {
	buf := make([]byte, protocol.Uint32Size)
	bo.PutUint32(buf, uint32(data))
	return w.Write(buf)
}

func writeShort(w *protocol.Writer, bo protocol.ByteOrder, data int16) error {
	buf := make([]byte, protocol.Uint16Size)
	bo.PutUint16(buf, uint16(data))
	return w.Write(buf)
}

func writeLong(w *protocol.Writer, bo protocol.ByteOrder, data int64) error {
	buf := make([]byte, protocol.Uint64Size)
	bo.PutUint64(buf, uint64(data))
	return w.Write(buf)
}

func writeVoids(w *protocol.Writer, count int) error {
	buf := make([]byte, count)
	for i := 0; i < count; i++ {
		buf[i] = byte(0)
	}
	return w.Write(buf)
}

func writeFloat(w *protocol.Writer, bo protocol.ByteOrder, data float32) error {
	buf := make([]byte, protocol.Uint32Size)
	bo.PutUint32(buf, math.Float32bits(data))
	return w.Write(buf)
}

func writeDouble(w *protocol.Writer, bo protocol.ByteOrder, data float64) error {
	buf := make([]byte, protocol.Uint64Size)
	bo.PutUint64(buf, math.Float64bits(data))
	return w.Write(buf)
}

func writeDuration(w *protocol.Writer, bo protocol.ByteOrder, du [2]uint32) error {
	buf := make([]byte, protocol.Uint64Size)
	bo.PutUint32(buf, du[0])
	bo.PutUint32(buf[4:], du[1])
	return w.Write(buf)
}

func writeDurations(w *protocol.Writer, du []uint32) error {
	return w.Write(protocol.ByteSliceFromUint32Slice(du))
}

func writeDouble2(w *protocol.Writer, bo protocol.ByteOrder, du [2]float64) error {
	buf := make([]byte, protocol.TwoUint64Size)
	bo.PutUint64(buf, math.Float64bits(du[0]))
	bo.PutUint64(buf[8:], math.Float64bits(du[1]))
	return w.Write(buf)
}

func writeDouble2s(w *protocol.Writer, du []float64) error {
	return w.Write(protocol.ByteSliceFromFloat64Slice(du))
}

func writeLong2(w *protocol.Writer, bo protocol.ByteOrder, du [2]uint64) error {
	buf := make([]byte, protocol.TwoUint64Size)
	bo.PutUint64(buf, du[0])
	bo.PutUint64(buf[8:], du[1])
	return w.Write(buf)
}

func writeLong2s(w *protocol.Writer, du []uint64) error {
	return w.Write(protocol.ByteSliceFromUint64Slice(du))
}

func writeString(w *protocol.Writer, str string) error {
	if err := w.WriteString(str); err != nil {
		return err
	}

	return w.WriteByte(protocol.StringSep)
}

func writeStrings(w *protocol.Writer, str []string) error {
	for _, v := range str {
		err := writeString(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeBlob(w *protocol.Writer, bo protocol.ByteOrder, byt []byte) error {
	length := len(byt)

	buf := make([]byte, 4)
	bo.PutUint32(buf, uint32(length))

	if err := w.Write(buf); err != nil {
		return err
	}

	return w.Write(byt)
}

func writeBlobs(w *protocol.Writer, blobData [][]byte) error {
	ind := 0
	buf := make([]byte, 4)
	for _, v := range blobData {
		protocol.LittleEndian.PutUint32(buf, uint32(len(v)))
		err := w.Write(buf)
		if err != nil {
			return err
		}
		err = w.Write(v)
		if err != nil {
			return err
		}

		ind += 4
	}

	return nil
}

func readDataType(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder) (DataType, error) {
	return read(r, t, bo)
}

func read(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder) (*dataType, error) {
	var err error
	dt := &dataType{
		t:  t,
		bo: bo,
	}

	switch t {
	case DtVoid, DtBool, DtChar:
		dt.data, err = r.ReadByte()
	case DtShort:
		dt.data, err = readShort(r, bo)
	case DtFloat:
		dt.data, err = readFloat(r, bo)
	case DtDouble:
		dt.data, err = readDouble(r, bo)
	case DtDuration:
		dt.data, err = readDuration(r, bo)
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		dt.data, err = readInt(r, bo)
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		dt.data, err = readLong(r, bo)
	case DtInt128, DtIP, DtUUID:
		dt.data, err = readLong2(r, bo)
	case DtComplex, DtPoint:
		dt.data, err = readDouble2(r, bo)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		dt.data, err = readString(r)
	case DtBlob:
		dt.data, err = readBlob(r, bo)
	case DtAny:
		dt.data, err = ParseDataForm(r, bo)
	}

	return dt, err
}

func readShort(r protocol.Reader, bo protocol.ByteOrder) (int16, error) {
	buf, err := r.ReadCertainBytes(2)
	if err != nil {
		return 0, err
	}

	return int16(bo.Uint16(buf)), nil
}

func readShortsWithLittleEndian(count int, r protocol.Reader) ([]int16, error) {
	buf, err := r.ReadCertainBytes(2 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Int16SliceFromByteSlice(buf), nil
}

func readShortsWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]int16, error) {
	buf, err := r.ReadCertainBytes(2 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	res := make([]int16, count)
	ind := 0
	for i := 0; i < count; i++ {
		res[i] = int16(bo.Uint16(buf[ind : ind+2]))
		ind += 2
	}

	return res, nil
}

func readInt(r protocol.Reader, bo protocol.ByteOrder) (int32, error) {
	buf, err := r.ReadCertainBytes(4)
	if err != nil {
		return 0, err
	}

	return int32(bo.Uint32(buf)), nil
}

func readIntWithLittleEndian(count int, r protocol.Reader) ([]int32, error) {
	buf, err := r.ReadCertainBytes(4 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Int32SliceFromByteSlice(buf), nil
}

func readIntWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]int32, error) {
	buf, err := r.ReadCertainBytes(4 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	res := make([]int32, count)
	ind := 0
	for i := 0; i < count; i++ {
		res[i] = int32(bo.Uint32(buf[ind : ind+4]))
		ind += 4
	}

	return res, nil
}

func readLong(r protocol.Reader, bo protocol.ByteOrder) (int64, error) {
	buf, err := r.ReadCertainBytes(8)
	if err != nil {
		return 0, err
	}

	return int64(bo.Uint64(buf)), nil
}

func readLongsWithLittleEndian(count int, r protocol.Reader) ([]int64, error) {
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Int64SliceFromByteSlice(buf), nil
}

func readLongsWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]int64, error) {
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	res := make([]int64, count)
	ind := 0
	for i := 0; i < count; i++ {
		res[i] = int64(bo.Uint64(buf[ind : ind+8]))
		ind += 8
	}

	return res, nil
}

func readFloat(r protocol.Reader, bo protocol.ByteOrder) (float32, error) {
	buf, err := r.ReadCertainBytes(4)
	if err != nil {
		return 0, err
	}

	return math.Float32frombits(bo.Uint32(buf)), nil
}

func readFloatsWithLittleEndian(count int, r protocol.Reader) ([]float32, error) {
	buf, err := r.ReadCertainBytes(4 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Float32SliceFromByteSlice(buf), nil
}

func readFloatsWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]float32, error) {
	res := make([]float32, count)
	buf, err := r.ReadCertainBytes(4 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count; i++ {
		res[i] = math.Float32frombits(bo.Uint32(buf[ind : ind+4]))
		ind += 4
	}

	return res, nil
}

func readDouble(r protocol.Reader, bo protocol.ByteOrder) (float64, error) {
	buf, err := r.ReadCertainBytes(8)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(bo.Uint64(buf)), nil
}

func readDoublesWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]float64, error) {
	res := make([]float64, count)
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count; i++ {
		res[i] = math.Float64frombits(bo.Uint64(buf[ind : ind+8]))
		ind += 8
	}

	return res, nil
}

func readDoublesWithLittleEndian(count int, r protocol.Reader) ([]float64, error) {
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Float64SliceFromByteSlice(buf), nil
}

func readDuration(r protocol.Reader, bo protocol.ByteOrder) ([2]uint32, error) {
	buf, err := r.ReadCertainBytes(8)
	if err != nil {
		return [2]uint32{}, err
	}

	return [2]uint32{
		bo.Uint32(buf),
		bo.Uint32(buf[4:]),
	}, nil
}

func readDurationsWithLittleEndian(count int, r protocol.Reader) ([]uint32, error) {
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Uint32SliceFromByteSlice(buf), nil
}

func readDurationsWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]uint32, error) {
	res := make([]uint32, 0, count)
	buf, err := r.ReadCertainBytes(8 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count; i++ {
		res = append(res, bo.Uint32(buf[ind:ind+4]), bo.Uint32(buf[ind+4:ind+8]))
		ind += 8
	}

	return res, nil
}

func readDouble2(r protocol.Reader, bo protocol.ByteOrder) ([2]float64, error) {
	buf, err := r.ReadCertainBytes(protocol.TwoUint64Size)
	if err != nil {
		return [2]float64{}, err
	}

	return [2]float64{
		math.Float64frombits(bo.Uint64(buf)),
		math.Float64frombits(bo.Uint64(buf[8:])),
	}, nil
}

func readDouble2sWithLittleEndian(count int, r protocol.Reader) ([]float64, error) {
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Float64SliceFromByteSlice(buf), nil
}

func readDouble2sWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]float64, error) {
	res := make([]float64, 0, count)
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count; i++ {
		res = append(res, math.Float64frombits(bo.Uint64(buf[ind:ind+8])), math.Float64frombits(bo.Uint64(buf[ind+8:ind+16])))
		ind += 16
	}

	return res, nil
}

func readLong2(r protocol.Reader, bo protocol.ByteOrder) ([2]uint64, error) {
	buf, err := r.ReadCertainBytes(16)
	if err != nil {
		return [2]uint64{}, err
	}

	return [2]uint64{
		bo.Uint64(buf),
		bo.Uint64(buf[8:]),
	}, nil
}

func readLong2sWithLittleEndian(count int, r protocol.Reader) ([]uint64, error) {
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Uint64SliceFromByteSlice(buf), nil
}

func readLong2sWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]uint64, error) {
	res := make([]uint64, 0, count)
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count; i++ {
		res = append(res, bo.Uint64(buf[ind:ind+8]), bo.Uint64(buf[ind+8:ind+16]))
		ind += 16
	}

	return res, nil
}

func readString(r protocol.Reader) (string, error) {
	byt, err := r.ReadBytes(protocol.StringSep)
	if err != nil || len(byt) == 0 {
		return "", err
	}

	return protocol.StringFromByteSlice(byt), nil
}

func readStrings(count int, r protocol.Reader) ([]string, error) {
	res := make([]string, count)
	for i := 0; i < count; i++ {
		byt, err := r.ReadBytes(protocol.StringSep)
		if err != nil {
			return nil, err
		}

		if len(byt) == 0 {
			res[i] = ""
		} else {
			res[i] = protocol.StringFromByteSlice(byt)
		}
	}

	return res, nil
}

func readBlob(r protocol.Reader, bo protocol.ByteOrder) ([]byte, error) {
	bs, err := r.ReadCertainBytes(4)
	if err != nil {
		return nil, err
	}

	length := bo.Uint32(bs)
	if length == 0 {
		return nil, nil
	}

	return r.ReadCertainBytes(int(length))
}

func readBlobs(count int, r protocol.Reader, bo protocol.ByteOrder) ([][]byte, error) {
	buf := protocol.NewBuffer(count, r)
	return buf.ReadBlobs(bo)
}

func readAny(count int, r protocol.Reader, bo protocol.ByteOrder) ([]DataForm, error) {
	var err error
	res := make([]DataForm, count)
	for i := 0; i < count; i++ {
		res[i], err = ParseDataForm(r, bo)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func readList(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder, count int) (DataTypeList, error) {
	dt := &dataTypeList{
		t:     t,
		count: count,
		bo:    bo,
	}

	if bo == protocol.LittleEndian {
		err := dt.littleEndianRead(count, r)
		return dt, err
	}

	err := dt.bigEndianRead(count, r)
	return dt, err
}

func (d *dataTypeList) littleEndianRead(count int, r protocol.Reader) error {
	var err error
	switch d.t {
	case DtVoid, DtBool, DtChar:
		d.charData, err = r.ReadCertainBytes(count)
	case DtShort:
		d.shortData, err = readShortsWithLittleEndian(count, r)
	case DtFloat:
		d.floatData, err = readFloatsWithLittleEndian(count, r)
	case DtDouble:
		d.doubleData, err = readDoublesWithLittleEndian(count, r)
	case DtDuration:
		d.durationData, err = readDurationsWithLittleEndian(count, r)
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		d.intData, err = readIntWithLittleEndian(count, r)
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		d.longData, err = readLongsWithLittleEndian(count, r)
	case DtInt128, DtIP, DtUUID:
		d.long2Data, err = readLong2sWithLittleEndian(count, r)
	case DtComplex, DtPoint:
		d.double2Data, err = readDouble2sWithLittleEndian(count, r)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData, err = readStrings(count, r)
	case DtBlob:
		d.blobData, err = readBlobs(count, r, d.bo)
	case DtAny:
		d.anyData, err = readAny(count, r, d.bo)
	}

	return err
}

func (d *dataTypeList) bigEndianRead(count int, r protocol.Reader) error {
	var err error
	switch d.t {
	case DtVoid, DtBool, DtChar:
		d.charData, err = r.ReadCertainBytes(count)
	case DtShort:
		d.shortData, err = readShortsWithBigEndian(count, r, d.bo)
	case DtFloat:
		d.floatData, err = readFloatsWithBigEndian(count, r, d.bo)
	case DtDouble:
		d.doubleData, err = readDoublesWithBigEndian(count, r, d.bo)
	case DtDuration:
		d.durationData, err = readDurationsWithBigEndian(count, r, d.bo)
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		d.intData, err = readIntWithBigEndian(count, r, d.bo)
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		d.longData, err = readLongsWithBigEndian(count, r, d.bo)
	case DtInt128, DtIP, DtUUID:
		d.long2Data, err = readLong2sWithBigEndian(count, r, d.bo)
	case DtComplex, DtPoint:
		d.double2Data, err = readDouble2sWithBigEndian(count, r, d.bo)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData, err = readStrings(count, r)
	case DtBlob:
		d.blobData, err = readBlobs(count, r, d.bo)
	case DtAny:
		d.anyData, err = readAny(count, r, d.bo)
	}

	return err
}
