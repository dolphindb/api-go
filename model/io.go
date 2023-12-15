package model

import (
	"fmt"
	"math"
	"math/big"

	"github.com/dolphindb/api-go/dialer/protocol"
)

func (d *dataType) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	var err error

	switch d.t {
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
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
	case DtDecimal32:
		err = writeInt2(w, bo, d.data.([2]int32))
	case DtDecimal64:
		err = writeDecimal64(w, bo, d.data.([2]int64))
	case DtDecimal128:
		err = writeDecimal128(w, bo, d.data.(decimal128Data))
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

func writeInt2(w *protocol.Writer, bo protocol.ByteOrder, data [2]int32) error {
	buf := make([]byte, protocol.Uint64Size)
	bo.PutUint32(buf, uint32(data[0]))
	bo.PutUint32(buf[4:], uint32(data[1]))
	return w.Write(buf)
}

func writeDecimal64(w *protocol.Writer, bo protocol.ByteOrder, data [2]int64) error {
	buf := make([]byte, 12)
	bo.PutUint32(buf, uint32(data[0]))
	bo.PutUint64(buf[4:], uint64(data[1]))
	return w.Write(buf)
}

func writeDecimal128(w *protocol.Writer, bo protocol.ByteOrder, data decimal128Data) error {
	buf := make([]byte, 4)
	bo.PutUint32(buf, uint32(data.scale))
	if err := w.Write(buf); err != nil {
		return err
	}

	newBytes := make([]byte, 16)
	err := fullBigIntBytes(newBytes, data.value, 0)
	if err != nil {
		return err
	}

	if bo == protocol.LittleEndian {
		reverseByteArray(newBytes)
	}

	return w.Write(newBytes)
}

func reverseByteArray(array []byte) {
	left := 0
	right := len(array) - 1
	for left < right {
		tmp := array[left]
		array[left] = array[right]
		array[right] = tmp

		left++
		right--
	}
}

func writeDecimal64s(w *protocol.Writer, bo protocol.ByteOrder, data []int64) error {
	buf := make([]byte, 4)
	bo.PutUint32(buf, uint32(data[0]))
	if err := w.Write(buf); err != nil {
		return err
	}

	return w.Write(protocol.ByteSliceFromInt64Slice(data[1:]))
}

func writeDecimal128s(w *protocol.Writer, bo protocol.ByteOrder, data decimal128Datas) error {
	buf := make([]byte, 4)
	bo.PutUint32(buf, uint32(data.scale))
	if err := w.Write(buf); err != nil {
		return err
	}

	newBytes := make([]byte, len(data.value)*16)
	for k, v := range data.value {
		err := fullBigIntBytes(newBytes, v, 16*k)
		if err != nil {
			return err
		}
	}

	if bo == protocol.LittleEndian {
		reverseByteArrayEvery8Byte(newBytes)
	}

	return w.Write(newBytes)
}

func fullBigIntBytes(dst []byte, src *big.Int, startInd int) error {
	oa := src.Bytes()
	loa := len(oa)
	if loa > 16 {
		return fmt.Errorf("byte length of Decimal128 %d exceed 16", loa)
	}

	if src.Sign() != -1 {
		copy(dst[16-loa+startInd:], oa)
	} else {
		copy(dst[16-loa+startInd:], oa)
		for j := 0; j < 16; j++ {
			dst[j+startInd] = ^dst[j+startInd]
		}

		carryOutBit(dst[startInd : startInd+16])
	}

	return nil
}

func carryOutBit(buf []byte) {
	for i := 15; i >= 0; i-- {
		if buf[i] != 255 {
			buf[i] += 1
			break
		}

		buf[i] = 0
	}
}

func backOutBit(buf []byte) {
	for i := 15; i >= 0; i-- {
		if buf[i] != 0 {
			buf[i] -= 1
			break
		}

		buf[i] = 255
	}
}

func reverseByteArrayEvery8Byte(array []byte) {
	st := 0
	end := st + 15
	for end < len(array) {
		for i := 0; i < 8; i++ {
			tmp := array[st+i]
			array[st+i] = array[end-i]
			array[end-i] = tmp
		}

		st += 16
		end += 16
	}
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

func writeDouble2(w *protocol.Writer, bo protocol.ByteOrder, du [2]float64) error {
	buf := make([]byte, protocol.TwoUint64Size)
	bo.PutUint64(buf, math.Float64bits(du[0]))
	bo.PutUint64(buf[8:], math.Float64bits(du[1]))
	return w.Write(buf)
}

func writeLong2(w *protocol.Writer, bo protocol.ByteOrder, du [2]uint64) error {
	buf := make([]byte, protocol.TwoUint64Size)
	bo.PutUint64(buf, du[0])
	bo.PutUint64(buf[8:], du[1])
	return w.Write(buf)
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

func writeBlobs(w *protocol.Writer, bo protocol.ByteOrder, blobData [][]byte) error {
	buf := make([]byte, 4)
	for _, v := range blobData {
		bo.PutUint32(buf, uint32(len(v)))
		err := w.Write(buf)
		if err != nil {
			return err
		}
		err = w.Write(v)
		if err != nil {
			return err
		}
	}

	return nil
}

// ParseDataType parses raw data to DataType
func ParseDataType(r protocol.Reader, t DataTypeByte, bo protocol.ByteOrder) (DataType, error) {
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
	case DtDecimal32:
		dt.data, err = readInt2(r, bo)
	case DtDecimal64:
		dt.data, err = readDecimal64(r, bo)
	case DtDecimal128:
		dt.data, err = readDecimal128(r, bo)
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

func readInt2(r protocol.Reader, bo protocol.ByteOrder) ([2]int32, error) {
	buf, err := r.ReadCertainBytes(8)
	if err != nil {
		return [2]int32{}, err
	}

	return [2]int32{int32(bo.Uint32(buf)), int32(bo.Uint32(buf[4:]))}, nil
}

func readDecimal64(r protocol.Reader, bo protocol.ByteOrder) ([2]int64, error) {
	buf, err := r.ReadCertainBytes(12)
	if err != nil {
		return [2]int64{}, err
	}

	return [2]int64{int64(bo.Uint32(buf)), int64(bo.Uint64(buf[4:]))}, nil
}

func readDecimal128(r protocol.Reader, bo protocol.ByteOrder) (decimal128Data, error) {
	buf, err := r.ReadCertainBytes(20)
	if err != nil {
		return decimal128Data{}, err
	}

	newBytes := buf[4:]
	if bo == protocol.LittleEndian {
		reverseByteArray(newBytes)
	}

	val := big.NewInt(0)
	if newBytes[0] > 127 {
		backOutBit(newBytes)
		orNegative(newBytes)
		val = val.Neg(big.NewInt(0).SetBytes(newBytes))
	} else {
		val = val.SetBytes(newBytes)
	}

	return decimal128Data{scale: int32(bo.Uint32(buf)), value: val}, nil
}

func orNegative(buf []byte) {
	for k, v := range buf {
		buf[k] = ^v
	}
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

func readBigIntWithBigEndian(count int, r protocol.Reader) ([]*big.Int, error) {
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return newBigIntFromBytes(count, buf), nil
}

func readBigIntWithLittleEndian(count int, r protocol.Reader) ([]*big.Int, error) {
	buf, err := r.ReadCertainBytes(16 * count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	reverseByteArrayEvery8Byte(buf)

	return newBigIntFromBytes(count, buf), nil
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

func readInt2sWithLittleEndian(count int, r protocol.Reader) ([]int32, error) {
	buf, err := r.ReadCertainBytes(4 + 4*count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	return protocol.Int32SliceFromByteSlice(buf), nil
}

func readInt2sWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]int32, error) {
	res := make([]int32, 0, count)
	buf, err := r.ReadCertainBytes(4 + 4*count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 0
	for i := 0; i < count+1; i++ {
		res = append(res, int32(bo.Uint32(buf[ind:])))
		ind += 4
	}

	return res, nil
}

func readDecimal64sWithLittleEndian(count int, r protocol.Reader) ([]int64, error) {
	res := make([]int64, 0, count)
	buf, err := r.ReadCertainBytes(4 + 8*count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 4
	res = append(res, int64(protocol.LittleEndian.Uint32(buf)))
	for i := 0; i < count; i++ {
		res = append(res, int64(protocol.LittleEndian.Uint64(buf[ind:])))
		ind += 8
	}

	return res, nil
}

func readDecimal128sWithLittleEndian(count int, r protocol.Reader) (decimal128Datas, error) {
	res := decimal128Datas{value: make([]*big.Int, 0, count)}
	buf, err := r.ReadCertainBytes(4 + 16*count)
	if err != nil || len(buf) == 0 {
		return decimal128Datas{}, err
	}

	res.scale = int32(protocol.LittleEndian.Uint32(buf))

	reverseByteArrayEvery8Byte(buf[4:])
	res.value = newBigIntFromBytes(count, buf[4:])

	return res, nil
}

func newBigIntFromBytes(count int, byts []byte) []*big.Int {
	res := make([]*big.Int, 0, count)
	for i := 0; i < count; i++ {
		ind := i * 16
		val := big.NewInt(0)
		if byts[ind] > 127 {
			backOutBit(byts[ind : ind+16])
			orNegative(byts[ind : ind+16])
			val = val.Neg(big.NewInt(0).SetBytes(byts[ind : ind+16]))
		} else {
			val = val.SetBytes(byts[ind : ind+16])
		}

		res = append(res, val)
	}

	return res
}

func readDecimal64sWithBigEndian(count int, r protocol.Reader, bo protocol.ByteOrder) ([]int64, error) {
	res := make([]int64, 0, count)
	buf, err := r.ReadCertainBytes(4 + 8*count)
	if err != nil || len(buf) == 0 {
		return nil, err
	}

	ind := 4
	res = append(res, int64(bo.Uint32(buf)))
	for i := 0; i < count; i++ {
		res = append(res, int64(bo.Uint64(buf[ind:])))
		ind += 8
	}

	return res, nil
}

func readDecimal128sWithBigEndian(count int, r protocol.Reader) (decimal128Datas, error) {
	res := decimal128Datas{value: make([]*big.Int, 0, count)}
	buf, err := r.ReadCertainBytes(4 + 16*count)
	if err != nil || len(buf) == 0 {
		return decimal128Datas{}, err
	}

	res.scale = int32(protocol.BigEndian.Uint32(buf))
	res.value = newBigIntFromBytes(count, buf[4:])

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
	res := make([][]byte, count)
	for i := 0; i < count; i++ {
		byt, err := r.ReadCertainBytes(4)
		if err != nil {
			return nil, err
		}

		length := int(bo.Uint32(byt))
		if length == 0 {
			continue
		}

		res[i], err = r.ReadCertainBytes(length)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
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
	case DtVoid:
	case DtBool, DtChar:
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
	case DtDecimal32:
		d.decimal32Data, err = readInt2sWithLittleEndian(count, r)
	case DtDecimal64:
		d.decimal64Data, err = readDecimal64sWithLittleEndian(count, r)
	case DtDecimal128:
		d.decimal128Data, err = readDecimal128sWithLittleEndian(count, r)
	case DtAny:
		d.anyData, err = readAny(count, r, d.bo)
	}

	return err
}

func (d *dataTypeList) bigEndianRead(count int, r protocol.Reader) error {
	var err error
	switch d.t {
	case DtVoid:
	case DtBool, DtChar:
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
	case DtDecimal32:
		d.decimal32Data, err = readInt2sWithBigEndian(count, r, d.bo)
	case DtDecimal64:
		d.decimal64Data, err = readDecimal64sWithBigEndian(count, r, d.bo)
	case DtDecimal128:
		d.decimal128Data, err = readDecimal128sWithBigEndian(count, r)
	case DtAny:
		d.anyData, err = readAny(count, r, d.bo)
	}

	return err
}
