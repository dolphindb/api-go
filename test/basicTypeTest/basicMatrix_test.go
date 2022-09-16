package test

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Matrix_DownLoad_int(t *testing.T) {
	Convey("Test_matrix_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_int_not_null:", func() {
			s, err := db.RunScript("[1, -2, 93, 1024, -2025, 1048576]$3:2")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int32{{1, 1024}, {-2, -2025}, {93, 1048576}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 3)
		})
		Convey("Test_matrix_int_null:", func() {
			s, err := db.RunScript("matrix(INT, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_short(t *testing.T) {
	Convey("Test_matrix_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_short_not_null:", func() {
			s, err := db.RunScript("[1h, -2h, 93h, 1024h, -2025h, 32766h]$3:2")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int16{{1, 1024}, {-2, -2025}, {93, 32766}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		Convey("Test_matrix_short_null:", func() {
			s, err := db.RunScript("matrix(SHORT, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int16{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_long(t *testing.T) {
	Convey("Test_matrix_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_long_not_null:", func() {
			s, err := db.RunScript("[1l, 12l, -15l,1024l, 1048576l, 24l]$3:2")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int64{{1, 1024}, {12, 1048576}, {-15, 24}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			by := bytes.NewBufferString("")
			w := protocol.NewWriter(by)
			err = result.Render(w, protocol.LittleEndian)
			So(err, ShouldBeNil)
			w.Flush()
			by.Reset()
		})
		Convey("Test_matrix_long_null:", func() {
			s, err := db.RunScript("matrix(LONG, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]int64{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_double(t *testing.T) {
	Convey("Test_matrix_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_double_not_null:", func() {
			s, err := db.RunScript("[1.1, -1.2, 1300.0, 1024.0, 1.5, 1048576.0]$3:2")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]float64{{1.1, 1024}, {-1.2, 1.5}, {1300, 1048576}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_matrix_double_null:", func() {
			s, err := db.RunScript("matrix(DOUBLE, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]float64{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_float(t *testing.T) {
	Convey("Test_matrix_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_float_not_null:", func() {
			s, err := db.RunScript("[1.1f, -1.2f, 1024.3f, -2025.4f, 1048576.5f, 5201314.6f]$3:2")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]float32{{1.1, -2025.4}, {-1.2, 1048576.5}, {1024.3, 5201314.6}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_matrix_float_null:", func() {
			s, err := db.RunScript("matrix(FLOAT, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][2]float32{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_symbol(t *testing.T) {
	Convey("Test_matrix_symbol:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_symbol_not_null:", func() {
			s, err := db.RunScript("symbol(`A +string(1..9))$3:3")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [3][3]string{{"A1", "A4", "A7"}, {"A2", "A5", "A8"}, {"A3", "A6", "A9"}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == zx[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 3)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 17)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbol")
		})
		Convey("Test_matrix_symbol_null:", func() {
			s, err := db.RunScript("matrix(SYMBOL, 3, 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 145)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbolExtend")
		})
		Convey("Test_matrix_symbol_all_null:", func() {
			s, err := db.RunScript("symbol(take(string(), 12))$3:4")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 4; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, "")
				}
			}
		})
		Convey("Test_matrix_symbol_some_null:", func() {
			s, err := db.RunScript("symbol(['AA', 'BB',NULL, 'CC',NULL, 'DD'])$2:3")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [2][3]string{{"AA"}, {"BB", "CC", "DD"}}
			for i := 0; i < 2; i++ {
				for j := 0; j < 3; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_date(t *testing.T) {
	Convey("Test_matrix_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_date_not_null:", func() {
			s, err := db.RunScript("a = 1969.12.31 1970.01.01 1970.01.02 2006.01.02 2006.01.03 2022.08.03 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC)}, {time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_matrix_date_null:", func() {
			s, err := db.RunScript("a = take(00d,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_month(t *testing.T) {
	Convey("Test_matrix_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_month_not_null:", func() {
			s, err := db.RunScript("a = 1969.12M 1970.01M 1970.02M 2006.01M 2006.02M 2022.08M $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC)}, {time.Date(1970, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 1, 0, 0, 0, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_matrix_month_early_1970:", func() {
			s, err := db.RunScript("a = take(1922.06M+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [3][2]string{{"1922-07", "1922-10"}, {"1922-08", "1922-11"}, {"1922-09", "1922-12"}}
			t0, _ := time.Parse("2006-01", time1[0][0])
			t1, _ := time.Parse("2006-01", time1[0][1])
			t2, _ := time.Parse("2006-01", time1[1][0])
			t3, _ := time.Parse("2006-01", time1[1][1])
			t4, _ := time.Parse("2006-01", time1[2][0])
			t5, _ := time.Parse("2006-01", time1[2][1])
			So(result.Get(0, 0).Value(), ShouldEqual, t0)
			So(result.Get(0, 1).Value(), ShouldEqual, t1)
			So(result.Get(1, 0).Value(), ShouldEqual, t2)
			So(result.Get(1, 1).Value(), ShouldEqual, t3)
			So(result.Get(2, 0).Value(), ShouldEqual, t4)
			So(result.Get(2, 1).Value(), ShouldEqual, t5)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_matrix_month_null:", func() {
			s, err := db.RunScript("a = take(month(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_time(t *testing.T) {
	Convey("Test_matrix_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_time_not_null:", func() {
			s, err := db.RunScript("a = 23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_matrix_time_null:", func() {
			s, err := db.RunScript("a = take(time(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_minute(t *testing.T) {
	Convey("Test_matrix_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_minute_not_null:", func() {
			s, err := db.RunScript("a = 23:59m 00:00m 00:01m 15:04m 15:05m 15:15m $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 5, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 1, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 15, 0, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
			result.SetNull(1, 0)
		})
		Convey("Test_matrix_minute_null:", func() {
			s, err := db.RunScript("a = take(00m,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_second(t *testing.T) {
	Convey("Test_matrix_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_second_not_null:", func() {
			s, err := db.RunScript("a = 23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_matrix_second_null:", func() {
			s, err := db.RunScript("a = take(00s,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_datetime(t *testing.T) {
	Convey("Test_matrix_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_datetime_not_null:", func() {
			s, err := db.RunScript("a = 1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_matrix_datetime_null:", func() {
			s, err := db.RunScript("a = take(datetime(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_timestamp(t *testing.T) {
	Convey("Test_matrix_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_timestamp_not_null:", func() {
			s, err := db.RunScript("a = 1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_matrix_timestamp_null:", func() {
			s, err := db.RunScript("a = take(timestamp(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_nanotime(t *testing.T) {
	Convey("Test_matrix_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_nanotime_not_null:", func() {
			s, err := db.RunScript("a = 23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_matrix_nanotime_null:", func() {
			s, err := db.RunScript("a = take(nanotime(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_nanotimestamp(t *testing.T) {
	Convey("Test_matrix_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_nanotimestamp_not_null:", func() {
			s, err := db.RunScript("a = 1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000 $3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_matrix_nanotimestamp_null:", func() {
			s, err := db.RunScript("a = take(nanotimestamp(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_datehour(t *testing.T) {
	Convey("Test_matrix_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_datehour_not_null:", func() {
			s, err := db.RunScript("a = datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000, 1970.01.01T00:00:01.999, 2006.01.02T15:04:04.999, 2006.01.02T15:04:05.000, 2022.08.03T15:00:15.000]$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			time1 := [][]time.Time{{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}, {time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 0, 0, time.UTC)}}
			var k int
			for i := 0; i < result.Data.Rows(); i++ {
				for j := 0; j < int(result.Data.ColumnCount); j++ {
					if result.Get(i, j).Value() == time1[i][j] {
						k++
					}
				}
			}
			So(result.Data.ColumnCount, ShouldEqual, 2)
			So(result.Data.RowCount, ShouldEqual, 3)
			So(k, ShouldEqual, result.Data.ColumnCount*result.Data.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		Convey("Test_matrix_datehour_null:", func() {
			s, err := db.RunScript("a = take(datehour(['','','','','',''])+1..6,6)$3:2;a")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					So(result.Get(i, j).IsNull(), ShouldEqual, true)
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_ony_one_column(t *testing.T) {
	Convey("Test_matrix_only_one_column:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_one_column:", func() {
			s, err := db.RunScript("matrix(1..6)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [6][1]int{}
			for i := 0; i < 6; i++ {
				for j := 0; j < 1; j++ {
					re := result.Get(i, j).Value()
					zx[i][j] = i + 1
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_only_one_row(t *testing.T) {
	Convey("Test_matrix_only_one_row:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_one_row:", func() {
			s, err := db.RunScript("matrix(take(1, 6)).transpose()")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			zx := [1][6]int{}
			for i := 0; i < 1; i++ {
				for j := 0; j < 6; j++ {
					re := result.Get(i, j).Value()
					zx[i][j] = i + 1
					So(re, ShouldEqual, zx[i][j])
				}
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_with_int_label(t *testing.T) {
	Convey("Test_matrix_with_int_label:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_int_with_label:", func() {
			s, err := db.RunScript("cross(add,1..3,1..4)")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			col := result.ColumnLabels.Data.Value()
			rerow := [3]int{1, 2, 3}
			recol := [4]int{1, 2, 3, 4}
			for i := 0; i < 3; i++ {
				So(row[i], ShouldEqual, rerow[i])
			}
			for i := 0; i < 4; i++ {
				So(col[i], ShouldEqual, recol[i])
			}
			zx := [3][4]int{}
			for i := 0; i < 3; i++ {
				for j := 0; j < 4; j++ {
					re := result.Get(i, j).Value()
					zx[i][j] = i + j + 2
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		Convey("Test_matrix_int_only_with_row_label :", func() {
			s, err := db.RunScript("m=1..6$3:2;m.rename!([0, 1, 2],);m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			rerow := [3]int{0, 1, 2}
			for i := 0; i < 3; i++ {
				So(row[i], ShouldEqual, rerow[i])
			}
			zx := [3][2]int{{1, 4}, {2, 5}, {3, 6}}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		Convey("Test_matrix_int_only_with_col_label :", func() {
			s, err := db.RunScript("m=1..6$3:2;m.rename!([0, 1]);m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			col := result.ColumnLabels.Data.Value()
			recol := [2]int{0, 1}
			for i := 0; i < 2; i++ {
				So(col[i], ShouldEqual, recol[i])
			}
			zx := [3][2]int{{1, 4}, {2, 5}, {3, 6}}
			for i := 0; i < 3; i++ {
				for j := 0; j < 2; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_DownLoad_with_time_label(t *testing.T) {
	Convey("Test_matrix_with_time_label:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_label_date_symbol:", func() {
			s, err := db.RunScript("m=matrix([2200, 1300, 2500, 8800], [6800, 5400,NULL,NULL], [1900, 2100, 3200,NULL]).rename!(2012.01.01..2012.01.04, symbol(`C`IBM`MS));m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			col := result.ColumnLabels.Data.Value()
			time1 := [4]string{"2012-01-01 00:00:00", "2012-01-02 00:00:00", "2012-01-03 00:00:00", "2012-01-04 00:00:00"}
			t0, _ := time.Parse("2006-01-02 15:04:05", time1[0])
			t1, _ := time.Parse("2006-01-02 15:04:05", time1[1])
			t2, _ := time.Parse("2006-01-02 15:04:05", time1[2])
			t3, _ := time.Parse("2006-01-02 15:04:05", time1[3])
			So(row[0], ShouldEqual, t0)
			So(row[1], ShouldEqual, t1)
			So(row[2], ShouldEqual, t2)
			So(row[3], ShouldEqual, t3)
			recol := [3]string{"C", "IBM", "MS"}
			for i := 0; i < 3; i++ {
				So(col[i], ShouldEqual, recol[i])
			}
			zx := [4][3]int{{2200, 6800, 1900}, {1300, 5400, 2100}, {2500, math.MinInt32, 3200}, {8800, math.MinInt32, math.MinInt32}}
			for i := 0; i < 4; i++ {
				for j := 0; j < 3; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		Convey("Test_matrix_label_second_symbol:", func() {
			s, err := db.RunScript("m=matrix([2200, 1300, 2500, 8800], [6800, 5400,NULL,NULL], [1900, 2100, 3200,NULL]).rename!([09:30:00, 10:00:00, 10:30:00, 11:00:00], symbol(`C`IBM`MS));m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			col := result.ColumnLabels.Data.Value()
			time1 := [4]string{"1970-01-01T09:30:00", "1970-01-01T10:00:00", "1970-01-01T10:30:00", "1970-01-01T11:00:00"}
			t0, _ := time.Parse("2006-01-02T15:04:05", time1[0])
			t1, _ := time.Parse("2006-01-02T15:04:05", time1[1])
			t2, _ := time.Parse("2006-01-02T15:04:05", time1[2])
			t3, _ := time.Parse("2006-01-02T15:04:05", time1[3])
			So(row[0], ShouldEqual, t0)
			So(row[1], ShouldEqual, t1)
			So(row[2], ShouldEqual, t2)
			So(row[3], ShouldEqual, t3)
			recol := [3]string{"C", "IBM", "MS"}
			for i := 0; i < 3; i++ {
				So(col[i], ShouldEqual, recol[i])
			}
			zx := [4][3]int{{2200, 6800, 1900}, {1300, 5400, 2100}, {2500, math.MinInt32, 3200}, {8800, math.MinInt32, math.MinInt32}}
			for i := 0; i < 4; i++ {
				for j := 0; j < 3; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		Convey("Test_matrix_label_symbol_date:", func() {
			s, err := db.RunScript("m=matrix([2200, 1300, 2500, 8800], [6800, 5400,NULL,NULL], [1900, 2100, 3200,NULL]).rename!(`C`IBM`MS`ZZ, 2012.01.01..2012.01.03);m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			col := result.ColumnLabels.Data.Value()
			rerow := [4]string{"C", "IBM", "MS", "ZZ"}
			for i := 0; i < 4; i++ {
				So(row[i], ShouldEqual, rerow[i])
			}
			time1 := [3]string{"2012-01-01 00:00:00", "2012-01-02 00:00:00", "2012-01-03 00:00:00"}
			t0, _ := time.Parse("2006-01-02 15:04:05", time1[0])
			t1, _ := time.Parse("2006-01-02 15:04:05", time1[1])
			t2, _ := time.Parse("2006-01-02 15:04:05", time1[2])
			So(col[0], ShouldEqual, t0)
			So(col[1], ShouldEqual, t1)
			So(col[2], ShouldEqual, t2)
			zx := [4][3]int{{2200, 6800, 1900}, {1300, 5400, 2100}, {2500, math.MinInt32, 3200}, {8800, math.MinInt32, math.MinInt32}}
			for i := 0; i < 4; i++ {
				for j := 0; j < 3; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		Convey("Test_matrix_label_symbol_second:", func() {
			s, err := db.RunScript("m=matrix([2200, 1300, 2500, 8800], [6800, 5400,NULL,NULL], [1900, 2100, 3200,NULL]).rename!(`C`IBM`MS`ZZ, [09:30:00, 10:00:00, 10:30:00]);m")
			So(err, ShouldBeNil)
			result := s.(*model.Matrix)
			row := result.RowLabels.Data.Value()
			col := result.ColumnLabels.Data.Value()
			rerow := [4]string{"C", "IBM", "MS", "ZZ"}
			for i := 0; i < 4; i++ {
				So(row[i], ShouldEqual, rerow[i])
			}
			time1 := [3]string{"1970-01-01T09:30:00", "1970-01-01T10:00:00", "1970-01-01T10:30:00"}
			t0, _ := time.Parse("2006-01-02T15:04:05", time1[0])
			t1, _ := time.Parse("2006-01-02T15:04:05", time1[1])
			t2, _ := time.Parse("2006-01-02T15:04:05", time1[2])
			So(col[0], ShouldEqual, t0)
			So(col[1], ShouldEqual, t1)
			So(col[2], ShouldEqual, t2)
			zx := [4][3]int{{2200, 6800, 1900}, {1300, 5400, 2100}, {2500, math.MinInt32, 3200}, {8800, math.MinInt32, math.MinInt32}}
			for i := 0; i < 4; i++ {
				for j := 0; j < 3; j++ {
					re := result.Get(i, j).Value()
					So(re, ShouldEqual, zx[i][j])
				}
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_int(t *testing.T) {
	Convey("Test_matrix_int_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_int_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST INT MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtInt)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_short(t *testing.T) {
	Convey("Test_matrix_short_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_char_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtShort, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST SHORT MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtShort)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_char(t *testing.T) {
	Convey("Test_matrix_char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_char_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtChar, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}
			for i := 0; i < len(re); i++ {
				So(re[i], ShouldEqual, zx[i])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST CHAR MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtChar)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_long(t *testing.T) {
	Convey("Test_matrix_long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_long_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtLong, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST LONG MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtLong)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_float(t *testing.T) {
	Convey("Test_matrix_float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_float_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtFloat, []float32{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []float32{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST FLOAT MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtFloat)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_double(t *testing.T) {
	Convey("Test_matrix_double_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_double_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DOUBLE MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtDouble)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_date(t *testing.T) {
	Convey("Test_matrix_date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_date_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATE MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtDate)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_month(t *testing.T) {
	Convey("Test_matrix_month_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_month_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST MONTH MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtMonth)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_time(t *testing.T) {
	Convey("Test_matrix_time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_time_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST TIME MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_minute(t *testing.T) {
	Convey("Test_matrix_minute_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_minute_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST MINUTE MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtMinute)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_second(t *testing.T) {
	Convey("Test_matrix_second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_second_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST SECOND MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtSecond)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_datetime(t *testing.T) {
	Convey("Test_matrix_datetime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_datetime_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATETIME MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtDatetime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_timestamp(t *testing.T) {
	Convey("Test_matrix_timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_timestamp_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST TIMESTAMP MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_nanotime(t *testing.T) {
	Convey("Test_matrix_nanotime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_nanotime_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST NANOTIME MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_nanotimestamp(t *testing.T) {
	Convey("Test_matrix_nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_nanotimestamp_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST NANOTIMESTAMP MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_datehour(t *testing.T) {
	Convey("Test_matrix_datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_datehour_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATEHOUR MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtDateHour)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_complex(t *testing.T) {
	Convey("Test_matrix_complex_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_complex_upload:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtComplex, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			So(err, ShouldBeNil)
			rl, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{1})
			So(err, ShouldBeNil)
			cl, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{1, 2, 3})
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), model.NewVector(rl), model.NewVector(cl))
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			zx := []string{"1.00000+1.00000i", "-1.00000+-1024.50000i", "1001022.40000+-30028.75000i"}
			var j int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					j++
				}
			}
			So(j, ShouldEqual, len(re))
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST COMPLEX MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtComplex)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Matrix_UpLoad_DataType_big_array(t *testing.T) {
	Convey("Test_matrix_big_array_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_matrix_int_big_array_upload:", func() {
			var i int32
			var j int
			intv := []int32{}
			for i = 0; i < 3000000*12; i += 12 {
				intv = append(intv, i)
			}
			intv = append(intv, model.NullInt)
			data, err := model.NewDataTypeListWithRaw(model.DtInt, intv)
			So(err, ShouldBeNil)
			mtx := model.NewMatrix(model.NewVector(data), nil, nil)
			_, err = db.Upload(map[string]model.DataForm{"s": mtx})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			result := res.(*model.Matrix)
			re := result.Data.Data.Value()
			for i := 0; i < len(re); i++ {
				if re[i] == intv[i] {
					j++
				}
			}
			So(j, ShouldEqual, 3000001)
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST INT MATRIX)")
			So(res.GetDataType(), ShouldEqual, model.DtInt)
		})
		So(db.Close(), ShouldBeNil)
	})
}
