// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate test;

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::Array;
use arrow_array::ArrayAccessor;
use arrow_buffer::NullBuffer;
use databend_common_expression::types::geography::Geography;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::geography::GeographyColumnBuilder;
use databend_common_expression::types::geography::GeographyRef;
use databend_common_expression::types::DataType;
use databend_common_geobuf::Ewkb;
use databend_common_geobuf::FeatureKind;
use databend_common_geobuf::ObjectKind;
use databend_common_geobuf::Wkb;
use geoarrow::algorithm::native::bounding_rect::BoundingRect;
use geoarrow::algorithm::native::TotalBounds;
use geoarrow::array::MixedGeometryArray;
use geoarrow::geo_traits::*;
use geoarrow::trait_::*;
use geoarrow::GeometryArrayTrait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use test::Bencher;

// test geo::bench_wkb_to_geobuf        ... bench: 338,992,675.50 ns/iter (+/- 30,080,487.62)
#[bench]
fn bench_wkb_to_geobuf(b: &mut Bencher) {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let reader = reader.map(|batch| {
        let batch = batch.unwrap();
        let geometry = batch.column_by_name("geometry").unwrap();
        geometry
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap()
            .clone()
    });
    let all: Vec<_> = reader.collect();

    b.iter(|| {
        for arr in &all {
            let _ = wkb_to_geobuf(arr);
        }
    })
}

#[test]
fn test_geobuf() {
    let all = load_geobuf();

    let statist = all.iter().fold(Statist::default(), |acc, column| Statist {
        num_row: acc.num_row + column.len(),
        mem: acc.mem + column.memory_size(),
    });
    println!("{statist:?}");

    for (i, part) in all.iter().enumerate() {
        let bbox = total_bounds(part);
        println!("row_group:{i} bbox:{bbox:?}")
    }

    let mut point = 0;
    let mut line = 0;
    let mut polygon = 0;
    let mut multi_polygon = 0;
    let mut other = 0;
    for part in all.iter() {
        for geog in part.iter() {
            match geog.0.kind().unwrap() {
                FeatureKind::Feature(k) | FeatureKind::Geometry(k) => match k {
                    ObjectKind::Point => point += 1,
                    ObjectKind::LineString => line += 1,
                    ObjectKind::Polygon => polygon += 1,
                    ObjectKind::MultiPolygon => multi_polygon += 1,
                    _ => other += 1,
                },
                FeatureKind::FeatureCollection => {
                    other += 1;
                }
            }
        }
    }

    // point 102538 line 189466 polygon 164480 multi_polygon 473 other 1
    println!(
        "point {point} line {line} polygon {polygon} multi_polygon {multi_polygon} other {other}"
    )
}

// test geo::bench_geobuf_load          ... bench: 603,078,927.40 ns/iter (+/- 45,899,069.17)
// test geo::bench_geobuf_load     ... bench: 557,991,343.70 ns/iter (+/- 59,629,855.83)
// load 解析 wbk，然后构造 geobuf。这里的慢是符合预期的，因为把 offset 写入 buf 会引起更多的扩容
#[bench]
fn bench_geobuf_load(b: &mut Bencher) {
    b.iter(load_geobuf)
}

#[test]
fn test_geobuf_write_parquet() {
    let all = load_geobuf();

    let filename = format!(
        "tests/it/testdata/temp_geobuf_{}.parquet",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    write_geobuf(&all, &filename);
}

// file size: 67762549
// test geo::bench_geobuf_write_parquet ... bench: 505,016,040.60 ns/iter (+/- 601,611,236.05)
#[bench]
fn bench_geobuf_write_parquet(b: &mut Bencher) {
    let all = load_geobuf();

    b.iter(|| {
        let filename = format!(
            "tests/it/testdata/temp_geobuf_{}.parquet",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        write_geobuf(&all, &filename);
        std::fs::remove_file(&filename).unwrap();
    })
}

#[test]
fn test_geobuf_read_parquet() {
    let all = load_geobuf();

    let filename = format!(
        "tests/it/testdata/temp_geobuf_{}.parquet",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    write_geobuf(&all, &filename);

    let all = read_geobuf(&filename);

    let statist = all.iter().fold(Statist::default(), |acc, column| Statist {
        num_row: acc.num_row + column.len(),
        mem: acc.mem + column.memory_size(),
    });
    println!("{statist:?}");

    std::fs::remove_file(&filename).unwrap();
}

// test geo::bench_geobuf_read_parquet  ... bench: 123,843,012.80 ns/iter (+/- 15,347,599.03)
#[bench]
fn bench_geobuf_read_parquet(b: &mut Bencher) {
    let all = load_geobuf();

    let filename = format!(
        "tests/it/testdata/temp_geobuf_{}.parquet",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    write_geobuf(&all, &filename);

    b.iter(|| {
        let _ = read_geobuf(&filename);
    });

    std::fs::remove_file(&filename).unwrap();
}

// test geo::bench_geobuf_bounds   ... bench:  25,642,409.80 ns/iter (+/- 2,727,722.00)
#[bench]
fn bench_geobuf_bounds(b: &mut Bencher) {
    let all = load_geobuf();

    b.iter(|| {
        for part in all.iter() {
            let _ = total_bounds(part);
        }
    })
}

#[test]
fn test_geoarrow() {
    let all = load_geoarrow();

    let statist = all.iter().fold(Statist::default(), |acc, mixed| Statist {
        num_row: acc.num_row + mixed.len(),
        mem: 0,
    });
    println!("{statist:?}");

    for (i, part) in all.iter().enumerate() {
        let bbox = part.total_bounds();
        println!("row_group:{i} bbox:{bbox:?}")
    }
}

// test geo::bench_geoarrow_load   ... bench: 325,833,397.10 ns/iter (+/- 20,798,587.56)
#[bench]
fn bench_geoarrow_load(b: &mut Bencher) {
    b.iter(load_geoarrow)
}

// https://github.com/apache/arrow-rs/issues/73
// 不支持 Union，除非非标自己实现一个
// https://github.com/apache/parquet-format/pull/240
// https://github.com/opengeospatial/geoparquet/issues/222#issuecomment-2128298217
// parquet 这边的意思是，没想好怎么办更好，先存WKB吧
// https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md
// 没有 mixed，没有 GeometryCollection
#[test]
fn test_geoarrow_write_parquet() {
    let all = load_geoarrow();

    let filename = format!(
        "tests/it/testdata/temp_geoarrow_{}.parquet",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    write_mixed(&all, &filename);
}

// test geo::bench_geoarrow_bounds ... bench:  89,073,962.70 ns/iter (+/- 4,899,218.94)
// geoarrow 试图兼容所有的情况，交错布局的点，高维的点，wbk这种需要解码的才能访问的也要支持。
// 他们的关注点在通用性，在整合各种情况，希望做成一个大而全的东西，而效率则是次要的，所以可以发现实现里面有一大堆抽象，也大量使用宏。
// 针对一些更具体的场景，很容易想到一些特化的算法，有更高的效率。例如这里算 bbox 就可以快2.5倍。
// 但是合并进 geoarrow 的体系里面去，需要考虑那些我们完全用不上的功能，感觉没有这个必要。
#[bench]
fn bench_geoarrow_bounds(b: &mut Bencher) {
    let all = load_geoarrow();

    b.iter(|| {
        for part in all.iter() {
            part.total_bounds();
        }
    })
}

#[test]
fn test_geobuf_as_geoarrow() {
    let all = load_geobuf();

    for part in all {
        Column(part).total_bounds();
    }
}

// 就非原生算法而言，geoarrow的价值在于提供了一个通用的访问层。通用和效率是矛盾的。
// test geo::bench_geoarrow_center      ... bench: 140,231,467.80 ns/iter (+/- 23,895,793.41)
#[bench]
fn bench_geoarrow_center(b: &mut Bencher) {
    let all = load_geoarrow();

    b.iter(|| {
        for (i, part) in all.iter().enumerate() {
            // GeometryCollection
            if i == 387 {
                continue;
            }
            geoarrow::algorithm::geo::Center::center(part);
        }
    })
}

// test geo::bench_geobuf_center        ... bench: 191,621,676.90 ns/iter (+/- 13,550,758.75)
// 这个算法那么简单没道理不用原生实现啊，不就是先算bbox，然后算中点么？根本没必要构造geometry，bbox可以直接算的。
#[bench]
fn bench_geobuf_center(b: &mut Bencher) {
    let all = load_geobuf();

    b.iter(|| {
        for (i, part) in all.iter().enumerate() {
            // GeometryCollection
            if i == 387 {
                continue;
            }
            Column(part.clone()).center();
        }
    })
}

// test geo::bench_wkb_center           ... bench:  71,486,765.50 ns/iter (+/- 10,710,319.22)
// 这里就好笑了，魔法越多，性能越差。
#[bench]
fn bench_wkb_center(b: &mut Bencher) {
    let all = load_wkb();

    b.iter(|| {
        for (i, part) in all.iter().enumerate() {
            // GeometryCollection
            if i == 387 {
                continue;
            }
            geoarrow::algorithm::geo::Center::center(part);
        }
    })
}

fn load_geobuf() -> Vec<GeographyColumn> {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let reader = reader.map(|batch| {
        let batch = batch.unwrap();
        let geometry = batch.column_by_name("geometry").unwrap();
        let geometry = geometry
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        wkb_to_geobuf(geometry)
    });
    reader.collect()
}

fn write_geobuf(all: &[GeographyColumn], filename: &str) {
    let file = std::fs::File::create(filename).unwrap();

    let field = databend_common_expression::Column::Geography(all[0].clone()).arrow_field();
    let schema = Arc::new(arrow_schema::Schema::new([Arc::new(field.into())]));

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for part in all.iter() {
        let arr = databend_common_expression::Column::Geography(part.clone()).into_arrow_rs();
        let batch = arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

fn read_geobuf(filename: &str) -> Vec<GeographyColumn> {
    let file = std::fs::File::open(filename).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let reader = reader.map(|batch| {
        let batch = batch.unwrap();
        let geometry = batch.column(0);
        let column = databend_common_expression::Column::from_arrow_rs(
            geometry.clone(),
            &DataType::Geography,
        )
        .unwrap();

        column.into_geography().unwrap()
    });
    reader.collect()
}

fn wkb_to_geobuf(arr: &arrow_array::BinaryArray) -> GeographyColumn {
    assert!(!arr.is_nullable());
    let mut builder = GeographyColumnBuilder::with_capacity(arr.len(), arr.len());
    for data in arr.iter() {
        if let Some(data) = data {
            let geometry = Ewkb(data).try_into().unwrap();
            let geog = Geography(geometry);
            builder.push(geog.as_ref())
        } else {
            builder.push_default();
        }
    }
    builder.build()
}

fn total_bounds(col: &GeographyColumn) -> BoundingRect {
    let lon = col.lon();
    let lat = col.lat();
    BoundingRect {
        minx: **lon.iter().min().unwrap(),
        miny: **lat.iter().min().unwrap(),
        maxx: **lon.iter().max().unwrap(),
        maxy: **lat.iter().max().unwrap(),
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Statist {
    num_row: usize,
    mem: usize,
}

fn load_geoarrow() -> Vec<MixedGeometryArray<i64, 2>> {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let reader = reader.map(|batch| {
        let batch = batch.unwrap();
        let geometry = batch.column_by_name("geometry").unwrap();
        let geometry = geometry
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        wbk_to_mixed(geometry)
    });
    reader.collect()
}

fn load_wkb() -> Vec<geoarrow::array::WKBArray<i32>> {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let reader = reader.map(|batch| {
        let batch = batch.unwrap();
        let geometry = batch.column_by_name("geometry").unwrap();
        let geometry = geometry
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        let metadata = Arc::new(geoarrow::array::metadata::ArrayMetadata::default());

        geoarrow::array::WKBArray::new(geometry.clone(), metadata)
    });
    reader.collect()
}

fn wbk_to_mixed(arr: &arrow_array::BinaryArray) -> MixedGeometryArray<i64, 2> {
    let metadata = Arc::new(geoarrow::array::metadata::ArrayMetadata::default());
    let wkb_arr = geoarrow::array::WKBArray::new(arr.clone(), metadata);

    let mut builder = geoarrow::array::MixedGeometryBuilder::<i64, 2>::new();

    for wkb in wkb_arr.iter() {
        match wkb {
            Some(wkb) => {
                let object = wkb.to_wkb_object();

                match object.as_type() {
                    GeometryType::Point(_) => {
                        builder.push_geometry(Some(&object)).unwrap();
                    }
                    GeometryType::GeometryCollection(_) => {
                        // 这一块实现，怎么办还不知道呢。
                        // WARN: not implemented
                        // builder
                        //     .push_geometry_preferring_multi(Some(&object))
                        //     .unwrap()

                        // WARN: not implemented
                        // builder.push_null()
                    }
                    _ => builder
                        .push_geometry_preferring_multi(Some(&object))
                        .unwrap(),
                }
            }
            None => builder.push_null(),
        }
    }

    builder.finish()
}

fn write_mixed(all: &[MixedGeometryArray<i64, 2>], filename: &str) {
    let file = std::fs::File::create(filename).unwrap();

    let field = all[0].data_type().to_field("mixed", false);
    let schema = Arc::new(arrow_schema::Schema::new([Arc::new(field)]));

    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for part in all.iter() {
        let arr = part.to_array_ref();
        let batch = arrow_array::RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

#[derive(Debug)]
struct Column(pub GeographyColumn);

impl Column {
    fn total_bounds(&self) -> BoundingRect {
        let mut bounds = BoundingRect::new();
        for scalar in self.iter().flatten() {
            let geom = scalar.0.0;
            let geom: geo::Geometry = geom.try_into().unwrap();
            bounds.add_geometry(&geom);
        }
        bounds
    }
}

// 估计传输层需要，这里不实现也不影响
impl geoarrow::trait_::GeometryArrayTrait for Column {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn data_type(&self) -> geoarrow::datatypes::GeoDataType {
        todo!()
    }

    fn storage_type(&self) -> arrow_schema::DataType {
        todo!()
    }

    fn extension_field(&self) -> Arc<arrow_schema::Field> {
        todo!()
    }

    fn extension_name(&self) -> &str {
        todo!()
    }

    fn into_array_ref(self) -> arrow_array::ArrayRef {
        todo!()
    }

    fn to_array_ref(&self) -> arrow_array::ArrayRef {
        todo!()
    }

    fn coord_type(&self) -> geoarrow::array::CoordType {
        geoarrow::array::CoordType::Separated
    }

    fn to_coord_type(
        &self,
        _coord_type: geoarrow::array::CoordType,
    ) -> Arc<dyn GeometryArrayTrait> {
        todo!()
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn metadata(&self) -> Arc<geoarrow::array::metadata::ArrayMetadata> {
        todo!()
    }

    fn with_metadata(
        &self,
        _metadata: Arc<geoarrow::array::metadata::ArrayMetadata>,
    ) -> geoarrow::trait_::GeometryArrayRef {
        todo!()
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        todo!()
    }
}

impl Column {
    fn center(&self) -> geoarrow::array::PointArray<2> {
        use geo::BoundingRect;
        use geoarrow::array::*;

        let mut output_array = PointBuilder::with_capacity(self.len());
        self.iter_geo().for_each(|maybe_g| {
            output_array.push_point(
                maybe_g
                    .and_then(|g| g.bounding_rect().map(|rect| rect.center()))
                    .as_ref(),
            )
        });
        output_array.into()
    }
}

impl<'a> geoarrow::trait_::GeometryArrayAccessor<'a> for Column {
    type Item = Scalar<'a>;

    type ItemGeo = geo::Geometry;

    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        Scalar(self.0.index_unchecked(index))
    }
}

struct Scalar<'a>(pub GeographyRef<'a>);

impl<'a> From<Scalar<'a>> for geo::Geometry {
    fn from(value: Scalar<'a>) -> Self {
        let geom = value.0.0;
        geom.try_into().unwrap()
    }
}

impl<'a> geoarrow::trait_::GeometryScalarTrait for Scalar<'a> {
    type ScalarGeo = geo::Geometry;

    fn to_geo(&self) -> Self::ScalarGeo {
        let geom = self.0.0.clone();
        geom.try_into().unwrap()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        self.to_geo()
    }
}

// 如果实现了 GeometryTrait 应该可以完全屏蔽掉底层差异
// impl<'a> GeometryTrait for Scalar<'a> {
//     type T;

//     type Point<'a>
//     where
//         Self: 'a;

//     type LineString<'a>
//     where
//         Self: 'a;

//     type Polygon<'a>
//     where
//         Self: 'a;

//     type MultiPoint<'a>
//     where
//         Self: 'a;

//     type MultiLineString<'a>
//     where
//         Self: 'a;

//     type MultiPolygon<'a>
//     where
//         Self: 'a;

//     type GeometryCollection<'a>
//     where
//         Self: 'a;

//     type Rect<'a>
//     where
//         Self: 'a;

//     fn dim(&self) -> usize {
//         todo!()
//     }

//     fn as_type(
//         &self,
//     ) -> GeometryType<
//         '_,
//         Self::Point<'_>,
//         Self::LineString<'_>,
//         Self::Polygon<'_>,
//         Self::MultiPoint<'_>,
//         Self::MultiLineString<'_>,
//         Self::MultiPolygon<'_>,
//         Self::GeometryCollection<'_>,
//         Self::Rect<'_>,
//     > {
//         todo!()
//     }
// }
