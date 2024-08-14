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

use arrow_array::Array;
use arrow_buffer::NullBuffer;
use databend_common_expression::types::geography::Geography;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::geography::GeographyColumnBuilder;
use databend_common_expression::types::geography::GeographyRef;
use databend_common_geobuf::Ewkb;
use databend_common_geobuf::FeatureKind;
use databend_common_geobuf::ObjectKind;
use geoarrow::algorithm::native::bounding_rect::BoundingRect;
use geoarrow::algorithm::native::TotalBounds;
use geoarrow::array::MixedGeometryArray;
use geoarrow::geo_traits::*;
use geoarrow::trait_::*;
use geoarrow::GeometryArrayTrait;
use ordered_float::OrderedFloat;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use test::Bencher;

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

// test geo::bench_geobuf_load     ... bench: 557,991,343.70 ns/iter (+/- 59,629,855.83)
// load 解析 wbk，然后构造 geobuf。这里的慢是符合预期的，因为把 offset 写入 buf 会引起更多的扩容
#[bench]
fn bench_geobuf_load(b: &mut Bencher) {
    b.iter(|| load_geobuf())
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
    b.iter(|| load_geoarrow())
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
#[test]
fn test_center() {
    let all = load_geoarrow();

    for part in all.iter() {
        geoarrow::algorithm::geo::Center::center(part);
    }
}

fn load_geobuf() -> Vec<GeographyColumn> {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(&path).unwrap();

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

        array_to_column(&geometry)
    });
    reader.collect()
}

fn array_to_column(arr: &arrow_array::BinaryArray) -> GeographyColumn {
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
    let lon = unsafe { std::mem::transmute::<&[f64], &[OrderedFloat<f64>]>(col.lon()) };
    let lat = unsafe { std::mem::transmute::<&[f64], &[OrderedFloat<f64>]>(col.lat()) };
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

fn load_geoarrow() -> Vec<MixedGeometryArray<i32, 2>> {
    let path = Path::new("tests/it/testdata/hong-kong-latest_nofilter_noclip_compact.parquet");
    let file = std::fs::File::open(&path).unwrap();

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

        array_to_mixed(&geometry)
    });
    reader.collect()
}

fn array_to_mixed(arr: &arrow_array::BinaryArray) -> MixedGeometryArray<i32, 2> {
    let metadata = Arc::new(geoarrow::array::metadata::ArrayMetadata::default());
    let wkb_arr = geoarrow::array::WKBArray::new(arr.clone(), metadata);

    let mut builder = geoarrow::array::MixedGeometryBuilder::<i32, 2>::new();

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

    fn to_coord_type(&self, coord_type: geoarrow::array::CoordType) -> Arc<dyn GeometryArrayTrait> {
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
        metadata: Arc<geoarrow::array::metadata::ArrayMetadata>,
    ) -> geoarrow::trait_::GeometryArrayRef {
        todo!()
    }

    fn as_ref(&self) -> &dyn GeometryArrayTrait {
        todo!()
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
        let geom = self.0.0;
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
