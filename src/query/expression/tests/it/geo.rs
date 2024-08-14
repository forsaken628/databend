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
use databend_common_expression::types::geography::Geography;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::geography::GeographyColumnBuilder;
use databend_common_geobuf::Ewkb;
use geoarrow;
use geoarrow::algorithm::native::bounding_rect::BoundingRect;
use geoarrow::algorithm::native::TotalBounds;
use geoarrow::array::MixedGeometryArray;
use geoarrow::geo_traits::GeometryTrait;
use geoarrow::geo_traits::GeometryType;
use geoarrow::trait_::GeometryArrayAccessor;
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
}

#[bench]
fn bench_geobuf_load(b: &mut Bencher) {
    b.iter(|| load_geobuf())
}

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

#[bench]
fn bench_geoarrow_load(b: &mut Bencher) {
    b.iter(|| load_geoarrow())
}

#[bench]
fn bench_geoarrow_bounds(b: &mut Bencher) {
    let all = load_geoarrow();

    b.iter(|| {
        for part in all.iter() {
            part.total_bounds();
        }
    })
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
