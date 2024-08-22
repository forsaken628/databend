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

use geographiclib_rs::Geodesic;
use s2::latlng::LatLng;
use s2::point::Point;
use s2::s1;

use super::s2_polyfill::*;

pub fn point_to_point_distance(a: &LatLng, b: &LatLng) -> f64 {
    let mut u = MinDistanceUpdater::new();
    u.update(&a.into(), &b.into());
    u.distance()
}

pub fn point_to_line_distance(a: &LatLng, b: &Polyline) -> f64 {
    let mut updater = MinDistanceUpdater::new();
    let mut c = GeographyDistanceCalculator {
        updater: &mut updater,
        bounding_box_intersects: false, // todo
    };

    on_point_to_line_string(&mut c, &a.into(), b);
    updater.distance()
}

/// MinDistanceUpdater finds the minimum distance using a sphere.
/// Methods will return early if it finds a minimum distance <= stopAfterLE.
#[derive(Debug)]
struct MinDistanceUpdater {
    desic: Geodesic,
    sphere_or_spheroid: UseSphereOrSpheroid,
    min_edge: Edge,
    min_d: s1::ChordAngle,
    stop_after: s1::ChordAngle,
    exclusivity: bool,
}

#[derive(Debug)]
enum UseSphereOrSpheroid {
    #[allow(dead_code)]
    Sphere,
    Spheroid,
}

impl MinDistanceUpdater {
    fn new() -> Self {
        Self {
            desic: Geodesic::wgs84(),
            sphere_or_spheroid: UseSphereOrSpheroid::Spheroid,
            min_edge: Edge::default(),
            min_d: s1::ChordAngle::inf(),
            stop_after: s1::ChordAngle(0.0),
            exclusivity: false,
        }
    }

    fn update(&mut self, a: &Point, b: &Point) -> bool {
        let sphere_distance = a.chordangle(b);
        if sphere_distance >= self.min_d {
            return false;
        }
        self.min_d = sphere_distance;
        self.min_edge = Edge { v0: *a, v1: *b };
        // If we have a threshold, determine if we can stop early.
        // If the sphere distance is within range of the stopAfter, we can
        // definitively say we've reach the close enough point.
        !self.exclusivity && self.min_d <= self.stop_after
            || self.exclusivity && self.min_d < self.stop_after
    }

    fn distance(&self) -> f64 {
        // If the distance is zero, avoid the call to spheroidDistance and return early.
        if self.min_d == s1::ChordAngle(0.0) {
            return 0.0;
        }
        match self.sphere_or_spheroid {
            UseSphereOrSpheroid::Sphere => {
                let angle: s1::Angle = (&self.min_d).into();
                angle.rad() * self.sphere_radius()
            }
            UseSphereOrSpheroid::Spheroid => {
                self.spheroid_distance(&self.min_edge.v0, &self.min_edge.v1)
            }
        }
    }

    fn is_max_distance(&self) -> bool {
        false
    }

    fn spheroid_distance(&self, a: &Point, b: &Point) -> f64 {
        geographiclib_rs::InverseGeodesic::inverse(
            &self.desic,
            a.latitude().deg(),
            a.longitude().deg(),
            b.latitude().deg(),
            b.longitude().deg(),
        )
    }

    fn sphere_radius(&self) -> f64 {
        let d = self.desic;
        (d.a * 2.0 + d._b) / 3.0
    }
}

#[derive(Debug)]
struct GeographyDistanceCalculator<'a> {
    updater: &'a mut MinDistanceUpdater,
    #[allow(dead_code)]
    bounding_box_intersects: bool,
}

impl GeographyDistanceCalculator<'_> {
    // ClosestPointToEdge projects the point onto the infinite line represented
    // by the edge. This will return the point on the line closest to the edge.
    // It will return the closest point on the line, as well as a bool representing
    // whether the point that is projected lies directly on the edge as a segment.
    //
    // For visualization and more, see: Section 6 / Figure 4 of
    // "Projective configuration theorems: old wine into new wineskins", Tabachnikov, Serge, 2016/07/16
    fn closest_point_to_edge(&self, edge: &Edge, point: &Point) -> (Point, bool) {
        let Edge { v0, v1 } = edge;

        // Project the point onto the normal of the edge. A great circle passing through
        // the normal and the point will intersect with the great circle represented
        // by the given edge.
        let normal = v0.0.cross(&v1.0).normalize();
        // To find the point where the great circle represented by the edge and the
        // great circle represented by (normal, point), we project the point
        // onto the normal.
        let normal_scaled_to_point = normal * (normal.dot(&point.0));
        // The difference between the point and the projection of the normal when normalized
        // should give us a point on the great circle which contains the vertexes of the edge.
        let closest_point = Point((point.0 - normal_scaled_to_point).normalize());
        // We then check whether the given point lies on the geodesic of the edge,
        // as the above algorithm only generates a point on the great circle
        // represented by the edge.

        // FIX ME
        // let lies_on = Polyline(vec![v0, v1]).IntersectsCell(s2.CellFromPoint(closest_point))
        let lies_on = true;
        (closest_point, lies_on)
    }
}

// on_point_to_line_string updates the distance between a point and a polyline.
// Returns true if the calling function should early exit.
fn on_point_to_line_string(c: &mut GeographyDistanceCalculator, a: &Point, b: &Polyline) -> bool {
    // Compare the first point, to avoid checking each V0 in the chain afterwards.
    if c.updater.update(a, &b.0[0]) {
        return true;
    }
    on_point_to_edges_except_first_edge_start(c, a, b)
}

// on_point_to_edges_except_first_edge_start updates the distance against the edges of a shape and a point.
// It will only check the V1 of each edge and assumes the first edge start does not need the distance
// to be computed.
fn on_point_to_edges_except_first_edge_start(
    c: &mut GeographyDistanceCalculator,
    a: &Point,
    b: &Polyline,
) -> bool {
    for edge in b.iter_edge() {
        // Check against all V1 of every edge.
        if c.updater.update(a, &edge.v1) {
            return true;
        }
        // The max distance between a point and the set of points representing an edge is the
        // maximum distance from the point and the pair of end-points of the edge, so we don't
        // need to update the distance using the projected point.
        if !c.updater.is_max_distance() {
            // Also project the point to the infinite line of the edge, and compare if the closestPoint
            // lies on the edge.
            if let (closest, true) = c.closest_point_to_edge(&edge, a) {
                if c.updater.update(a, &closest) {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use s2::latlng::LatLng;

    use super::point_to_line_distance;
    use super::point_to_point_distance;
    use super::Polyline;

    #[test]
    fn test_distance() {
        // copy from https://github.com/cockroachdb/cockroach/blob/master/pkg/geo/geogfn/distance_test.go
        let a = LatLng::from_degrees(33.9434, -118.4079);
        let b = LatLng::from_degrees(49.0083, 2.5559);

        let d = point_to_point_distance(&a, &b);
        assert_eq!(9124665.273176732, d);

        let line = Polyline(vec![
            LatLng::from_degrees(1.0, 1.0).into(),
            LatLng::from_degrees(2.0, 2.0).into(),
            LatLng::from_degrees(3.0, 3.0).into(),
        ]);

        let a = LatLng::from_degrees(2.6, 2.4);
        let d = point_to_line_distance(&a.into(), &line);
        assert_eq!(15660.439599330552, d);
    }
}
