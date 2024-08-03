use geozero::error::GeozeroError;
use geozero::GeozeroGeometry;

use super::Geometry;
use super::GeometryBuilder;

pub struct Wkb<B: AsRef<[u8]>>(pub B);

impl<B: AsRef<[u8]>> TryFrom<Wkb<B>> for Geometry {
    type Error = GeozeroError;

    fn try_from(value: Wkb<B>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geozero::wkb::Wkb(value.0).process_geom(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Wkb<Vec<u8>>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Wkb<Vec<u8>>, Self::Error> {
        use geozero::CoordDimensions;
        use geozero::ToWkb;

        Ok(Wkb(self.to_ewkb(CoordDimensions::xy(), None)?))
    }
}
