use rain_math_float::Float;
use std::borrow::Borrow;

use st0x_float_serde::option_float_string_serde;
pub(crate) use st0x_float_serde::{
    deserialize_float_from_number_or_string, deserialize_option_float_from_number_or_string,
    format_float, serialize_float_as_string,
};

pub(crate) fn serialize_option_float<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Borrow<Option<Float>>,
{
    option_float_string_serde::serialize(value, serializer)
}
