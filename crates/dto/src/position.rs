//! Per-symbol net position DTO.

use rain_math_float::Float;
use serde::{Deserialize, Serialize};

use st0x_finance::Symbol;
use st0x_float_serde::float_string_serde;

/// Per-symbol net position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    #[serde(with = "float_string_serde")]
    pub net: Float,
}
