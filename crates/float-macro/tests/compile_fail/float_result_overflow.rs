use st0x_float_macro::float_result;

fn main() {
    // Verifies the compile error names the actual invoking macro
    // (`float_result!`) rather than hardcoding `float!`.
    let _ = float_result!(99999999999999999999999999999999999999999999999999999999999999999999999);
}
