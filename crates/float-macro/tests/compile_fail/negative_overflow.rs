use st0x_float_macro::float;

fn main() {
    // Negative number far exceeding Float's representable range
    let _ = float!(-99999999999999999999999999999999999999999999999999999999999999999999999);
}
