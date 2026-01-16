fn main() -> Result<(), ts_rs::ExportError> {
    st0x_hedge::dashboard::export_bindings()?;
    println!("TypeScript bindings exported successfully!");
    Ok(())
}
