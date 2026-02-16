use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Export TypeScript bindings for the dashboard")]
struct Args {
    /// Output directory for generated .ts files
    out_dir: PathBuf,
}

fn main() -> Result<(), ts_rs::ExportError> {
    let args = Args::parse();
    st0x_dto::export_bindings(&args.out_dir)
}
