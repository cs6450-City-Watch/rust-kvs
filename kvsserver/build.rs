fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("protos/sometime.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
    Ok(())
}
