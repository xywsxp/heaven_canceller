fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&["proto/misaka_signal.proto"], &["proto/"])?;
    
    println!("cargo:rerun-if-changed=proto/misaka_signal.proto");
    Ok(())
}
