fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec!["proto/misaka_network.proto"];
    let includes = vec!["proto"];

    // 使用 tonic_prost_build 编译 gRPC 服务
    tonic_prost_build::configure()
        .build_server(false)  // 生成服务端代码
        .build_client(true) // 生成客户端代码
        .compile_protos(&proto_files, &includes)?;

    // 告诉 cargo 当 proto 文件变化时重新构建
    for proto in &proto_files {
        println!("cargo:rerun-if-changed={}", proto);
    }

    Ok(())
}