fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&["proto/proximo.proto"], &["proto"])
        .unwrap();
}
