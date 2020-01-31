fn main() {
    tonic_build::compile_protos("proto/proximo.proto").unwrap();
}
