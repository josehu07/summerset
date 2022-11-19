fn main() -> Result<(), Box<dyn std::error::Error>> {
    // compile all protobufs under `proto/`
    tonic_build::compile_protos("proto/external_api.proto")?;
    tonic_build::compile_protos("proto/simple_push.proto")?;
    Ok(())
}
