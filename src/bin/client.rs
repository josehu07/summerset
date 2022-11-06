use summerset_proto::summerset_api_client::SummersetApiClient;
use summerset_proto::{GetRequest, PutRequest};

pub mod summerset_proto {
    tonic::include_proto!("kv_api");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SummersetApiClient::connect("http://[::1]:50051").await?;

    let put_request = tonic::Request::new(PutRequest {
        request_id: 1234,
        key: "Jose".into(),
        value: "180".into(),
    });

    let put_response = client.put(put_request).await?;
    println!("Put Response = {:?}", put_response);

    let get_request = tonic::Request::new(GetRequest {
        request_id: 1234,
        key: "Jose".into(),
    });

    let get_response = client.get(get_request).await?;
    println!("Get Response = {:?}", get_response);

    Ok(())
}
