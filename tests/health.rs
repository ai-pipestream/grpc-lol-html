// SPDX-License-Identifier: Apache-2.0

//! The health service reports what an orchestrator needs to know: the process
//! is up, and the extraction service behind it is serving.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};

use grpc_lol_html::LolHtmlGrpc;
use grpc_lol_html::proto::v1::lol_html_service_server::LolHtmlServiceServer;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// Start the server with the health service registered, as `main` does, and
/// return a connected health client.
async fn start_server() -> HealthClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();

    let service = LolHtmlGrpc::new().into_service();
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_serving::<LolHtmlServiceServer<LolHtmlGrpc>>()
        .await;

    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .add_service(health)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server failed");
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to server");
    HealthClient::new(channel)
}

/// `Check` against the extraction service's own name must report SERVING; a
/// bare probe with no service name is the process-level answer and must too.
#[tokio::test]
async fn the_health_service_reports_serving() {
    let mut client = start_server().await;

    for service in ["", "lolhtml.v1.LolHtmlService"] {
        let status = client
            .check(HealthCheckRequest {
                service: service.to_owned(),
            })
            .await
            .expect("health check")
            .into_inner()
            .status;
        assert_eq!(
            status,
            ServingStatus::Serving as i32,
            "service {service:?} should report SERVING"
        );
    }
}

/// A name nobody registered gets NOT_FOUND rather than a guess, which is the
/// property that makes a positive answer mean something.
#[tokio::test]
async fn an_unregistered_service_name_is_not_found() {
    let mut client = start_server().await;

    let err = client
        .check(HealthCheckRequest {
            service: "lolhtml.v1.NoSuchService".to_owned(),
        })
        .await
        .expect_err("an unknown service should not report a status");
    assert_eq!(err.code(), tonic::Code::NotFound);
}
