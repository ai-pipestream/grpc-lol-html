// SPDX-License-Identifier: Apache-2.0

//! `GetServiceInfo` is how the shared demo shell discovers this service's
//! tab: the response must carry the repository name, the crate version, and
//! the `UiInfo` block whose shape every ai-pipestream service shares.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};

use grpc_lol_html::LolHtmlGrpc;
use grpc_lol_html::proto::v1::GetServiceInfoRequest;
use grpc_lol_html::proto::v1::lol_html_service_client::LolHtmlServiceClient;

/// Start the server on an ephemeral port and return a connected client.
async fn start_server() -> LolHtmlServiceClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();

    let service = LolHtmlGrpc::new().into_service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server failed");
    });

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to server");
    LolHtmlServiceClient::new(channel)
}

#[tokio::test]
async fn get_service_info_returns_the_ui_advertisement() {
    let mut client = start_server().await;

    let info = client
        .get_service_info(GetServiceInfoRequest {})
        .await
        .expect("get service info")
        .into_inner();

    assert_eq!(info.name, "grpc-lol-html");
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));

    let ui = info.ui.expect("ui advertisement must be present");
    assert_eq!(ui.title, "LOL HTML");
    assert_eq!(ui.path, "/ui/lol-html");
    assert_eq!(
        ui.description,
        "Streams CSS-selector matches out of HTML via lol-html"
    );
}
