// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::env;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::{unfold, FuturesUnordered};
use futures::{stream, StreamExt, TryStreamExt};
// use tokio_stream::StreamExt;
use bytes::{BufMut, Bytes, BytesMut};
use googleapis_tonic_google_storage_v2::google::storage::v2::{
    storage_client::StorageClient, write_object_request, ChecksummedData, Object as GcsObject,
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadObjectRequest, ReadObjectResponse,
    StartResumableWriteRequest, StartResumableWriteResponse, WriteObjectRequest,
    WriteObjectResponse, WriteObjectSpec,
};
use mock_instant::thread_local::MockClock;
use mockall::{automock, mock, predicate::*};
use nativelink_config::stores::GCSSpec;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_store::gcs_store::CredentialProvider;
use nativelink_store::gcs_store::GCSStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatus, HealthStatusIndicator};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::spawn;
use nativelink_util::store_trait::{StoreLike, UploadSizeInfo};
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tokio::time::{sleep, Instant};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Response;
use tonic::{Request, Status};

// use tracing::{event, Level};
// use crate::cas_utils::is_zero_digest;

const BUCKET_NAME: &str = "dummy-bucket-name";
const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
const REGION: &str = "testregion";

pub trait MockableStorageClient: Send + Sync {
    fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> BoxFuture<'static, Result<Response<tonic::codec::Streaming<ReadObjectResponse>>, Status>>;

    // fn write_object(
    //     &self,
    //     request: impl tonic::IntoStreamingRequest<Message = WriteObjectRequest>,
    // ) -> BoxFuture<'static, Result<Response<WriteObjectResponse>, Status>>;

    fn start_resumable_write(
        &self,
        request: Request<StartResumableWriteRequest>,
    ) -> BoxFuture<'static, Result<Response<StartResumableWriteResponse>, Status>>;

    fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> BoxFuture<'static, Result<Response<QueryWriteStatusResponse>, Status>>;
}

impl MockableStorageClient for StorageClient<Channel> {
    fn read_object(
        &self,
        request: Request<ReadObjectRequest>,
    ) -> BoxFuture<'static, Result<Response<tonic::codec::Streaming<ReadObjectResponse>>, Status>>
    {
        let client = self.clone();
        Box::pin(async move { client.read_object(request).await })
    }

    // fn write_object(
    //     &self,
    //     request: impl tonic::IntoStreamingRequest<Message = WriteObjectRequest>,
    // ) -> BoxFuture<'static, Result<Response<WriteObjectResponse>, Status>> {
    //     let client = self.clone();
    //     Box::pin(async move { client.write_object(request).await })
    // }

    fn start_resumable_write(
        &self,
        request: Request<StartResumableWriteRequest>,
    ) -> BoxFuture<'static, Result<Response<StartResumableWriteResponse>, Status>> {
        let client = self.clone();
        Box::pin(async move { client.start_resumable_write(request).await })
    }

    fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> BoxFuture<'static, Result<Response<QueryWriteStatusResponse>, Status>> {
        let client = self.clone();
        Box::pin(async move { client.query_write_status(request).await })
    }
}

mock! {
    pub StorageClient {}

    #[async_trait::async_trait]
    impl  MockableStorageClient for StorageClient {
        fn read_object(
            &self,
            request: Request<ReadObjectRequest>,
        ) -> BoxFuture<'static, Result<Response<tonic::codec::Streaming<ReadObjectResponse>>, Status>>;

        fn start_resumable_write(
            &self,
            request: Request<StartResumableWriteRequest>,
        ) -> BoxFuture<'static, Result<Response<StartResumableWriteResponse>, Status>>;

        fn query_write_status(
            &self,
            request: Request<QueryWriteStatusRequest>,
        ) -> BoxFuture<'static, Result<Response<QueryWriteStatusResponse>, Status>>;
    }
}

fn setup_mock_client(
    response: Result<Response<ReadObjectResponse>, Status>,
) -> Arc<dyn MockableStorageClient> {
    let mut mock_client = MockStorageClient::new();
    mock_client.expect_read_object().return_once(|_| response);

    Arc::new(mock_client)
}

async fn create_gcs_store(
    mock_client: Arc<dyn MockableStorageClient>,
) -> Arc<GCSStore<impl Fn() -> MockInstantWrapped + Send + Sync>> {
    let credential_provider = Arc::new(CredentialProvider::new().await.unwrap());

    GCSStore::new_with_client_and_jitter(
        &GCSSpec {
            bucket: BUCKET_NAME.to_string(),
            ..Default::default()
        },
        mock_client,
        credential_provider,
        Arc::new(move |_delay| Duration::from_secs(0)),
        || MockInstantWrapped::default(),
    )
    .unwrap()
}

#[nativelink_test]
async fn test_has_object_found() -> Result<(), Error> {
    let mock_client = setup_mock_client(Ok(Response::new(ReadObjectResponse {
        metadata: Some(GcsObject {
            size: 512,
            ..Default::default()
        }),
        ..Default::default()
    })));

    let store = create_gcs_store(mock_client).await;

    let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
    let result = store.has(digest).await;

    assert_eq!(
        result,
        Ok(Some(512)),
        "Expected to find item, got: {result:?}"
    );

    Ok(())
}

// #[nativelink_test]
// async fn test_has_object_not_found() -> Result<(), Error> {
//     let mock_client = setup_mock_client(Err(tonic::Status::not_found("Object not found")));

//     let store = create_gcs_store(mock_client).await?;

//     let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
//     let result = store.has(&digest).await;

//     assert_eq!(result, Ok(None), "Expected to not find item, got: {result:?}");

//     Ok(())
// }
