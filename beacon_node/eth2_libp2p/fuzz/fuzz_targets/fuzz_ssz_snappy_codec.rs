#![no_main]
use libfuzzer_sys::fuzz_target;

use eth2_libp2p::rpc::{SSZSnappyInboundCodec, Protocol, Version, Encoding, ProtocolId};
use libp2p::bytes::BytesMut;
use tokio_util::codec::Decoder;
use types::MainnetEthSpec;

// From beacon_node/eth2-libp2p/src/rpc/protocol.rs
const MAX_RPC_SIZE: usize = 1_048_576; // 1M

fuzz_target!(|wrap: (Vec<u8>, Protocol)| {
    let (data, status) = wrap;

    let protocol = ProtocolId::new(
        status,
        Version::V1,
        Encoding::SSZSnappy,
    );
    let mut codec = SSZSnappyInboundCodec::<MainnetEthSpec>::new(protocol , MAX_RPC_SIZE);

    let mut buffer = BytesMut::from(data.as_slice());

    let _ = codec.decode(&mut buffer);
});
